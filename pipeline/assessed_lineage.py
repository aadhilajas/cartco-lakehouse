"""Column derivations tied to actual assessed table names and snapshot versions."""
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from pipeline import column_lineage
from openlineage.client.run import Dataset

NAMESPACE = "iceberg://cartco-assessed"
PRODUCER = "https://github.com/aadhilajas/cartco-lakehouse"
BASE = "https://openlineage.io/spec/facets/"


def mappings(candidate):
    state = Dataset(namespace=NAMESPACE, name=candidate["catalog"]["silver"]["table"])
    inventory = Dataset(namespace=NAMESPACE, name=candidate["catalog"]["bronze_inventory"]["table"])
    result = {}
    for name, identity in candidate["catalog"]["gold"].items():
        columns = [field["name"] for field in identity["schema"]["fields"]]
        if name != "inventory_turnover":
            facet = column_lineage.gold(name, columns, state, {})
        else:
            values = {}
            for column in columns:
                if column in {"period_start", "period_end", "currency", "cost_basis", "period_basis", "is_synthetic"}:
                    deps, description = [], "Declared period, INR, fixed-standard-cost and synthetic fixture contract"
                elif column == "cogs":
                    deps = [(state, x) for x in ["channel", "product_id", "quantity", "order_status", "is_deleted", "order_date"]] + [(inventory, x) for x in ["sku", "location_id", "unit_cost"]]
                    description = "GROUP completed latest-state order quantities by source-qualified SKU; multiply by declared fixed standard cost"
                else:
                    deps = [(inventory, x) for x in ["sku", "location_id", "snapshot_date", "on_hand", "unit_cost"]]
                    if column in {"turnover", "quality_status"}:
                        deps += [(state, x) for x in ["channel", "product_id", "quantity", "order_status", "is_deleted", "order_date"]]
                    description = "GROUP observed daily stock values; divide by declared days; missing coverage or zero stock produces a null ratio"
                values[column] = (deps, description)
            facet = column_lineage.facet(values)
        result[name] = facet
    return result


def dataset(identity, facets=None):
    fields = [{"name": field["name"], "type": str(field["type"]), "description": field.get("metadata", {}).get("comment", "")} for field in identity["schema"]["fields"]]
    return {"namespace": NAMESPACE, "name": identity["table"], "facets": {
        "schema": {"_producer": PRODUCER, "_schemaURL": BASE + "1-1-1/SchemaDatasetFacet.json", "fields": fields},
        "version": {"_producer": PRODUCER, "_schemaURL": BASE + "1-0-0/DatasetVersionDatasetFacet.json", "datasetVersion": str(identity["snapshot_id"])}, **(facets or {})}}


def serial_facet(facet):
    return {"_producer": PRODUCER, "_schemaURL": BASE + "1-2-0/ColumnLineageDatasetFacet.json", "fields": {
        name: {"inputFields": [{"namespace": item.namespace, "name": item.name, "field": item.field} for item in field.inputFields],
               "transformationDescription": field.transformationDescription, "transformationType": field.transformationType}
        for name, field in facet.fields.items()}}


def emit_and_verify(candidate, *, endpoint, output):
    endpoint = endpoint.rstrip("/")
    results = {}
    facets = mappings(candidate)
    for name, identity in candidate["catalog"]["gold"].items():
        mapping = serial_facet(facets[name])
        inputs = [dataset(candidate["catalog"]["silver"])]
        if name == "inventory_turnover":
            inputs.append(dataset(candidate["catalog"]["bronze_inventory"]))
        event = {"eventTime": datetime.now(timezone.utc).isoformat(), "producer": PRODUCER,
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json", "eventType": "COMPLETE",
            "run": {"runId": str(uuid4())}, "job": {"namespace": "cartco.assessed", "name": "spark.gold." + name},
            "inputs": inputs, "outputs": [dataset(identity, {"columnLineage": mapping})]}
        request = Request(endpoint + "/api/v1/lineage", data=json.dumps(event).encode(), headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(request, timeout=15) as response:
            if response.status not in {200, 201, 202}:
                raise ValueError("Marquez rejected assessed event")
        url = endpoint + "/api/v1/column-lineage?" + urlencode({"nodeId": "dataset:" + NAMESPACE + ":" + identity["table"], "depth": 1})
        required = {column for column, definition in mapping["fields"].items() if definition["inputFields"]}
        deadline, checked = time.monotonic() + 30, None
        while time.monotonic() < deadline:
            with urlopen(url, timeout=10) as response:
                graph = json.load(response)
            fields = {node["data"]["field"]: node["data"] for node in graph.get("graph", []) if node.get("data", {}).get("dataset") == identity["table"] and node["data"].get("namespace") == NAMESPACE}
            if set(fields) >= required:
                checked = True
                for column, definition in mapping["fields"].items():
                    if column not in required:
                        continue  # Constants have no graph edge; verify their schema below.
                    actual = {(item["namespace"], item.get("name", item.get("dataset")), item["field"]) for item in fields[column].get("inputFields", [])}
                    expected = {(item["namespace"], item["name"], item["field"]) for item in definition["inputFields"]}
                    if actual != expected: checked = False
                if checked: break
            time.sleep(1)
        if not checked:
            raise ValueError(f"Marquez column derivations not verified: {name}")
        metadata_url = endpoint + "/api/v1/namespaces/" + quote(NAMESPACE, safe="") + "/datasets/" + quote(identity["table"], safe="")
        with urlopen(metadata_url, timeout=10) as response:
            metadata = json.load(response)
        if {field["name"] for field in metadata["fields"]} != set(mapping["fields"]):
            raise ValueError("Marquez output schema fields differ")
        results[name] = {"run_id": event["run"]["runId"], "fields_verified": len(mapping["fields"]),
            "nonconstant_derivations_verified": len(required), "constant_fields": sorted(set(mapping["fields"]) - required), "graph": graph}
    report = {"status": "verified", "release_id": candidate["release_id"], "verified_at": datetime.now(timezone.utc).isoformat(), "tables": results}
    Path(output).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report
