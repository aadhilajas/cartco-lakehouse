"""Inspect local lineage journals/alerts without transmitting or acknowledging them."""

# Support direct CLI execution and imports from any working directory.
import sys as _sys
from pathlib import Path as _Path
_SCRIPT_PATH = _Path(__file__).resolve()
_REPOSITORY_ROOT = next(
    (parent for parent in _SCRIPT_PATH.parents
     if (parent / "pipeline/source_identity.py").is_file()
     and (parent / "scripts/layout.json").is_file()),
    _SCRIPT_PATH.parent,  # Individually copied stdlib tools do not need a repo.
)
if str(_REPOSITORY_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPOSITORY_ROOT))
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import sys

import jsonschema

sys.path.insert(0, str(_REPOSITORY_ROOT))
from pipeline.runtime import run_path


def inspect_run(work_root, batch_date, run_id):
    path = run_path(work_root, batch_date, run_id)
    schema = json.loads((_REPOSITORY_ROOT / "docs/schemas/cartco-observability-v1.json").read_text(encoding="utf-8"))
    validators = {name: jsonschema.Draft202012Validator(value, format_checker=jsonschema.FormatChecker())
                  for name, value in schema["$defs"].items()}
    events, deliveries, stages, pairs = Counter(), Counter(), defaultdict(Counter), defaultdict(list)
    errors = []
    evidence_errors = 0
    for filename in sorted((path / "observability").glob("*.event.json")):
        try:
            content = filename.read_bytes()
            event = json.loads(content)
            facet = event["run"]["facets"]["cartco_runtime"]
            validators["RuntimeRunFacet"].validate(facet)
            if event["run"]["runId"] != facet["invocationId"] or facet["orchestrationRunId"] != run_id or facet["batchDate"] != batch_date:
                raise ValueError("Journal event context differs")
            if event["job"]["name"] != "versioned_commerce_release." + facet["stage"] or event["eventType"] not in {"START", "COMPLETE", "FAIL"}:
                raise ValueError("Journal event job/state differs")
            for dataset in event.get("inputs", []) + event.get("outputs", []):
                if not isinstance(dataset.get("namespace"), str) or not isinstance(dataset.get("name"), str):
                    raise ValueError("Invalid dataset identity")
                snapshot = dataset.get("facets", {}).get("cartco_snapshot")
                if snapshot:
                    validators["SnapshotDatasetFacet"].validate(snapshot)
            delivery_path = filename.with_name(filename.name.replace(".event.json", ".delivery.json"))
            if delivery_path.exists():
                delivery = json.loads(delivery_path.read_text(encoding="utf-8"))
                if delivery["event_sha256"] != hashlib.sha256(content).hexdigest() or delivery["status"] not in {"disabled", "unavailable", "client_completed", "failed"}:
                    raise ValueError("Journal event/delivery checksum or status differs")
                deliveries[delivery["status"]] += 1
            else:
                deliveries["pending"] += 1
            evidence_errors += "evidenceErrorType" in facet["evidence"]
            events[event["eventType"]] += 1
            stages[facet["stage"]][event["eventType"]] += 1
            pairs[event["run"]["runId"]].append(event["eventType"])
        except Exception as exc:
            errors.append(dict(file=filename.name, error_type=type(exc).__name__))
    alerts, terminal = Counter(), 0
    for filename in (Path(work_root) / "alerts").glob("*.json"):
        try:
            alert = json.loads(filename.read_text(encoding="utf-8"))
            if alert.get("run_id") == run_id and alert.get("batch_date") == batch_date:
                if alert.get("schema_version") != "cartco-alert.v1" or not isinstance(alert.get("terminal"), bool):
                    raise ValueError("Invalid alert record")
                alerts[alert["kind"]] += 1
                terminal += alert["terminal"]
        except Exception as exc:
            errors.append(dict(file=filename.name, error_type=type(exc).__name__))
    incomplete = sum(values.count("START") != 1 or values.count("COMPLETE") + values.count("FAIL") != 1 for values in pairs.values())
    complete_stages = set(stages) == {"prepare", "shopify", "amazon", "pos", "inventory", "publish"} and all(counts["COMPLETE"] for counts in stages.values())
    clean = bool(complete_stages and not errors and not evidence_errors and not incomplete and not events["FAIL"] and not terminal and not sum(alerts.values()) and not deliveries["pending"])
    return dict(batch_date=batch_date, run_id=run_id, event_counts=dict(events), delivery_counts=dict(deliveries),
        stages={key: dict(value) for key, value in stages.items()}, invocations=len(pairs), incomplete_invocations=incomplete,
        alerts=dict(alerts), terminal_alerts=terminal, validation_errors=errors, evidence_errors=evidence_errors, all_stages_have_completed=bool(complete_stages),
        clean_history=clean, all_client_calls_completed=bool(sum(events.values()) > 0 and sum(deliveries.values()) == deliveries["client_completed"]),
        backend_graph_verified=False, notifications_sent=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--require-clean", action="store_true", help="Require all six stages and no historical failures/integrity gaps")
    parser.add_argument("--require-client", action="store_true", help="Require emit() calls to return; does not prove Marquez graph persistence")
    args = parser.parse_args()
    result = inspect_run(args.work_root, args.batch_date, args.run_id)
    print(json.dumps(result, indent=2))
    if (args.require_clean and not result["clean_history"]) or (args.require_client and not result["all_client_calls_completed"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
