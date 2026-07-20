# Lineage (OpenLineage + Marquez)

Local lineage for the working Shopify Bronze → GE → Silver path. This is a
targeted observability add-on, not a general lineage platform — see
"What's covered" below before assuming more than what's actually here.

## How it works

- **Backend:** [Marquez](https://marquez-project.ai/) (`marquezproject/marquez:0.51.1`
  + its own Postgres, `marquezproject/marquez-web:0.51.1` for the UI), added
  to `docker-compose.yml` as `marquez-db` / `marquez-api` / `marquez-web`.
  Local-only — no external network calls.
- **Emission:** each instrumented script's `run()` calls the OpenLineage
  Python client (`openlineage-python==1.50.0`) directly, via the small
  wrapper in [`lineage/emitter.py`](../lineage/emitter.py). It emits a
  `START` event before the work begins and a `COMPLETE` event after (or a
  `FAIL` event if the script raises, followed by re-raising the original
  exception — lineage never swallows a real failure).
- **Why manual emission instead of `apache-airflow-providers-openlineage`:**
  the instrumented tasks are a `BashOperator` running a standalone script
  (`ingest_to_bronze`) and two `PythonOperator`s calling plain Python
  functions (`run_ge_checkpoint`, `transform_to_silver`) — none of these are
  the declarative SQL/operator-level work the provider's automatic
  extraction is built around. Emitting explicitly from inside each script's
  `run()` is simpler to verify and works identically whether the script is
  invoked by Airflow or by hand.
- **Fail-safe by design:** `lineage/emitter.py` never raises. If
  `OPENLINEAGE_URL` is unset, or Marquez is down, or the HTTP call fails,
  the emitter logs a warning and the pipeline continues exactly as if
  lineage instrumentation weren't there at all.

## Starting it locally

```bash
docker compose up -d --build
```

This now also brings up `marquez-db`, `marquez-api` (HTTP API on
`localhost:5000`, admin/health on `localhost:5001`), and `marquez-web` (UI on
`localhost:3000`). Airflow's containers get `OPENLINEAGE_URL=http://marquez-api:5000`
and `OPENLINEAGE_NAMESPACE=cartco-lakehouse` automatically (see
`x-airflow-common` in `docker-compose.yml`).

## Running the DAG and inspecting lineage

1. Trigger the DAG as usual:
   ```bash
   docker exec cartco-airflow-scheduler airflow dags trigger shopify_bronze_ingestion
   ```
2. Open the Marquez UI at [http://localhost:3000](http://localhost:3000) and
   select the `cartco-lakehouse` namespace. You should see three jobs:
   - `shopify_bronze_ingestion.ingest_to_bronze` → writes `bronze.shopify_orders`
   - `shopify_bronze_ingestion.run_ge_checkpoint` → reads `bronze.shopify_orders`
   - `shopify_bronze_ingestion.transform_to_silver` → reads `bronze.shopify_orders`, writes `silver.shopify_orders`
3. Or query the HTTP API directly, e.g.:
   ```bash
   curl -s http://localhost:5000/api/v1/namespaces/cartco-lakehouse/jobs | python -m json.tool
   ```

## What's covered now

- One `START` + one `COMPLETE` (or `FAIL`) OpenLineage event per run of each
  of the three Shopify pipeline steps: Bronze write, GE checkpoint, Silver
  write.
- Dataset-level lineage (which job reads/writes which Iceberg table) for
  `bronze.shopify_orders` and `silver.shopify_orders`.
- Job namespace `cartco-lakehouse`; dataset namespace `iceberg://cartco-lakehouse`.
- Verified locally: see `deliverables/week3/lineage_proof.txt` for the actual
  Marquez API responses and job/dataset names from a live run.

## What's intentionally not covered yet

- **Column-level lineage.** Only whole-table input/output datasets are
  emitted — no field-level facets (e.g. which Bronze column feeds which
  Silver column).
- **Schema facets / row-count facets on the OpenLineage events themselves.**
  Row counts are still logged by each script (see their existing audit log
  lines) but are not (yet) attached as OpenLineage dataset facets.
- **Any channel other than Shopify.** Amazon, POS, distributor, and ERP
  connectors don't exist yet, so there is nothing to instrument for them.
- **Gold layer.** Not built yet.
- **`generate_data`** (the synthetic-CSV-generation task) is not
  instrumented — it doesn't read or write an Iceberg table, so there's no
  dataset-level lineage edge for it to contribute.
- **Automatic Airflow-level lineage** (e.g. via
  `apache-airflow-providers-openlineage`'s listener). This project uses
  manual, explicit emission instead — see "Why manual emission" above.
