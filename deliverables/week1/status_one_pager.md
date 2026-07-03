# Week 1 Status — CartCo Unified Commerce Lakehouse (B1)

## What's done

- Local data-platform stack running via Docker Compose: MinIO, Iceberg REST catalog, Postgres (Airflow metadata DB), and Airflow webserver + scheduler (LocalExecutor). All services verified healthy and reachable (Airflow UI on :8080, MinIO console on :9001) — see `screenshots/minio_bucket.png`.
- Custom Airflow image (`Dockerfile.airflow`) built with PyIceberg 0.7.1, PyArrow, Great Expectations, Faker, and PySpark (PySpark installed but not yet used).
- Synthetic Shopify orders generator (`ingestion/generate_synthetic_shopify_data.py`) producing 50,000 seeded, realistic rows (order dates over ~2 years, weighted statuses, category-based pricing).
- Shopify Bronze ingestion (`ingestion/shopify_bronze.py`): reads the CSV and writes `bronze.shopify_orders` as an Iceberg table on MinIO with an explicit schema, partitioned by `ingestion_date`, written idempotently (partition-level overwrite — a same-day re-run does not duplicate rows). Logs start time, rows read, rows written, end time.
- Verified end-to-end via a real Airflow DAG (`shopify_bronze_ingestion`, tasks `generate_data >> ingest_to_bronze`) with a successful run — proof in `screenshots/` (`airflow_dag_list.png`, `airflow_graph.png`, `airflow_generate_data_log.png`, `airflow_ingest_log.png`) and terminal logs (`bronze_ingestion_output.txt`, `airflow_dag_run_output.txt`).
- Architecture documented: Mermaid diagram in the README plus a C4-style PDF (`architecture_diagram/cartco_architecture.pdf`).
- 5 ADRs drafted (table format, orchestrator, partition strategy, ingestion tool, schema evolution).
- README rebuilt to the required standard; tech-stack table validated against the actual code.

## What's stuck

- Only the Shopify source is implemented — Amazon, in-store Kafka, and SFTP distributor connectors are still stubs.
- Bronze uses PyIceberg + PyArrow, not Spark; PySpark is installed in the image but not yet used (reserved for Silver/Gold). Compute runs in-process in the Airflow container, not on a separate Spark cluster.
- No Silver/Gold layers yet, so no Great Expectations checkpoints are wired.
- Lineage (OpenLineage + Marquez) not wired up — deferred to Week 2.
- No automated test suite yet — verification is manual (DAG run + read-back).
- Loom demo not yet recorded.

## Next week's 3 goals

1. Ship a skinny end-to-end slice: build the first Silver transform (`silver.shopify_orders` — schema enforcement + deduplication) with a Great Expectations checkpoint gating the write.
2. Add the Amazon Bronze connector (synthetic data + Iceberg load) following the Shopify pattern.
3. Wire OpenLineage → Marquez so the Shopify Bronze (and new Silver) run emits lineage, and record the Week 1/2 Loom demo.
