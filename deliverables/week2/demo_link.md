# Week 2 Demo

Recording not yet made — no Loom link exists for Week 2 at this time.

When recorded, it will cover: the 4-task `shopify_bronze_ingestion` DAG (`generate_data → ingest_to_bronze → run_ge_checkpoint → transform_to_silver`) running to success in Airflow, the Great Expectations checkpoint's 8 passing expectations against `bronze.shopify_orders`, and an independent read-back of `silver.shopify_orders` showing 50,000 canonicalized rows.
