# CartCo Data Sources

Synthetic datasets representing CartCo's five sales channels. Full schema documentation and sample files will be added as ingestion scripts are implemented.

## Sources (planned)

| Source | Format | Channel | Bronze table |
|--------|--------|---------|--------------|
| Shopify | JSON (API export) | D2C website | `bronze.shopify_orders` |
| Amazon | CSV (settlement reports) | Marketplace | `bronze.amazon_orders` |
| Kafka POS | Avro/JSON stream | Physical stores | `bronze.instore_transactions` |
| SFTP Distributor | CSV | Supply chain | `bronze.distributor_shipments` |
| ERP export | CSV | Internal inventory | `bronze.erp_inventory` |

## Synthetic data

All data is synthetically generated for the internship — no real PII or production credentials are used.

## Partitioning

All Bronze tables are partitioned by `ingestion_date=YYYY-MM-DD`.
