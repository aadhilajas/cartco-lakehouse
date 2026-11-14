# Current Iceberg catalog

The current Spark application uses **Nessie/JDBC2 with MinIO**, not the earlier
REST/SQLite catalog. Names and schemas below were read from the committed public
snapshot during the 14 November 2026 documentation review. Each later release
has its own identities; do not treat these documentation examples as a live query.

## Table map

| Snapshot key | Table name or pattern | Partition |
|---|---|---|
| `bronze_orders` | `nessie.cartco_assessed.bronze_orders` | ingestion_date |
| `bronze_wire_objects` | `nessie.cartco_assessed.bronze_wire_objects` | ingestion_date |
| `bronze_inventory` | `nessie.cartco_assessed.bronze_inventory` | ingestion_date |
| `silver` | `nessie.cartco_assessed.silver_order_state` | channel |
| `daily_revenue` | `nessie.cartco_assessed.daily_revenue_<release_id>` | Unpartitioned release table |
| `channel_performance` | `nessie.cartco_assessed.channel_performance_<release_id>` | Unpartitioned release table |
| `customer_360` | `nessie.cartco_assessed.customer_360_<release_id>` | Unpartitioned release table |
| `category_unit_sales` | `nessie.cartco_assessed.category_unit_sales_<release_id>` | Unpartitioned release table |
| `inventory_turnover` | `nessie.cartco_assessed.inventory_turnover_<release_id>` | Unpartitioned release table |

## Semantics and publication

Bronze preserves source-shaped data and original wire evidence. Silver retains
channel-qualified order keys, source versions and tombstones. Money uses exact
decimals, and business dates follow the declared source/timezone contracts.

Five Gold outputs provide daily revenue, channel performance, customer 360,
category unit sales and genuine cost/stock inventory turnover. Fulfillment time
is unavailable without measured source timestamps. Customer matching remains
channel-qualified; public business exports do not include customer IDs.

A release manifest pins table UUIDs and snapshots; an atomic CURRENT pointer
selects the coherent release. This does not claim a multi-table ACID transaction.
Public snapshot identifiers are strings so JavaScript cannot round them.

## Current column schemas

Field comments, owner metadata and UUID/snapshot identities are retained in the
[published snapshot](../dashboard/public/published/snapshot.json). Bronze inventory
uses source-shaped strings; casts and validation belong to subsequent processing.

### bronze_orders

| Column | Spark type |
|---|---|
| `source` | `string` |
| `batch_id` | `string` |
| `ingestion_date` | `date` |
| `raw_payload` | `string` |

### bronze_wire_objects

| Column | Spark type |
|---|---|
| `source` | `string` |
| `ingestion_date` | `date` |
| `sha256` | `string` |
| `payload` | `binary` |

### bronze_inventory

| Column | Spark type |
|---|---|
| `sku` | `string` |
| `location_id` | `string` |
| `snapshot_date` | `string` |
| `on_hand` | `string` |
| `unit_cost` | `string` |
| `currency` | `string` |
| `ingestion_date` | `date` |
| `batch_id` | `string` |

### silver

| Column | Spark type |
|---|---|
| `order_id` | `string` |
| `customer_id` | `string` |
| `channel` | `string` |
| `order_date` | `date` |
| `product_id` | `string` |
| `product_category` | `string` |
| `quantity` | `integer` |
| `unit_price` | `decimal(18,2)` |
| `order_status` | `string` |
| `order_total` | `decimal(18,2)` |
| `transform_timestamp` | `timestamp_ntz` |
| `source_updated_at` | `timestamp_ntz` |
| `is_deleted` | `boolean` |
| `currency` | `string` |

### daily_revenue

| Column | Spark type |
|---|---|
| `order_date` | `date` |
| `channel` | `string` |
| `total_revenue` | `decimal(18,2)` |
| `order_count` | `integer` |
| `avg_order_value` | `decimal(18,2)` |
| `currency` | `string` |

### channel_performance

| Column | Spark type |
|---|---|
| `channel` | `string` |
| `total_revenue` | `decimal(18,2)` |
| `total_orders` | `integer` |
| `cancellation_rate` | `double` |
| `refund_rate` | `double` |
| `placed_order_value` | `decimal(18,2)` |
| `cancelled_order_value` | `decimal(18,2)` |
| `refunded_order_value` | `decimal(18,2)` |
| `pending_order_value` | `decimal(18,2)` |
| `avg_fulfillment_days` | `double` |
| `currency` | `string` |

### customer_360

| Column | Spark type |
|---|---|
| `customer_id` | `string` |
| `channel` | `string` |
| `total_orders` | `integer` |
| `total_spend` | `decimal(18,2)` |
| `first_order_date` | `date` |
| `last_order_date` | `date` |
| `favorite_category` | `string` |
| `currency` | `string` |

### category_unit_sales

| Column | Spark type |
|---|---|
| `product_category` | `string` |
| `channel` | `string` |
| `units_sold` | `integer` |
| `reporting_period` | `string` |

### inventory_turnover

| Column | Spark type |
|---|---|
| `sku` | `string` |
| `location_id` | `string` |
| `observed_days` | `integer` |
| `inventory_value_days` | `decimal(24,2)` |
| `cogs` | `decimal(18,2)` |
| `expected_days` | `integer` |
| `average_daily_inventory_value` | `decimal(28,10)` |
| `turnover` | `decimal(38,12)` |
| `quality_status` | `string` |
| `currency` | `string` |
| `period_start` | `date` |
| `period_end` | `date` |
| `cost_basis` | `string` |
| `period_basis` | `string` |
| `is_synthetic` | `boolean` |

## Verification and historical catalog

Use the [local acceptance runner](../scripts/README.md) and
[verify_assessed_catalog.py](../scripts/diagnostics/verify_assessed_catalog.py) inside the
current Spark environment. The dashboard catalog registry displays the approved
snapshot identities; the Vercel page does not query a persistent public Nessie.

The [earlier catalog dictionary](history/baseline-catalog.md) describes legacy
tables, doubles and REST-catalog commands. It is preserved for baseline
regression context, not recommended as the current schema reference.
