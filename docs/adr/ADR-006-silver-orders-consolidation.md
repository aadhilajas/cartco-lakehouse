# ADR-006: Consolidating per-channel Silver tables into a single silver.orders table

**Status:** Accepted
**Date:** 2026-07-25

## Context

Originally, the lakehouse Silver layer wrote channel data to separate tables (e.g. `silver.shopify_orders` for Shopify). As new channels like Amazon were introduced, keeping per-channel Silver tables would force every Gold mart query to maintain explicit `UNION ALL` logic across all channel tables. Furthermore, each new channel added to the lakehouse would require altering Gold transformations to union the additional Silver table.

The canonical entity model at Silver dictates that all orders — regardless of source channel — share a common schema (`order_id`, `customer_id`, `channel`, `order_date`, `product_id`, `product_category`, `quantity`, `unit_price`, `order_status`, `order_total`, `transform_timestamp`).

## Decision

Consolidate all Silver order transformations (Shopify, Amazon, and future channels) to write to a **single, unified `silver.orders` Iceberg table**.

To ensure that re-running one channel's Silver transform does not overwrite or delete data belonging to other channels within the same `order_date` partitions, every channel's Silver write is executed using PyIceberg's partition-level overwrite with an explicit **channel-scoped filter**:

```python
overwrite_filter = And(
    And(
        GreaterThanOrEqual("order_date", min_order_date.isoformat()),
        LessThanOrEqual("order_date", max_order_date.isoformat()),
    ),
    EqualTo("channel", channel_name),
)
silver_table.overwrite(df=silver_arrow, overwrite_filter=overwrite_filter)
```

## Consequences

**Positive:**
- **Simplified Gold Layer:** Gold marts query a single `silver.orders` table without needing complex `UNION ALL` subqueries.
- **Isolated Idempotency:** Each channel's Silver transform remains fully idempotent. Re-running Shopify Silver replaces only `channel='shopify'` rows in the specified date range, leaving `channel='amazon'` rows completely untouched.
- **Scalability:** Adding a new channel (e.g., POS or WooCommerce) only requires conforming raw Bronze data to the canonical Silver schema and writing to `silver.orders` with `channel='<new_channel>'`. Gold marts automatically pick up the new channel.

**Negative:**
- **Concurrent Write Conflict Risk:** If two channel Silver DAGs run simultaneously and attempt to commit snapshot changes to `silver.orders` at the exact same millisecond, Iceberg commit retries are invoked. (Mitigated by low concurrent write volume and Iceberg's optimistic concurrency control).
