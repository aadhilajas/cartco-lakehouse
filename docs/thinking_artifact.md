# Why Bronze Stays Source-Faithful: The Design Decision Behind CartCo's Medallion Architecture

*Aadhil A Kareem · July 2026 · CartCo Unified Commerce Lakehouse (Problem Code B1)*

---

## The problem

CartCo sells through multiple channels, and each channel speaks its own language. The Shopify D2C storefront reports order statuses as `delivered`, `cancelled`, and `returned`. Amazon's marketplace reports use `shipped`, `cancelled`, `refund_initiated`, and `pending`. The column that identifies a product is called `product_id` in Shopify (format: `SHOP-00042`) and `asin` in Amazon (format: `B0CX7K2JMP`). Amazon orders carry a `marketplace` field (`amazon.in`, `amazon.com`, `amazon.co.uk`) and a `fulfillment_channel` (`FBA` vs `FBM`) that have no Shopify equivalent at all.

When I sat down to design the ingestion pipeline, the first question was: where does the harmonisation happen? At some point, `delivered` and `shipped` both need to become `completed` in a unified revenue mart. At some point, `product_id` and `asin` both need to feed into the same `product_identifier` column. The question isn't whether to normalise — it's where in the pipeline that normalisation boundary sits.

This matters more than it sounds. Getting the boundary wrong means one of two things: either you lose source fidelity too early (and can never reconstruct what the source actually sent), or you push too much complexity downstream (and every Gold mart has to independently handle source-specific edge cases). CartCo's medallion architecture has three layers — Bronze, Silver, Gold — and the normalisation boundary can sit at any of those transitions.

## The options I considered

### Option A: Normalise at ingestion (before Bronze)

The ingestion script itself would map `shipped` → `completed`, rename `asin` → `product_id`, and write a unified schema into a single `bronze.orders` table. Every source would land in the same table with the same columns.

**Upside:** Simple downstream. Silver and Gold see one clean schema from day one.

**Downside:** Bronze is no longer a raw landing zone. If Amazon changes their status vocabulary (say they add `partially_shipped`), I'd need to update the ingestion script's mapping logic *and* re-ingest historical data, because the raw source values were never preserved. Worse, I'd have no way to audit what Amazon actually sent on a given day — the source truth would be gone the moment it crossed the Bronze boundary.

### Option B: Normalise at the Bronze → Silver boundary

Bronze stays source-faithful — `ingestion/shopify_bronze.py` writes Shopify's own column names and status values unchanged into `bronze.shopify_orders`, and `ingestion/amazon_bronze.py` writes Amazon's columns (including `asin`, `marketplace`, `fulfillment_channel`) unchanged into `bronze.amazon_orders`. Separate tables, separate schemas, source-native vocabulary.

Silver owns the canonical mapping. `transforms/silver_shopify_orders.py` reads Bronze, maps `delivered` → `completed`, `cancelled` → `cancelled`, `returned` → `refunded` via `STATUS_CANONICAL_MAP`, renames `status` to `order_status`, derives `order_total = quantity × unit_price`, and writes to `silver.shopify_orders` with a canonical schema. An equivalent Silver Amazon transform would do the same for Amazon's vocabulary (`shipped` → `completed`, `refund_initiated` → `refunded`, `pending` → `pending`).

**Upside:** Bronze is a lossless archive. Silver is the contract boundary. Each layer has one job.

**Downside:** Silver transforms are per-source and carry mapping logic that could have been centralised.

### Option C: Normalise at the Silver → Gold boundary

Both Bronze and Silver stay source-faithful. Silver deduplicates and cleans the data but keeps source-native column names and status values. The Gold revenue mart is the first layer that unifies Shopify's `product_id` with Amazon's `asin` into a single `product_identifier`.

**Upside:** Silver is simpler (no mapping logic). Gold is the single place where cross-channel harmonisation happens.

**Downside:** Every Gold mart has to independently handle the source-to-canonical mapping. If I build a revenue mart, a channel performance mart, and a customer 360 mart, each one reimplements the same `shipped` → `completed` logic. That's three copies of a mapping that should live in one place.

## The decision: Option B

I chose Option B — Silver owns the canonical mapping. Bronze stays source-faithful.

This is what's actually implemented in the codebase. Here's how it plays out concretely:

**Bronze ingestion** (`ingestion/shopify_bronze.py`, lines 84–120) defines a `TABLE_SCHEMA` with Shopify's own column names: `product_id`, `status`, `unit_price`. The schema is declared explicitly as a `pyiceberg.schema.Schema` with 10 `NestedField` entries. The ingestion script reads the CSV with `pd.read_csv()`, adds an `ingestion_date` partition column, converts to a PyArrow table, and writes to `bronze.shopify_orders` via `table.overwrite()` with an `EqualTo("ingestion_date", today)` filter for idempotency. No business logic, no renaming, no filtering. Bronze is a format conversion layer: CSV in, Iceberg out, nothing else.

The Amazon Bronze connector (`ingestion/amazon_bronze.py`) follows the identical pattern but with Amazon's own schema — `asin` instead of `product_id`, plus `marketplace` and `fulfillment_channel` columns that don't exist in Shopify at all. Two separate Iceberg tables (`bronze.shopify_orders`, `bronze.amazon_orders`), two separate schemas, zero harmonisation.

**The quality gate** sits between Bronze and Silver. `transforms/ge_bronze_shopify_checkpoint.py` runs 8 Great Expectations checks against `bronze.shopify_orders` *before* Silver is allowed to run. Critically, expectation #3 validates that `status` is one of `["delivered", "cancelled", "returned"]` — the *source-valid* Bronze vocabulary, not the canonical Silver vocabulary. This is documented explicitly in the checkpoint's module docstring (lines 8–18): "this checkpoint validates `status` against the actual source vocabulary the Shopify generator emits, not the canonical Silver vocabulary. Mapping those source values to the canonical set is Silver's job alone." If `completed` (a Silver value) somehow appeared in Bronze, the quality gate would *reject* it — because seeing a canonical value in Bronze means something upstream already broke the Bronze contract.

**Silver transform** (`transforms/silver_shopify_orders.py`, lines 149–153) defines `STATUS_CANONICAL_MAP`:

```python
STATUS_CANONICAL_MAP = {
    "delivered": "completed",
    "cancelled": "cancelled",
    "returned": "refunded",
}
```

The `_canonicalize_status_to_order_status()` function (lines 203–229) applies this mapping using `pyarrow.compute.if_else()` chains — pure PyArrow, no pandas — and renames the column from `status` to `order_status` in the same step. Values outside the map are set to `"unknown"` rather than raising, because the GE checkpoint is the layer responsible for rejecting invalid Bronze data. By the time a batch reaches Silver, its statuses should already be known-good. The `"unknown"` fallback is a defensive backstop, not a primary validation mechanism.

Silver also truncates `order_date` from timestamp to date, deduplicates by `order_id` (keeping the latest `order_date`), drops rows with null `order_id` or `customer_id`, fills default values for `quantity`/`unit_price`/`product_category`, and derives `order_total = quantity × unit_price`. The output schema has 11 fields — a different shape from Bronze's 10.

## What this enables

**Independent Bronze evolution.** When I added the Amazon source, I didn't touch the Shopify pipeline at all. `bronze.amazon_orders` has its own schema with `asin`, `marketplace`, and `fulfillment_channel` — columns that are meaningless in a Shopify context. Each source's Bronze table evolves independently. If Amazon adds a new field tomorrow (say `estimated_delivery_date`), I add it to the Amazon Bronze schema and it flows through without affecting Shopify's pipeline or Silver's canonical contract.

**Source-faithful audit trail.** If finance asks "what did Amazon actually report for order X on July 15th?", I can scan `bronze.amazon_orders` with `EqualTo("ingestion_date", "2026-07-15")` and see the raw source values — `shipped`, not `completed`. The source truth is preserved. This is especially important for the status field: if Amazon changes their vocabulary (renames `refund_initiated` to `refund_processed`), the Bronze table will faithfully capture the new value, the GE checkpoint will catch it (it's not in the expected set), and the pipeline will halt before Silver tries to map a value it doesn't recognise. That's exactly the failure mode I want — loud and early, not silent and late.

**GE checkpoint as a contract enforcer.** The 8-expectation quality gate only works *because* Bronze is source-faithful. The checkpoint validates source-vocabulary invariants (`status ∈ {delivered, cancelled, returned}`) that would be meaningless if Bronze had already mapped everything to canonical values. This is the architectural payoff: Bronze has one job (land raw data), the checkpoint has one job (validate source contracts), and Silver has one job (canonicalise). Each layer's responsibility is unambiguous.

## What this costs

**Per-source Silver transforms.** Every channel needs its own Silver transform with its own `STATUS_CANONICAL_MAP`. Shopify maps `delivered` → `completed`; Amazon will map `shipped` → `completed`. That's duplicated mapping logic — two dictionaries doing conceptually the same thing. With 5 channels (Shopify, Amazon, in-store, distributor, ERP), that's 5 mapping dictionaries to maintain.

**No single Bronze table to query.** A quick "how many orders across all channels?" query can't just scan one table — it has to union `bronze.shopify_orders` and `bronze.amazon_orders` (and eventually more). This is deliberate: Bronze is not an analytics layer. But it does mean Bronze is useless for cross-channel questions; you have to go to Silver or Gold.

**Silver schema is manually maintained.** The canonical schema (`TABLE_SCHEMA` in `silver_shopify_orders.py`, 11 `NestedField` entries) is defined in Python code, not discovered dynamically. Adding a new canonical column means updating the schema definition, the `COLUMN_ORDER` list, and the transform function. There's no schema registry enforcing consistency across Silver transforms for different channels. At the current scale (2 channels, 1 Silver table), this is manageable. At 10 channels, it would be a maintenance burden.

## What I'd do differently with more time

**Shared canonical mapping registry.** Instead of per-source `STATUS_CANONICAL_MAP` dictionaries scattered across Silver transforms, I'd build a small `transforms/canonical.py` module that owns all source-to-canonical mappings in one place. Each Silver transform would import from it. This doesn't change the architecture — Silver still owns the mapping — but it centralises the maintenance surface.

**Per-source GE checkpoint factories.** The Shopify GE checkpoint (`ge_bronze_shopify_checkpoint.py`, 319 lines) is largely boilerplate — ephemeral context setup, validator construction, expectation iteration, failure reporting. For Amazon, I'd need an almost-identical checkpoint with different `VALID_STATUSES` and different column expectations (`asin` instead of `product_id`). A factory function that takes a source config (valid statuses, required columns, value ranges) and returns a checkpoint runner would eliminate that duplication.

**Column-level lineage.** The current OpenLineage emission (`lineage/emitter.py`) is table-level only — Marquez knows that `silver.shopify_orders` depends on `bronze.shopify_orders`, but not that `order_status` is derived from `status` via `STATUS_CANONICAL_MAP`. Adding `SchemaDatasetFacet` and `ColumnLineageDatasetFacet` to the emitted events would make the Bronze-to-Silver column-level derivation visible in the lineage graph. This would be especially valuable for auditors who need to trace a specific Gold metric back through Silver's mapping to the raw Bronze source value.

**Automated Silver schema compatibility tests.** A test that asserts "all Silver tables have exactly these canonical columns in this order" would catch schema drift before it reaches production. Right now, Silver schema conformance is only enforced by the explicit `TABLE_SCHEMA` and `COLUMN_ORDER` in each Silver transform — there's no cross-channel test that says "Shopify Silver and Amazon Silver must have the same output schema."

---

*This document accompanies the CartCo Unified Commerce Lakehouse project (Problem Code B1). The design decisions described here are implemented in the codebase at [github.com/aadhilajas/cartco-lakehouse](https://github.com/aadhilajas/cartco-lakehouse) and verified against a live, fully green 4-task Airflow DAG run.*
