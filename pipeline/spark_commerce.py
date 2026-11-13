"""Assessed commerce compute implemented with Spark DataFrames and windows.

The Python/PyArrow implementation remains an independent correctness oracle.
This module has no PyArrow dependency and never accesses the original catalog.
"""
from functools import reduce
import re

from pyspark.sql import functions as F, Window
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                              DecimalType, DateType, TimestampNTZType, BooleanType)

MONEY = DecimalType(18, 2)
STATE_SCHEMA = StructType([
    StructField("order_id", StringType()), StructField("customer_id", StringType()),
    StructField("channel", StringType()), StructField("order_date", DateType()),
    StructField("product_id", StringType()), StructField("product_category", StringType()),
    StructField("quantity", IntegerType()), StructField("unit_price", MONEY),
    StructField("order_status", StringType()), StructField("order_total", MONEY),
    StructField("transform_timestamp", TimestampNTZType()), StructField("source_updated_at", TimestampNTZType()),
    StructField("is_deleted", BooleanType()), StructField("currency", StringType())])
BUSINESS = [field.name for field in STATE_SCHEMA if field.name not in {"channel", "order_id", "source_updated_at", "transform_timestamp"}]
STATUSES = {"shopify": {"delivered": "completed", "cancelled": "cancelled", "returned": "refunded"},
            "amazon": {"shipped": "completed", "cancelled": "cancelled", "refund_initiated": "refunded", "pending": "pending"},
            "pos": {"paid": "completed", "voided": "cancelled", "refunded": "refunded", "open": "pending"}}


def exact_division(numerator, denominator, scale):
    """Final per-pool ratio; Spark performs grouping and joins.

    Quantize the rational result once; avoid intermediate scale reduction.
    """
    from decimal import Decimal, localcontext, ROUND_HALF_UP
    if numerator is None or denominator is None or denominator == 0:
        return None
    with localcontext() as context:
        context.prec = 70
        return (Decimal(numerator) / Decimal(denominator)).quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)


def reject_if(frame, condition, message):
    if frame.where(condition).limit(1).count():
        raise ValueError(message)


def normalize(raw, *, source, contract, applied_at):
    if source not in STATUSES:
        raise ValueError("Unsupported Spark order source")
    # UTC/session settings are fixed by the runner; wire timestamps must be explicit.
    for name in ["order_id", "source_updated_at", "is_deleted"]:
        if name not in raw.columns:
            raise ValueError("Missing source identity/version/deletion field")
    live = F.col("is_deleted") == "false"
    reject_if(raw, F.col("order_id").isNull() | (F.trim("order_id") != F.col("order_id")) | (F.length("order_id") == 0) |
        ~F.col("is_deleted").isin("true", "false") | F.col("is_deleted").isNull() |
        ~F.col("source_updated_at").rlike(r"T.*(Z|[+-][0-9]{2}:[0-9]{2})$") | F.to_timestamp("source_updated_at").isNull(), "Invalid source identity/version/deletion")
    version = F.to_timestamp("source_updated_at")
    reject_if(raw, version >= F.to_timestamp(F.lit(contract["extraction_end"])), "Source version beyond extraction end")
    if contract["delivery_mode"] == "incremental":
        reject_if(raw, version < F.to_timestamp(F.lit(contract["extraction_start"])), "Source version before incremental start")
    product = {"shopify": "product_id", "amazon": "asin", "pos": "sku"}[source]
    for name in ["customer_id", "order_date", product, "product_category", "quantity", "unit_price", "status", "currency"]:
        if name not in raw.columns:
            raw = raw.withColumn(name, F.lit(None).cast("string"))
    status = F.create_map(*[F.lit(x) for pair in STATUSES[source].items() for x in pair])[F.col("status")]
    reject_if(raw, live & (F.col("currency").isNull() | (F.col("currency") != "INR") |
        F.col("quantity").isNull() | ~F.col("quantity").rlike(r"^[0-9]+$") | F.col("quantity").cast("long").isNull() | (F.col("quantity").cast("long") > 10000) |
        F.col("unit_price").isNull() | ~F.col("unit_price").rlike(r"^[0-9]+(\.[0-9]{1,2})?$") |
        F.col("unit_price").cast(MONEY).isNull() | (F.col("unit_price").cast(MONEY) > 500000) | status.isNull() |
        F.to_timestamp("order_date").isNull() | ~F.col("order_date").rlike(r"T.*(Z|[+-][0-9]{2}:[0-9]{2})$")), "Live source violates exact INR/order contract")
    for name in [product, "product_category"] + ([] if source == "pos" else ["customer_id"]):
        reject_if(raw, live & (F.col(name).isNull() | (F.length(name) == 0) | (F.trim(name) != F.col(name))), "Missing canonical business key")
    customer = F.col("customer_id")
    if source == "pos":
        from pipeline.pos import pos_order_id
        identity = F.udf(pos_order_id, StringType())
        reject_if(raw, F.col("order_id") != identity("store_id", "terminal_id", "transaction_id"), "POS identity differs")
        for name in ["line_count", "tax_amount", "discount_amount", "order_total"]:
            if name not in raw.columns:
                raw = raw.withColumn(name, F.lit(None).cast("string"))
        reject_if(raw, live & (F.col("customer_id").isNull() | (F.trim("customer_id") != F.col("customer_id")) |
            F.col("line_count").isNull() | (F.col("line_count") != "1") |
            ~F.col("tax_amount").rlike(r"^0+(\.0{1,2})?$") | ~F.col("discount_amount").rlike(r"^0+(\.0{1,2})?$") |
            ~F.col("order_total").rlike(r"^[0-9]+(\.[0-9]{1,2})?$") |
            F.col("tax_amount").cast(MONEY).isNull() | (F.col("tax_amount").cast(MONEY) != 0) |
            F.col("discount_amount").cast(MONEY).isNull() | (F.col("discount_amount").cast(MONEY) != 0) |
            F.col("order_total").cast(MONEY).isNull() | (F.col("order_total").cast(MONEY) != F.col("quantity").cast("int") * F.col("unit_price").cast(MONEY))), "POS single-line totals differ")
        customer = F.when(F.length("customer_id") > 0, F.concat(F.lit("pos:customer:"), F.col("customer_id"))).otherwise(F.concat(F.lit("pos:guest:"), F.col("order_id")))
    values = {"order_id": F.col("order_id"), "channel": F.lit(source), "customer_id": customer,
        "order_date": F.to_date(F.from_utc_timestamp(F.to_timestamp("order_date"), contract["business_timezone"])),
        "product_id": F.col(product), "product_category": F.col("product_category"), "quantity": F.col("quantity").cast("int"),
        "unit_price": F.col("unit_price").cast(MONEY), "order_status": status,
        "order_total": (F.col("quantity").cast("int") * F.col("unit_price").cast(MONEY)).cast(MONEY),
        "transform_timestamp": F.to_timestamp(F.lit(applied_at)), "source_updated_at": version,
        "is_deleted": F.col("is_deleted") == "true", "currency": F.lit("INR")}
    return raw.select(*[(F.when(live, values[field.name]).otherwise(F.lit(None)).cast(field.dataType)
        if field.name in BUSINESS and field.name != "is_deleted" else values[field.name].cast(field.dataType)).alias(field.name) for field in STATE_SCHEMA])


def merge_state(existing, incoming):
    if existing.groupBy("channel", "order_id").count().where("count > 1").limit(1).count():
        raise ValueError("Existing Silver has duplicate keys")
    combined = existing.withColumn("_existing", F.lit(1)).unionByName(incoming.withColumn("_existing", F.lit(0)))
    conflicts = combined.groupBy("channel", "order_id", "source_updated_at").agg(F.countDistinct(F.struct(*BUSINESS)).alias("versions"))
    if conflicts.where("versions > 1").limit(1).count():
        raise ValueError("Equal source versions contain conflicting business values")
    # Existing wins an exact replay, retaining its original transform audit time.
    window = Window.partitionBy("channel", "order_id").orderBy(F.col("source_updated_at").desc(), F.col("_existing").desc())
    return combined.withColumn("_rank", F.row_number().over(window)).where("_rank = 1").drop("_existing", "_rank")


def gold_marts(state):
    active = state.where(~F.col("is_deleted"))
    completed = active.where(F.col("order_status") == "completed")
    daily = completed.groupBy("order_date", "channel").agg(F.sum("order_total").cast(MONEY).alias("total_revenue"), F.count("order_id").cast("int").alias("order_count"))
    daily = daily.withColumn("avg_order_value", F.round(F.col("total_revenue") / F.col("order_count"), 2).cast(MONEY)).withColumn("currency", F.lit("INR"))
    zero = F.lit("0.00").cast(MONEY)
    amount = lambda status: F.sum(F.when(F.col("order_status") == status, F.col("order_total")).otherwise(zero)).cast(MONEY)
    channel = active.groupBy("channel").agg(amount("completed").alias("total_revenue"), F.count("order_id").cast("int").alias("total_orders"),
        F.avg(F.when(F.col("order_status") == "cancelled", 1.0).otherwise(0.0)).alias("cancellation_rate"),
        F.avg(F.when(F.col("order_status") == "refunded", 1.0).otherwise(0.0)).alias("refund_rate"),
        F.sum("order_total").cast(MONEY).alias("placed_order_value"), amount("cancelled").alias("cancelled_order_value"),
        amount("refunded").alias("refunded_order_value"), amount("pending").alias("pending_order_value"))
    channel = channel.withColumn("avg_fulfillment_days", F.lit(None).cast("double")).withColumn("currency", F.lit("INR"))
    customer = completed.groupBy("customer_id", "channel").agg(F.count("order_id").cast("int").alias("total_orders"), F.sum("order_total").cast(MONEY).alias("total_spend"),
        F.min("order_date").alias("first_order_date"), F.max("order_date").alias("last_order_date"))
    categories = completed.groupBy("customer_id", "channel", "product_category").agg(F.sum("quantity").alias("units"))
    window = Window.partitionBy("customer_id", "channel").orderBy(F.col("units").desc(), F.col("product_category").asc())
    favorites = categories.withColumn("rank", F.row_number().over(window)).where("rank = 1").select("customer_id", "channel", F.col("product_category").alias("favorite_category"))
    customer = customer.join(favorites, ["customer_id", "channel"]).withColumn("currency", F.lit("INR"))
    bounds = completed.agg(F.min("order_date").alias("start"), F.max("order_date").alias("end"))
    units = completed.groupBy("product_category", "channel").agg(F.sum("quantity").cast("int").alias("units_sold")).crossJoin(F.broadcast(bounds))
    units = units.withColumn("reporting_period", F.concat_ws(" to ", F.col("start").cast("string"), F.col("end").cast("string"))).drop("start", "end")
    return {"daily_revenue": daily, "channel_performance": channel, "customer_360": customer, "category_unit_sales": units}


def inventory_turnover(state, inventory, *, period_start, period_end):
    """Fixed-standard-cost turnover; one declared location per source-qualified SKU.

    period_end is exclusive. Missing daily stock observations produce a visible
    incomplete status and null turnover; they never become zero stock.
    """
    from datetime import date
    if not 0 < (date.fromisoformat(period_end) - date.fromisoformat(period_start)).days <= 3660:
        raise ValueError("Inventory period must increase and stay bounded")
    stock = inventory.select("sku", "location_id", F.to_date("snapshot_date").alias("snapshot_date"), F.col("on_hand").cast("int").alias("on_hand"), F.col("unit_cost").cast(MONEY).alias("unit_cost"), "currency")
    reject_if(stock, F.col("snapshot_date").isNull() | F.col("on_hand").isNull() | (F.col("on_hand") < 0) | F.col("unit_cost").isNull() | (F.col("unit_cost") < 0) | (F.col("currency") != "INR"), "Invalid SFTP inventory values")
    if stock.groupBy("sku", "location_id", "snapshot_date").count().where("count > 1").limit(1).count():
        raise ValueError("Duplicate inventory observation")
    scope = stock.groupBy("sku").agg(F.countDistinct("location_id").alias("locations"), F.countDistinct("unit_cost").alias("costs"))
    if scope.where("locations != 1 OR costs != 1").limit(1).count():
        raise ValueError("Turnover requires one fixed cost and location per declared SKU")
    declared = stock.groupBy("sku", "location_id").agg(F.first("unit_cost").alias("unit_cost"))
    stock = stock.where((F.col("snapshot_date") >= F.lit(period_start).cast("date")) & (F.col("snapshot_date") < F.lit(period_end).cast("date")))
    summary = stock.groupBy("sku", "location_id").agg(F.first("unit_cost").alias("unit_cost"), F.countDistinct("snapshot_date").cast("int").alias("observed_days"),
        F.sum(F.col("on_hand") * F.col("unit_cost")).cast(DecimalType(24, 2)).alias("inventory_value_days"))
    summary = declared.join(summary.drop("unit_cost"), ["sku", "location_id"], "left").fillna({"observed_days": 0, "inventory_value_days": 0})
    sold = state.where((~F.col("is_deleted")) & (F.col("order_status") == "completed") & (F.col("order_date") >= F.lit(period_start).cast("date")) & (F.col("order_date") < F.lit(period_end).cast("date")))
    sold = sold.withColumn("sku", F.concat_ws(":", F.col("channel"), F.col("product_id")))
    if sold.join(summary, "sku", "left_anti").limit(1).count():
        raise ValueError("Completed sales lack an inventory/cost scope")
    cogs = sold.groupBy("sku").agg(F.sum("quantity").alias("units"))
    result = summary.join(cogs, "sku", "left").fillna({"units": 0})
    result = result.withColumn("cogs", (F.col("units") * F.col("unit_cost")).cast(MONEY)).withColumn("expected_days", F.datediff(F.lit(period_end), F.lit(period_start)))
    reject_if(result, F.col("expected_days") <= 0, "Inventory period must increase")
    complete = F.col("observed_days") == F.col("expected_days")
    average = F.udf(lambda total, days: exact_division(total, days, 10), DecimalType(28, 10))
    turnover = F.udf(lambda cogs, days, total: exact_division(cogs * days, total, 12), DecimalType(38, 12))
    result = result.withColumn("average_daily_inventory_value", F.when(complete, average("inventory_value_days", "expected_days")))
    result = result.withColumn("turnover", F.when(complete & (F.col("inventory_value_days") > 0), turnover("cogs", "expected_days", "inventory_value_days")))
    return result.withColumn("quality_status", F.when(~complete, "incomplete_daily_coverage").when(F.col("average_daily_inventory_value") == 0, "zero_inventory_value").otherwise("complete")).withColumn("currency", F.lit("INR")).withColumn("period_start", F.lit(period_start).cast("date")).withColumn("period_end", F.lit(period_end).cast("date")).withColumn("cost_basis", F.lit("fixed-standard-cost-v1")).withColumn("period_basis", F.lit("order_date_latest_state")).withColumn("is_synthetic", F.lit(True)).drop("units", "unit_cost")
