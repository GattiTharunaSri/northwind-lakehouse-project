"""Delta Live Tables pipeline for Northwind Silver layer.

Declarative version of:
- Phase 3A: Silver products (incremental MERGE)
- Phase 3B: Silver customers (SCD Type 2)
- Phase 3C: Silver orders (dedup + quarantine via expectations)

Run as a DLT pipeline (not as a notebook). Pipeline config:
- Pipeline mode: triggered (batch) or continuous
- Target catalog: dbs_main_catalog
- Target schema: northwind_silver_dlt  (separate from manual silver to compare)
- Source: This file
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)

# ============================================================================
# CONFIG
# ============================================================================
CATALOG = "dbs_main_catalog"
BRONZE_SCHEMA = "northwind_bronze"

# ============================================================================
# PRODUCTS — Silver
# ============================================================================
@dlt.table(
    name="silver_products",
    comment="Silver products: type-cast, with derived margin_pct and price_band.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("positive_price", "price > 0")
@dlt.expect("realistic_margin", "margin_pct BETWEEN -50 AND 99")
def silver_products():
    """One row per (product_id, last_updated)."""
    return (
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.products_raw")
        .withColumn("price",     F.col("price").cast(DecimalType(10, 2)))
        .withColumn("cost",      F.col("cost").cast(DecimalType(10, 2)))
        .withColumn("weight_kg", F.col("weight_kg").cast(DecimalType(8, 3)))
        .withColumn("last_updated", F.to_date("last_updated"))
        .withColumn("is_active", F.col("is_active").cast("boolean"))
        .withColumn(
            "margin_pct",
            F.when(F.col("price") > 0,
                F.round((F.col("price") - F.col("cost")) / F.col("price") * 100, 2))
             .otherwise(None)
        )
        .withColumn(
            "price_band",
            F.when(F.col("price") < 50, "BUDGET")
             .when(F.col("price") < 200, "MID")
             .otherwise("PREMIUM")
        )
        .select(
            "product_id", "product_name", "category", "subcategory", "brand",
            "price", "cost", "margin_pct", "price_band",
            "weight_kg", "is_active", "last_updated",
        )
    )


# ============================================================================
# CUSTOMERS — Silver SCD Type 2 via apply_changes
# ============================================================================

# Step 1: A staging "view" that prepares Bronze data for SCD2
@dlt.view(
    name="customers_staging",
    comment="Cleansed customer data ready for SCD2 ingestion."
)
def customers_staging():
    return (
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw")
        .withColumn("address_street",  F.col("address.street"))
        .withColumn("address_city",    F.col("address.city"))
        .withColumn("address_state",   F.col("address.state"))
        .withColumn("address_zip",     F.col("address.zip"))
        .withColumn("address_country", F.col("address.country"))
        .withColumn("email_hash", F.sha2(F.lower(F.col("email")), 256))
        .withColumn("phone_hash", F.sha2(F.regexp_replace(F.col("phone"), r"\D", ""), 256))
        .withColumn("date_of_birth", F.to_date("date_of_birth"))
        .withColumn("signup_date",   F.to_date("signup_date"))
        .withColumn("_snapshot_date", F.to_date("_snapshot_date"))
        .filter(F.col("customer_id").isNotNull())
        .select(
            "customer_id", "first_name", "last_name", "email_hash", "phone_hash",
            "date_of_birth", "address_street", "address_city", "address_state",
            "address_zip", "address_country", "signup_date", "loyalty_tier",
            "preferred_language", "_snapshot_date",
        )
    )


# Step 2: Declare the SCD2 target table (auto-managed by DLT)
dlt.create_streaming_table(
    name="silver_dim_customers",
    comment="SCD Type 2 customer dimension — managed by DLT apply_changes.",
    table_properties={"quality": "silver"},
)

# Step 3: Tell DLT how to apply changes from staging into the target as SCD2
dlt.apply_changes(
    target="silver_dim_customers",
    source="customers_staging",
    keys=["customer_id"],
    sequence_by=F.col("_snapshot_date"),
    stored_as_scd_type=2,  # ← This single line replaces ~100 lines of manual MERGE!
    # Track these columns; if any change, create a new SCD2 version
    track_history_column_list=[
        "first_name", "last_name", "email_hash", "phone_hash",
        "date_of_birth", "address_street", "address_city", "address_state",
        "address_zip", "address_country", "loyalty_tier", "preferred_language",
    ],
)


# ============================================================================
# ORDERS — Silver fact with expectations
# ============================================================================

SHIPPING_ADDRESS_SCHEMA = StructType([
    StructField("street",  StringType(), True),
    StructField("city",    StringType(), True),
    StructField("state",   StringType(), True),
    StructField("zip",     StringType(), True),
    StructField("country", StringType(), True),
])

ITEMS_ARRAY_SCHEMA = ArrayType(StructType([
    StructField("product_id", StringType(),  True),
    StructField("quantity",   IntegerType(), True),
    StructField("unit_price", DoubleType(),  True),
]))


@dlt.view(name="orders_cleaned")
def orders_cleaned():
    """Parse JSON columns and flatten — pre-dedup, pre-validation."""
    return (
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.orders_raw")
        .withColumn("shipping_address",
            F.from_json(F.col("shipping_address"), SHIPPING_ADDRESS_SCHEMA))
        .withColumn("items",
            F.from_json(F.col("items"), ITEMS_ARRAY_SCHEMA))
        .withColumn("order_date",   F.to_timestamp("order_date"))
        .withColumn("total_amount", F.col("total_amount").cast(DecimalType(12, 2)))
        .withColumn("ship_street",  F.col("shipping_address.street"))
        .withColumn("ship_city",    F.col("shipping_address.city"))
        .withColumn("ship_state",   F.col("shipping_address.state"))
        .withColumn("ship_zip",     F.col("shipping_address.zip"))
        .withColumn("ship_country", F.col("shipping_address.country"))
        .withColumn("item_count",   F.size("items"))
        .withColumn("_extracted_at", F.to_timestamp("_extracted_at"))
    )


@dlt.table(
    name="silver_fact_orders",
    comment="Silver orders fact — deduplicated, validated, type-cast.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
# DLT data quality expectations — declarative replacement for our quarantine logic
@dlt.expect_or_drop("valid_order_id",  "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer",  "customer_id IS NOT NULL")
@dlt.expect_or_drop("positive_total",  "total_amount > 0")
@dlt.expect_or_drop("non_empty_items", "item_count > 0")
@dlt.expect("not_future_dated",  "order_date <= current_timestamp()")
def silver_fact_orders():
    """One row per order, with deduplication and validation."""
    from pyspark.sql.window import Window
    
    cleaned = dlt.read("orders_cleaned")
    
    # Deduplicate on order_id, keeping latest extraction
    dedup_window = Window.partitionBy("order_id").orderBy(
        F.desc("_extracted_at"), F.desc("_ingestion_timestamp")
    )
    return (cleaned
        .withColumn("_dup_rank", F.row_number().over(dedup_window))
        .filter(F.col("_dup_rank") == 1)
        .drop("_dup_rank", "shipping_address", "items")
    )