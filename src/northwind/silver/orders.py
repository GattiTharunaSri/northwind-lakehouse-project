"""Silver layer for orders.

Three outputs:
- silver.fact_orders            — one row per valid order
- silver.fact_order_items       — one row per item line within each order
- silver.fact_orders_quarantine — orders that failed validation
"""
from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

from ._utils import ensure_silver_schema, table_exists


def _flatten_and_clean(bronze_df: DataFrame) -> DataFrame:
    """Type-cast, flatten, and add a row_hash to incoming Bronze orders."""
    return (bronze_df
        .withColumn("order_date",   F.to_timestamp("order_date"))
        .withColumn("total_amount", F.col("total_amount").cast(DecimalType(12, 2)))
        # Flatten shipping_address
        .withColumn("ship_street",  F.col("shipping_address.street"))
        .withColumn("ship_city",    F.col("shipping_address.city"))
        .withColumn("ship_state",   F.col("shipping_address.state"))
        .withColumn("ship_zip",     F.col("shipping_address.zip"))
        .withColumn("ship_country", F.col("shipping_address.country"))
        # Item count derived from the array
        .withColumn("item_count", F.size("items"))
        .withColumn("_extracted_at", F.to_timestamp("_extracted_at"))
    )


def _deduplicate_orders(df: DataFrame) -> DataFrame:
    """Pick exactly one row per order_id when duplicates exist.
    
    Tiebreaker: latest _extracted_at wins. If still tied, latest _ingestion_timestamp.
    This is deterministic — re-running on the same data picks the same winner.
    """
    dedup_window = Window.partitionBy("order_id").orderBy(
        F.desc("_extracted_at"),
        F.desc("_ingestion_timestamp"),
    )
    return (df
        .withColumn("_dup_rank", F.row_number().over(dedup_window))
        .filter(F.col("_dup_rank") == 1)
        .drop("_dup_rank"))


def _validate_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split orders into (valid, quarantine).
    
    Quarantine reasons:
    - null order_id
    - null customer_id (orphan)
    - non-positive total_amount
    - empty items array
    - order_date in the future
    """
    df_with_reason = df.withColumn(
        "_quarantine_reason",
        F.when(F.col("order_id").isNull(), "NULL_ORDER_ID")
         .when(F.col("customer_id").isNull(), "NULL_CUSTOMER_ID")
         .when(F.col("total_amount") <= 0, "NON_POSITIVE_TOTAL")
         .when(F.size("items") == 0, "EMPTY_ITEMS")
         .when(F.col("order_date") > F.current_timestamp(), "FUTURE_ORDER_DATE")
         .otherwise(None)
    )
    valid = df_with_reason.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
    quarantine = df_with_reason.filter(F.col("_quarantine_reason").isNotNull())
    return valid, quarantine


def transform_orders(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Full orders Silver transform pipeline.
    
    Returns (valid_orders_df, quarantine_df).
    """
    cleaned = _flatten_and_clean(bronze_df)
    deduped = _deduplicate_orders(cleaned)
    valid, quarantine = _validate_orders(deduped)
    
    # Final shape for valid orders
    valid_final = valid.select(
        "order_id",
        "customer_id",
        "order_date",
        "status",
        "total_amount",
        "item_count",
        "payment_method",
        "ship_street",
        "ship_city",
        "ship_state",
        "ship_zip",
        "ship_country",
        # discount_code may not exist on pre-day-5 rows — use safe access
        F.col("discount_code") if "discount_code" in valid.columns else F.lit(None).alias("discount_code"),
        "items",  # We'll explode this in the line-items table
        "_extracted_at",
        F.col("_source_file").alias("_bronze_source_file"),
        F.col("_ingestion_timestamp").alias("_bronze_ingested_at"),
    ).withColumn("_silver_processed_at", F.current_timestamp())
    
    return valid_final, quarantine


def build_silver_orders(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    bronze_schema: str = "northwind_bronze",
    silver_schema: str = "northwind_silver",
) -> dict[str, str]:
    """Build the Silver orders fact tables and quarantine table.
    
    Returns a dict mapping logical name → full table name.
    """
    bronze_table     = f"{catalog}.{bronze_schema}.orders_raw"
    fact_orders      = f"{catalog}.{silver_schema}.fact_orders"
    fact_items       = f"{catalog}.{silver_schema}.fact_order_items"
    quarantine_table = f"{catalog}.{silver_schema}.fact_orders_quarantine"
    
    print(f"🔧 Building Silver orders")
    
    ensure_silver_schema(spark, catalog, silver_schema)
    
    bronze_df = spark.table(bronze_table)
    valid_df, quarantine_df = transform_orders(bronze_df)
    
    bronze_count   = bronze_df.count()
    valid_count    = valid_df.count()
    quarantine_count = quarantine_df.count()
    print(f"   Bronze rows: {bronze_count:,}")
    print(f"   Valid (after dedup + validation): {valid_count:,}")
    print(f"   Quarantined: {quarantine_count:,}")
    
    # Sanity: dedup loss + quarantine loss should reconcile vs Bronze
    distinct_orders = bronze_df.select("order_id").distinct().count()
    print(f"   Distinct order_ids in Bronze: {distinct_orders:,}")
    print(f"   Dedup eliminated: {bronze_count - distinct_orders:,}")
    
    # ---- Write fact_orders ----
    if not table_exists(spark, fact_orders):
        # Drop the items array — it's going into fact_order_items, not fact_orders
        (valid_df.drop("items").write
            .format("delta")
            .mode("overwrite")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(fact_orders))
        
        # Add CHECK constraints AFTER table creation (Delta requires this)
        spark.sql(f"ALTER TABLE {fact_orders} ADD CONSTRAINT positive_total CHECK (total_amount > 0)")
        spark.sql(f"ALTER TABLE {fact_orders} ADD CONSTRAINT non_null_customer CHECK (customer_id IS NOT NULL)")
        spark.sql(f"ALTER TABLE {fact_orders} ADD CONSTRAINT non_empty_items CHECK (item_count > 0)")
        print(f"✅ Created {fact_orders} with CHECK constraints")
    else:
        valid_df.drop("items").createOrReplaceTempView("orders_staging")
        merge_sql = f"""
        MERGE INTO {fact_orders} target
        USING orders_staging source
            ON target.order_id = source.order_id
        WHEN NOT MATCHED THEN INSERT *
        """
        # Note: orders are append-only — no UPDATE clause
        # If an order_id is already in Silver, we keep the original version
        result = spark.sql(merge_sql)
        result.show(truncate=False)
        print(f"✅ Merged into {fact_orders}")
    
    # ---- Write fact_order_items (exploded line items) ----
    items_df = (valid_df
        .select(
            "order_id",
            "customer_id",
            "order_date",
            F.posexplode("items").alias("line_number", "item")
        )
        .select(
            "order_id",
            "customer_id",
            "order_date",
            (F.col("line_number") + 1).alias("line_number"),
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.unit_price").cast(DecimalType(10, 2)).alias("unit_price"),
            (F.col("item.quantity") * F.col("item.unit_price")).cast(DecimalType(12, 2)).alias("line_amount"),
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    
    if not table_exists(spark, fact_items):
        (items_df.write
            .format("delta")
            .mode("overwrite")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(fact_items))
        spark.sql(f"ALTER TABLE {fact_items} ADD CONSTRAINT positive_qty CHECK (quantity > 0)")
        spark.sql(f"ALTER TABLE {fact_items} ADD CONSTRAINT non_negative_price CHECK (unit_price >= 0)")
        print(f"✅ Created {fact_items}")
    else:
        items_df.createOrReplaceTempView("items_staging")
        merge_sql = f"""
        MERGE INTO {fact_items} target
        USING items_staging source
            ON target.order_id = source.order_id AND target.line_number = source.line_number
        WHEN NOT MATCHED THEN INSERT *
        """
        result = spark.sql(merge_sql)
        result.show(truncate=False)
        print(f"✅ Merged into {fact_items}")
    
    # ---- Write quarantine table ----
    if quarantine_count > 0:
        # Quarantine is append-only — we keep history of every reject
        quarantine_to_write = (quarantine_df
            .select(
                "order_id", "customer_id", "order_date", "status",
                "total_amount", "item_count", "_quarantine_reason",
                F.col("_source_file").alias("_bronze_source_file"),
            )
            .withColumn("_silver_processed_at", F.current_timestamp())
        )
        if not table_exists(spark, quarantine_table):
            (quarantine_to_write.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(quarantine_table))
        else:
            (quarantine_to_write.write
                .format("delta")
                .mode("append")
                .saveAsTable(quarantine_table))
        print(f"⚠️  Quarantined {quarantine_count:,} rows → {quarantine_table}")
    
    # ---- Optimize ----
    spark.sql(f"OPTIMIZE {fact_orders}")
    spark.sql(f"OPTIMIZE {fact_items}")
    
    return {
        "fact_orders": fact_orders,
        "fact_items": fact_items,
        "quarantine": quarantine_table,
    }