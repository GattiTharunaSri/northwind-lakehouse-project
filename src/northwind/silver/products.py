"""Silver layer for products.

Pattern: incremental MERGE from Bronze.
Each (product_id, last_updated) is unique — we capture every observed snapshot.
"""
from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from ._utils import ensure_silver_schema, table_exists


def transform_products(bronze_df: DataFrame) -> DataFrame:
    """Apply Silver-layer transformations to raw products.
    
    Responsibilities:
    1. Type casting (decimals for money, date for dates)
    2. Derived columns (margin_pct, price_band)
    3. Drop bad rows (null PK, non-positive price/cost)
    4. Add audit columns
    """
    return (bronze_df
        # --- Type casting ---
        .withColumn("price", F.col("price").cast(DecimalType(10, 2)))
        .withColumn("cost", F.col("cost").cast(DecimalType(10, 2)))
        .withColumn("weight_kg", F.col("weight_kg").cast(DecimalType(8, 3)))
        .withColumn("last_updated", F.to_date("last_updated"))
        .withColumn("is_active", F.col("is_active").cast("boolean"))
        
        # --- Derived columns ---
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
        
        # --- Data quality filtering ---
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("price") > 0)
        .filter(F.col("cost") >= 0)
        
        # --- Final column selection ---
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "price",
            "cost",
            "margin_pct",
            "price_band",
            "weight_kg",
            "is_active",
            "last_updated",
            # Carry over Bronze lineage for traceability
            F.col("_source_file").alias("_bronze_source_file"),
            F.col("_ingestion_timestamp").alias("_bronze_ingested_at"),
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
    )


def build_silver_products(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    bronze_schema: str = "northwind_bronze",
    silver_schema: str = "northwind_silver",
    target_table: str = "products",
) -> str:
    """Build or incrementally update the Silver products table.
    
    Uses a CONDITIONAL merge: only updates rows when business columns actually changed.
    This produces true idempotent behavior — re-running with no new data = 0 updates.
    """
    bronze_table = f"{catalog}.{bronze_schema}.products_raw"
    silver_table = f"{catalog}.{silver_schema}.{target_table}"
    
    print(f"🔧 Building Silver products: {silver_table}")
    
    ensure_silver_schema(spark, catalog, silver_schema)
    
    bronze_df = spark.table(bronze_table)
    silver_df = transform_products(bronze_df)
    
    print(f"   Bronze rows: {bronze_df.count():,}")
    print(f"   After Silver transform: {silver_df.count():,}")
    
    if not table_exists(spark, silver_table):
        (silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(silver_table))
        print(f"✅ Created {silver_table} with {silver_df.count():,} rows")
    else:
        silver_df.createOrReplaceTempView("silver_products_staging")
        
        # Conditional MERGE: only update when business columns differ.
        # Audit columns (_silver_processed_at, _bronze_*) are intentionally
        # excluded from the change detection — they always differ but aren't
        # "real" changes.
        merge_sql = f"""
        MERGE INTO {silver_table} target
        USING silver_products_staging source
            ON  target.product_id = source.product_id
            AND target.last_updated = source.last_updated
        WHEN MATCHED AND (
            target.product_name      <=> source.product_name      = false OR
            target.category          <=> source.category          = false OR
            target.subcategory       <=> source.subcategory       = false OR
            target.brand             <=> source.brand             = false OR
            target.price             <=> source.price             = false OR
            target.cost              <=> source.cost              = false OR
            target.margin_pct        <=> source.margin_pct        = false OR
            target.price_band        <=> source.price_band        = false OR
            target.weight_kg         <=> source.weight_kg         = false OR
            target.is_active         <=> source.is_active         = false
        )
        THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        result = spark.sql(merge_sql)
        result.show(truncate=False)
        print(f"✅ Merged into {silver_table}")
    
    spark.sql(f"OPTIMIZE {silver_table}")
    
    return silver_table