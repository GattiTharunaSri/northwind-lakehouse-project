"""Gold layer: fact_sales — denormalized sales fact with point-in-time dimension joins.

The killer feature: we join customer & product attributes AS THEY WERE 
on the order date, not as they are today. This preserves analytical truth.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_fact_sales(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    silver_schema: str = "northwind_silver",
    gold_schema: str = "northwind_gold",
) -> str:
    """Build the central sales fact with point-in-time joins to dimensions.
    
    Grain: one row per (order_id, line_number) — the same as fact_order_items.
    """
    target_table = f"{catalog}.{gold_schema}.fact_sales"
    
    print(f"💰 Building {target_table}")
    
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")
    
    # Source tables
    items     = spark.table(f"{catalog}.{silver_schema}.fact_order_items")
    orders    = spark.table(f"{catalog}.{silver_schema}.fact_orders")
    customers = spark.table(f"{catalog}.{silver_schema}.dim_customers")
    products  = spark.table(f"{catalog}.{silver_schema}.products")
    
    # ========================================================================
    # THE TEMPORAL JOIN — this is the moneymaker
    # ========================================================================
    # Join order items + orders first
    items_with_order = (items.alias("i")
        .join(orders.alias("o"), on="order_id", how="inner")
        .select(
            F.col("i.order_id"),
            F.col("i.line_number"),
            F.col("i.product_id"),
            F.col("i.quantity"),
            F.col("i.unit_price"),
            F.col("i.line_amount"),
            F.col("o.customer_id"),
            F.col("o.order_date"),
            F.col("o.status").alias("order_status"),
            F.col("o.payment_method"),
            F.col("o.ship_state"),
            F.col("o.ship_country"),
            F.to_date(F.col("o.order_date")).alias("order_date_key"),
        )
    )
    
    # Now the temporal join to customers
    # We want the customer version where:
    #   effective_from <= order_date <= effective_to
    sales_with_customer = (items_with_order.alias("s")
        .join(
            customers.alias("c"),
            (F.col("s.customer_id") == F.col("c.customer_id")) &
            (F.col("s.order_date_key") >= F.col("c.effective_from")) &
            (F.col("s.order_date_key") <= F.col("c.effective_to")),
            how="left"  # left join — keep order even if customer dim doesn't have a match
        )
        .select(
            F.col("s.*"),
            F.col("c.first_name").alias("customer_first_name"),
            F.col("c.last_name").alias("customer_last_name"),
            F.col("c.loyalty_tier").alias("customer_loyalty_tier_at_order_time"),
            F.col("c.address_state").alias("customer_state_at_order_time"),
            F.col("c.address_country").alias("customer_country_at_order_time"),
            F.col("c.preferred_language").alias("customer_language_at_order_time"),
        )
    )
    
    # Now the temporal join to products (price/category as of the order date)
    # Products dim has one row per (product_id, last_updated) — pick the row
    # whose last_updated is the most recent BEFORE the order_date.
    
    # Step 1: For each (order line, product), find the product version active at order time
    products_aliased = products.select(
        F.col("product_id").alias("p_product_id"),
        F.col("last_updated").alias("p_effective_from"),
        F.col("product_name"),
        F.col("category"),
        F.col("subcategory"),
        F.col("brand"),
        F.col("price").alias("price_at_order_time"),
        F.col("cost").alias("cost_at_order_time"),
        F.col("margin_pct").alias("margin_pct_at_order_time"),
        F.col("price_band").alias("price_band_at_order_time"),
    )
    
    # Use a range join — match each sale to the product version whose 
    # last_updated <= order_date_key, then keep only the latest match.
    # Implementation: join then row_number to pick the latest.
    from pyspark.sql.window import Window
    
    sales_with_product_candidates = (sales_with_customer.alias("s")
        .join(
            products_aliased.alias("p"),
            (F.col("s.product_id") == F.col("p.p_product_id")) &
            (F.col("p.p_effective_from") <= F.col("s.order_date_key")),
            how="left"
        )
    )
    
    # Pick the latest matching product version per (order_id, line_number)
    pick_window = Window.partitionBy("order_id", "line_number").orderBy(
        F.desc("p_effective_from")
    )
    
    final = (sales_with_product_candidates
        .withColumn("_rn", F.row_number().over(pick_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "p_product_id", "p_effective_from")
        .withColumn("_gold_processed_at", F.current_timestamp())
    )
    
    # Drop the working column
    final = final.drop("order_date_key")
    
    # Add it back as the date_key for joining to dim_date
    final = final.withColumn("date_key", F.to_date(F.col("order_date")))
    
    # ========================================================================
    # Write with Liquid Clustering for query performance
    # ========================================================================
    (final.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy("date_key", "customer_id", "product_id")  # Liquid Clustering
        .saveAsTable(target_table))
    
    spark.sql(f"OPTIMIZE {target_table}")
    
    count = spark.table(target_table).count()
    print(f"✅ {target_table}: {count:,} sales rows")
    
    # Quick sanity check
    null_customer_attrs = spark.sql(f"""
        SELECT COUNT(*) as c FROM {target_table}
        WHERE customer_loyalty_tier_at_order_time IS NULL
    """).collect()[0]["c"]
    null_product_attrs = spark.sql(f"""
        SELECT COUNT(*) as c FROM {target_table}
        WHERE category IS NULL
    """).collect()[0]["c"]
    print(f"   Rows missing customer attrs: {null_customer_attrs:,} (expect 0 if all customers were in dim)")
    print(f"   Rows missing product attrs:  {null_product_attrs:,} (expect 0)")
    
    return target_table