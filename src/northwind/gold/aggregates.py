"""Gold layer: pre-computed aggregates for dashboards and analytics."""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_daily_revenue_by_category(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    gold_schema: str = "northwind_gold",
) -> str:
    """Daily revenue & order counts, broken out by product category."""
    target_table = f"{catalog}.{gold_schema}.daily_revenue_by_category"
    
    print(f"📊 Building {target_table}")
    
    df = spark.sql(f"""
        SELECT
            date_key,
            category,
            subcategory,
            COUNT(DISTINCT order_id) as orders,
            SUM(quantity)            as units_sold,
            SUM(line_amount)         as revenue,
            SUM(quantity * cost_at_order_time) as total_cost,
            SUM(line_amount) - SUM(quantity * cost_at_order_time) as gross_margin,
            ROUND(
                (SUM(line_amount) - SUM(quantity * cost_at_order_time)) 
                / NULLIF(SUM(line_amount), 0) * 100, 2
            ) as gross_margin_pct
        FROM {catalog}.{gold_schema}.fact_sales
        WHERE category IS NOT NULL
        GROUP BY d 
    """)
    
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy("date_key", "category")
        .saveAsTable(target_table))
    
    count = spark.table(target_table).count()
    print(f"✅ {target_table}: {count:,} (date, category, subcategory) groups")
    return target_table


def build_customer_lifetime_value(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    gold_schema: str = "northwind_gold",
) -> str:
    """RFM analysis: Recency, Frequency, Monetary value per customer."""
    target_table = f"{catalog}.{gold_schema}.customer_lifetime_value"
    
    print(f"💎 Building {target_table}")
    
    df = spark.sql(f"""
        WITH base AS (
            SELECT
                customer_id,
                MAX(order_date) as last_order_date,
                MIN(order_date) as first_order_date,
                COUNT(DISTINCT order_id) as total_orders,
                SUM(line_amount) as lifetime_revenue,
                AVG(line_amount * quantity) as avg_line_value,
                COUNT(DISTINCT date_key) as distinct_order_days,
                MAX(customer_loyalty_tier_at_order_time) as latest_loyalty_tier
            FROM {catalog}.{gold_schema}.fact_sales
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
        )
        SELECT
            *,
            DATEDIFF(CURRENT_DATE(), DATE(last_order_date)) as days_since_last_order,
            DATEDIFF(DATE(last_order_date), DATE(first_order_date)) as customer_tenure_days,
            -- RFM scoring (1-5 quintiles)
            NTILE(5) OVER (ORDER BY DATEDIFF(CURRENT_DATE(), DATE(last_order_date)) DESC) as recency_score,
            NTILE(5) OVER (ORDER BY total_orders ASC) as frequency_score,
            NTILE(5) OVER (ORDER BY lifetime_revenue ASC) as monetary_score
        FROM base
    """)
    
    df = df.withColumn(
        "rfm_segment",
        F.when((F.col("recency_score") >= 4) & (F.col("frequency_score") >= 4) & (F.col("monetary_score") >= 4), "CHAMPION")
         .when((F.col("recency_score") >= 4) & (F.col("monetary_score") >= 4), "BIG_SPENDER")
         .when(F.col("recency_score") >= 4, "RECENT")
         .when((F.col("recency_score") <= 2) & (F.col("frequency_score") >= 4), "AT_RISK")
         .when(F.col("recency_score") <= 1, "LOST")
         .otherwise("REGULAR")
    )
    
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy("rfm_segment", "customer_id")
        .saveAsTable(target_table))
    
    count = spark.table(target_table).count()
    print(f"✅ {target_table}: {count:,} customers")
    return target_table


def build_product_performance(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    gold_schema: str = "northwind_gold",
) -> str:
    """Per-product sales metrics — velocity, margin, customer reach."""
    target_table = f"{catalog}.{gold_schema}.product_performance"
    
    print(f"📦 Building {target_table}")
    
    df = spark.sql(f"""
        SELECT
            product_id,
            ANY_VALUE(product_name) as product_name,
            ANY_VALUE(category) as category,
            ANY_VALUE(brand) as brand,
            COUNT(DISTINCT order_id)    as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(quantity)               as total_units_sold,
            SUM(line_amount)            as total_revenue,
            AVG(unit_price)             as avg_selling_price,
            ROUND(
                SUM(line_amount) / COUNT(DISTINCT date_key), 2
            ) as avg_daily_revenue,
            COUNT(DISTINCT date_key)    as days_with_sales,
            MAX(date_key)               as last_sale_date
        FROM {catalog}.{gold_schema}.fact_sales
        WHERE product_id IS NOT NULL
        GROUP BY product_id
    """)
    
    df = df.withColumn(
        "performance_tier",
        F.when(F.col("total_revenue") > 50_000, "TOP")
         .when(F.col("total_revenue") > 10_000, "MID")
         .when(F.col("total_revenue") > 1_000,  "LOW")
         .otherwise("MINIMAL")
    )
    
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy("category", "performance_tier")
        .saveAsTable(target_table))
    
    count = spark.table(target_table).count()
    print(f"✅ {target_table}: {count:,} products")
    return target_table