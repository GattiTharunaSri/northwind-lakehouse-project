# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Build All

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import sys
REPO_SRC = "/Workspace/Users/tharuna11072000@gmail.com/northwind-lakehouse-project/src"
if REPO_SRC not in sys.path:
    sys.path.insert(0, REPO_SRC)

from northwind.gold.dim_date import build_dim_date
from northwind.gold.fact_sales import build_fact_sales
from northwind.gold.aggregates import (
    build_daily_revenue_by_category,
    build_customer_lifetime_value,
    build_product_performance,
)

CATALOG = "dbs_main_catalog"

# COMMAND ----------
# 1. Date dimension
build_dim_date(spark, catalog=CATALOG)

# COMMAND ----------
# 2. Central fact (this depends on Silver tables)
build_fact_sales(spark, catalog=CATALOG)

# COMMAND ----------
# 3. Aggregates
build_daily_revenue_by_category(spark, catalog=CATALOG)

# COMMAND ----------
build_customer_lifetime_value(spark, catalog=CATALOG)

# COMMAND ----------
build_product_performance(spark, catalog=CATALOG)

# COMMAND ----------
# MAGIC %md ## Quick Verification

# COMMAND ----------
# Top 5 customer segments
spark.sql(f"""
    SELECT rfm_segment, 
           COUNT(*) as customers,
           ROUND(AVG(lifetime_revenue), 2) as avg_ltv,
           ROUND(SUM(lifetime_revenue), 2) as total_revenue
    FROM {CATALOG}.northwind_gold.customer_lifetime_value
    GROUP BY rfm_segment
    ORDER BY total_revenue DESC
""").show()

# COMMAND ----------
# Top categories
spark.sql(f"""
    SELECT category,
           SUM(orders)       as total_orders,
           SUM(units_sold)   as total_units,
           ROUND(SUM(revenue), 2) as total_revenue,
           ROUND(AVG(gross_margin_pct), 2) as avg_margin_pct
    FROM {CATALOG}.northwind_gold.daily_revenue_by_category
    GROUP BY category
    ORDER BY total_revenue DESC
""").show()

# COMMAND ----------
# Top 10 products
spark.sql(f"""
    SELECT product_name, category, brand, total_orders, unique_customers, 
           ROUND(total_revenue, 2) as revenue, performance_tier
    FROM {CATALOG}.northwind_gold.product_performance
    ORDER BY total_revenue DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------
# Sample temporal join verification — make sure customer attrs reflect order-time state
spark.sql(f"""
    SELECT customer_id,
           order_id,
           order_date,
           customer_loyalty_tier_at_order_time as tier_at_order,
           customer_state_at_order_time as state_at_order,
           line_amount
    FROM {CATALOG}.northwind_gold.fact_sales
    WHERE customer_id = (
        SELECT customer_id FROM {CATALOG}.northwind_silver.dim_customers
        GROUP BY customer_id HAVING COUNT(*) > 1 LIMIT 1
    )
    ORDER BY order_date
    LIMIT 10
""").show(truncate=False)