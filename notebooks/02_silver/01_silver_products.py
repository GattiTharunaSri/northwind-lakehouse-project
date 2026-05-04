# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Products
# MAGIC 
# MAGIC Builds the Silver products table from Bronze using MERGE for idempotent updates.

# COMMAND ----------
import sys

REPO_SRC = "/Workspace/Users/tharuna11072000@gmail.com/northwind_lakehouse-project/src"
if REPO_SRC not in sys.path:
    sys.path.insert(0, REPO_SRC)

from northwind.silver.products import build_silver_products

CATALOG = "dbs_main_catalog"
BRONZE_SCHEMA = "northwind_bronze"
SILVER_SCHEMA = "northwind_silver"

# COMMAND ----------
# MAGIC %md ## Run the build

# COMMAND ----------
silver_table = build_silver_products(
    spark=spark,
    catalog=CATALOG,
    bronze_schema=BRONZE_SCHEMA,
    silver_schema=SILVER_SCHEMA,
)

# COMMAND ----------
# MAGIC %md ## Verify

# COMMAND ----------
# Row count and uniqueness
total = spark.table(silver_table).count()
unique_keys = spark.sql(f"""
    SELECT COUNT(*) as unique_keys 
    FROM (SELECT DISTINCT product_id, last_updated FROM {silver_table})
""").collect()[0]["unique_keys"]

print(f"Total rows:  {total:,}")
print(f"Unique keys: {unique_keys:,}")
print(f"Uniqueness: {'✅ PASS' if total == unique_keys else '❌ FAIL — duplicates exist'}")

# COMMAND ----------
# Sample data with derived columns
spark.sql(f"""
    SELECT product_id, product_name, category, price, cost, margin_pct, price_band, last_updated
    FROM {silver_table}
    ORDER BY product_id, last_updated
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------
# Distribution of price bands
spark.sql(f"""
    SELECT price_band, COUNT(*) as products, ROUND(AVG(price), 2) as avg_price
    FROM {silver_table}
    GROUP BY price_band
    ORDER BY avg_price
""").show()

# COMMAND ----------
# A glimpse at price changes for a single product
spark.sql(f"""
    SELECT product_id, last_updated, price, cost, margin_pct
    FROM {silver_table}
    WHERE product_id IN (
        SELECT product_id FROM (
            SELECT product_id, COUNT(DISTINCT price) as price_versions
            FROM {silver_table}
            GROUP BY product_id
            HAVING COUNT(DISTINCT price) > 1
        ) LIMIT 3
    )
    ORDER BY product_id, last_updated
""").show()