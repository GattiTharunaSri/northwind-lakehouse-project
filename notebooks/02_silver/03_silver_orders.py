# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Orders + Order Items + Quarantine

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
REPO_SRC = "/Workspace/Users/tharuna11072000@gmail.com/northwind-lakehouse-project/src"
if REPO_SRC not in sys.path:
    sys.path.insert(0, REPO_SRC)

from northwind.silver.orders import build_silver_orders

CATALOG = "dbs_main_catalog"

# COMMAND ----------

results = build_silver_orders(spark=spark, catalog=CATALOG)
print(results)

# COMMAND ----------

# MAGIC %md ## Verification

# COMMAND ----------

from pyspark.sql import functions as F

fact_orders     = results["fact_orders"]
fact_items      = results["fact_items"]
quarantine_tbl  = results["quarantine"]

# 1. Uniqueness — should be 100%
total = spark.table(fact_orders).count()
distinct = spark.table(fact_orders).select("order_id").distinct().count()
print(f"fact_orders rows:           {total:,}")
print(f"fact_orders unique order_id: {distinct:,}")
print(f"Uniqueness: {'✅ PASS' if total == distinct else '❌ FAIL'}")

# COMMAND ----------

# 2. Quarantine breakdown — should match the dirt we injected
spark.sql(f"""
    SELECT _quarantine_reason, COUNT(*) as count
    FROM {quarantine_tbl}
    GROUP BY _quarantine_reason
    ORDER BY count DESC
""").show()

# COMMAND ----------

# 3. Order items uniqueness
items_total = spark.table(fact_items).count()
items_unique = spark.sql(f"""
    SELECT COUNT(*) as c FROM (SELECT DISTINCT order_id, line_number FROM {fact_items})
""").collect()[0]["c"]
print(f"fact_order_items rows: {items_total:,}")
print(f"Unique (order_id, line_number): {items_unique:,}")
print(f"Uniqueness: {'✅ PASS' if items_total == items_unique else '❌ FAIL'}")

# COMMAND ----------

# 4. Test the CHECK constraint — try to insert a bad row, should fail
try:
    spark.sql(f"""
        INSERT INTO {fact_orders} VALUES (
            'BAD_ORDER_001', 'CUST_TEST', current_timestamp(), 'PENDING',
            -100.00, 1, 'CARD',
            'Test', 'Test', 'TS', '00000', 'US', NULL,
            current_timestamp(), '/test', current_timestamp(), current_timestamp()
        )
    """)
    print("❌ Bad row was INSERTED — constraint failed!")
except Exception as e:
    print(f"✅ Bad row REJECTED by CHECK constraint:")
    print(f"   {str(e)[:200]}")

# COMMAND ----------

# 5. Sample of valid orders
spark.sql(f"""
    SELECT order_id, customer_id, order_date, status, total_amount, item_count
    FROM {fact_orders}
    ORDER BY order_date DESC
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# 6. Sample of order items
spark.sql(f"""
    SELECT order_id, line_number, product_id, quantity, unit_price, line_amount
    FROM {fact_items}
    WHERE order_id IN (SELECT order_id FROM {fact_orders} LIMIT 1)
    ORDER BY line_number
""").show()

# COMMAND ----------

CATALOG = "dbs_main_catalog"
bronze_orders = spark.table(f"{CATALOG}.northwind_bronze.orders_raw")

print("=== Full schema ===")
bronze_orders.printSchema()

print("\n=== Sample of shipping_address ===")
bronze_orders.select("shipping_address").show(3, truncate=False)

print("\n=== Sample of items ===")
bronze_orders.select("items").show(3, truncate=False)
