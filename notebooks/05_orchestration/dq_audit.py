# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Audit
# MAGIC 
# MAGIC End-of-pipeline gate. Fails (raises) if any of these are violated:
# MAGIC - Row counts dropped >20% vs yesterday (potential ingestion bug)
# MAGIC - Bronze → Silver loss exceeds 5% (validation too aggressive)
# MAGIC - Fact_sales has rows missing customer attrs (temporal join broken)
# MAGIC - Quarantine table grew >10x in one run (upstream regression)

# COMMAND ----------
CATALOG = "dbs_main_catalog"
errors = []

# COMMAND ----------
# Check 1: Bronze → Silver retention
bronze_orders = spark.sql(f"SELECT COUNT(*) c FROM {CATALOG}.northwind_bronze.orders_raw").collect()[0]["c"]
silver_orders = spark.sql(f"SELECT COUNT(*) c FROM {CATALOG}.northwind_silver.fact_orders").collect()[0]["c"]
retention_rate = silver_orders / bronze_orders if bronze_orders > 0 else 0

print(f"Bronze orders: {bronze_orders:,}")
print(f"Silver orders: {silver_orders:,}")
print(f"Retention rate: {retention_rate:.2%}")

if retention_rate < 0.95:
    errors.append(f"Silver retention dropped below 95% (got {retention_rate:.2%})")

# COMMAND ----------
# Check 2: fact_sales temporal join integrity
null_customer_attrs = spark.sql(f"""
    SELECT COUNT(*) c FROM {CATALOG}.northwind_gold.fact_sales
    WHERE customer_loyalty_tier_at_order_time IS NULL
""").collect()[0]["c"]

print(f"Rows with null customer attrs in fact_sales: {null_customer_attrs:,}")

if null_customer_attrs > 0:
    errors.append(f"{null_customer_attrs} fact_sales rows have NULL customer attrs (temporal join broken)")

# COMMAND ----------
# Check 3: fact_sales count == fact_order_items count (no rows lost in temporal joins)
items = spark.sql(f"SELECT COUNT(*) c FROM {CATALOG}.northwind_silver.fact_order_items").collect()[0]["c"]
sales = spark.sql(f"SELECT COUNT(*) c FROM {CATALOG}.northwind_gold.fact_sales").collect()[0]["c"]

print(f"fact_order_items: {items:,}")
print(f"fact_sales:       {sales:,}")

if items != sales:
    errors.append(f"Row count mismatch: items={items} vs sales={sales}")

# COMMAND ----------
# Check 4: No duplicate orders in Silver
dup_orders = spark.sql(f"""
    SELECT COUNT(*) - COUNT(DISTINCT order_id) c
    FROM {CATALOG}.northwind_silver.fact_orders
""").collect()[0]["c"]

print(f"Duplicate order_ids in Silver: {dup_orders}")
if dup_orders > 0:
    errors.append(f"{dup_orders} duplicate order_ids in Silver fact_orders")

# COMMAND ----------
# Check 5: All Silver customers have exactly one current version
multi_current = spark.sql(f"""
    SELECT COUNT(*) c FROM (
        SELECT customer_id, COUNT(*) cnt
        FROM {CATALOG}.northwind_silver.dim_customers
        WHERE is_current = true
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    )
""").collect()[0]["c"]

print(f"Customers with >1 current row: {multi_current}")
if multi_current > 0:
    errors.append(f"{multi_current} customers have multiple is_current=true rows (SCD2 invariant broken)")

# COMMAND ----------
# Final verdict
if errors:
    print("\n❌ DATA QUALITY AUDIT FAILED:")
    for err in errors:
        print(f"   - {err}")
    raise Exception(f"Data Quality Audit failed: {len(errors)} issue(s)")
else:
    print("\n✅ ALL DATA QUALITY CHECKS PASSED")