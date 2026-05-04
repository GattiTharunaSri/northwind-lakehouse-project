# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Customers (SCD Type 2)

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import sys
REPO_SRC = "/Workspace/Users/tharuna11072000@gmail.com/northwind-lakehouse-project/src"
if REPO_SRC not in sys.path:
    sys.path.insert(0, REPO_SRC)

from northwind.silver.customers import build_silver_customers

CATALOG = "dbs_main_catalog"

# COMMAND ----------
silver_table = build_silver_customers(
    spark=spark,
    catalog=CATALOG,
    bronze_schema="northwind_bronze",
    silver_schema="northwind_silver",
)

# COMMAND ----------
# MAGIC %md ## Verify the SCD2 Structure

# COMMAND ----------
# Total versions vs current customers
spark.sql(f"""
    SELECT 
        COUNT(*) as total_versions,
        SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_rows,
        COUNT(DISTINCT customer_id) as distinct_customers
    FROM {silver_table}
""").show()

# COMMAND ----------
# Distribution of versions per customer — most should have 1, some 2, fewer 3
spark.sql(f"""
    SELECT versions_per_customer, COUNT(*) as customers
    FROM (
        SELECT customer_id, COUNT(*) as versions_per_customer
        FROM {silver_table}
        GROUP BY customer_id
    )
    GROUP BY versions_per_customer
    ORDER BY versions_per_customer
""").show()

# COMMAND ----------
# Pull a customer with multiple versions to see SCD2 in action
sample = spark.sql(f"""
    SELECT customer_id 
    FROM {silver_table} 
    GROUP BY customer_id 
    HAVING COUNT(*) > 1 
    LIMIT 1
""").collect()[0]["customer_id"]

print(f"Customer {sample} version history:\n")
spark.sql(f"""
    SELECT customer_id, address_city, loyalty_tier, 
           effective_from, effective_to, is_current
    FROM {silver_table}
    WHERE customer_id = '{sample}'
    ORDER BY effective_from
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md ## Test a Temporal Join
# MAGIC 
# MAGIC The whole point of SCD2: "What did this customer look like on a specific date?"

# COMMAND ----------
spark.sql(f"""
    SELECT customer_id, address_city, loyalty_tier, effective_from, effective_to
    FROM {silver_table}
    WHERE customer_id = '{sample}'
      AND date('2025-01-10') BETWEEN effective_from AND effective_to
""").show(truncate=False)

# COMMAND ----------
spark.sql(f"""
    SELECT customer_id, address_city, loyalty_tier, effective_from, effective_to
    FROM {silver_table}
    WHERE customer_id = '{sample}'
      AND date('2025-01-20') BETWEEN effective_from AND effective_to
""").show(truncate=False)