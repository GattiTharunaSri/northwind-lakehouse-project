# Databricks notebook source
# MAGIC %md
# MAGIC # Run All Bronze Ingestion Pipelines
# MAGIC
# MAGIC Executes all four Bronze ingestion jobs in sequence using availableNow triggers.

# COMMAND ----------

# DBTITLE 1,Cell 2
import sys

# Adjust to YOUR actual repo path. Find it via:
#   import os; print(os.listdir("/Workspace/Repos"))
# or: print(os.listdir("/Workspace/Users"))
REPO_SRC = "/Workspace/Users/tharuna11072000@gmail.com/northwind-lakehouse-project/src"
if REPO_SRC not in sys.path:
    sys.path.insert(0, REPO_SRC)

from northwind.bronze.orders import ingest_orders
from northwind.bronze.products import ingest_products
from northwind.bronze.customers import ingest_customers
from northwind.bronze.clickstream import ingest_clickstream

CATALOG = "dbs_main_catalog"
SCHEMA = "northwind_bronze"

# COMMAND ----------

# MAGIC %md ## Ingest Orders

# COMMAND ----------

ingest_orders(spark, catalog=CATALOG, schema=SCHEMA)

# COMMAND ----------

# MAGIC %md ## Ingest Products

# COMMAND ----------

ingest_products(spark, catalog=CATALOG, schema=SCHEMA)

# COMMAND ----------

# MAGIC %md ## Ingest Customers

# COMMAND ----------

ingest_customers(spark, catalog=CATALOG, schema=SCHEMA)  # Verify catalog and schema, ensure data exists at the specified location.

# COMMAND ----------

# MAGIC %md ## Ingest Clickstream

# COMMAND ----------

ingest_clickstream(spark, catalog=CATALOG, schema=SCHEMA, streaming=False)

# COMMAND ----------

# MAGIC %md ## Verify all Bronze tables

# COMMAND ----------

for table in ["orders_raw", "products_raw", "customers_raw", "clickstream_raw"]:
    full_name = f"{CATALOG}.{SCHEMA}.{table}"
    count = spark.table(full_name).count()
    print(f"{full_name}: {count:,} rows")
    
    # Show the lineage metadata to prove it landed correctly
    spark.sql(f"""
        SELECT _source_file, COUNT(*) as rows, MIN(_ingestion_timestamp) as first_ingest
        FROM {full_name}
        GROUP BY _source_file
        ORDER BY first_ingest
        LIMIT 5
    """).show(truncate=False)

# COMMAND ----------

for table in ["orders_raw", "products_raw", "customers_raw", "clickstream_raw"]:
    full_name = f"{CATALOG}.{SCHEMA}.{table}"
    count = spark.table(full_name).count()
    print(f"{full_name}: {count:,} rows")

# COMMAND ----------

from pyspark.sql import functions as F

# Find customers whose address changed between week 0 and week 2
customers_df = spark.table(f"{CATALOG}.{SCHEMA}.customers_raw")

print("Snapshots in Bronze:")
customers_df.groupBy("_snapshot_date").count().orderBy("_snapshot_date").show()

# How many customers have multiple distinct addresses across snapshots?
addr_changes = (customers_df
    .groupBy("customer_id")
    .agg(F.countDistinct(F.col("address.street")).alias("distinct_addrs"))
    .filter(F.col("distinct_addrs") > 1))

print(f"Customers with address changes across snapshots: {addr_changes.count():,}")
addr_changes.show(5)

# How many have loyalty tier changes?
tier_changes = (customers_df
    .groupBy("customer_id")
    .agg(F.countDistinct("loyalty_tier").alias("distinct_tiers"))
    .filter(F.col("distinct_tiers") > 1))

print(f"Customers with tier changes across snapshots: {tier_changes.count():,}")

# COMMAND ----------

CATALOG = "dbs_main_catalog"
SCHEMA = "northwind_bronze"

# Remove existing customer landing files (we'll regenerate)
dbutils.fs.rm(f"/Volumes/{CATALOG}/{SCHEMA}/landing/customers", recurse=True)

# Drop the Bronze table and its checkpoint/schema
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.customers_raw")
dbutils.fs.rm(f"/Volumes/{CATALOG}/{SCHEMA}/_internal/_checkpoints/bronze_customers", recurse=True)
dbutils.fs.rm(f"/Volumes/{CATALOG}/{SCHEMA}/_internal/_schemas/customers", recurse=True)

print("✅ Customer state cleared")
