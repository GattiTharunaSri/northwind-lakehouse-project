# Databricks notebook source
# MAGIC %md
# MAGIC # Run All Bronze Ingestion Pipelines
# MAGIC 
# MAGIC Executes all four Bronze ingestion jobs in sequence using availableNow triggers.

# COMMAND ----------
import sys

# Adjust to YOUR actual repo path. Find it via:
#   import os; print(os.listdir("/Workspace/Repos"))
# or: print(os.listdir("/Workspace/Users"))
REPO_SRC = "/Workspace/Users/tharuna11072000@gmail.com/northwind_lakehouse-project/src"
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
ingest_customers(spark, catalog=CATALOG, schema=SCHEMA)

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