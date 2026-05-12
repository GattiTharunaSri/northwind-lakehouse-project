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
# MAGIC %md ## Ingest Products

# COMMAND ----------

ingest_products(spark, catalog=CATALOG, schema=SCHEMA)