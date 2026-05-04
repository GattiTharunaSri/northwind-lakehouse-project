# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generator Runner
# MAGIC
# MAGIC Generates simulated data for Day N of the Northwind retail simulation,
# MAGIC and writes it to the Bronze landing volume in JSON/CSV/Parquet formats.

# COMMAND ----------

# MAGIC %pip install faker==30.3.0
# MAGIC %restart_python

# COMMAND ----------

import json
import sys
from pathlib import Path

# Add the repo's src/ to path — adjust if your repo path differs
# When using Databricks Repos / Git folders, this should be /Workspace/Repos/<you>/<repo>/src
REPO_SRC = "/Workspace/Users/tharuna11072000@gmail.com/northwind-lakehouse-project/src"
if REPO_SRC not in sys.path:
    sys.path.insert(0, REPO_SRC)

from northwind.generators.orders import generate_orders_batch
from northwind.generators.products import generate_products_snapshot
from northwind.generators.customers import generate_customers_snapshot
from northwind.generators.clickstream import generate_clickstream_batch

# COMMAND ----------

# Parameters — set via Databricks widgets so we can replay any day
dbutils.widgets.text("day_offset", "10", "Simulation Day Offset")
dbutils.widgets.text("orders_per_batch", "500", "Orders per Batch")
dbutils.widgets.text("clickstream_per_batch", "2000", "Clickstream Events")
dbutils.widgets.text("num_batches", "3", "Mini-batches to write")

day_offset = int(dbutils.widgets.get("day_offset"))
orders_per_batch = int(dbutils.widgets.get("orders_per_batch"))
clickstream_per_batch = int(dbutils.widgets.get("clickstream_per_batch"))
num_batches = int(dbutils.widgets.get("num_batches"))

CATALOG = "dbs_main_catalog"
BASE_PATH = f"/Volumes/{CATALOG}/northwind_bronze/landing"

print(f"Generating Day {day_offset} data → {BASE_PATH}")

# COMMAND ----------

# 1. ORDERS — write multiple JSONL files to simulate mini-batches throughout the day
import uuid
orders_path = f"{BASE_PATH}/orders"
dbutils.fs.mkdirs(orders_path)

for batch_num in range(num_batches):
    orders = generate_orders_batch(day_offset=day_offset, batch_size=orders_per_batch)
    file_name = f"orders_day{day_offset:03d}_batch{batch_num:02d}_{uuid.uuid4().hex[:8]}.jsonl"
    file_path = f"{orders_path}/{file_name}"
    
    # Write line-by-line as JSONL
    content = "\n".join(json.dumps(o, default=str) for o in orders)
    dbutils.fs.put(file_path, content, overwrite=True)
    print(f"  ✅ Wrote {len(orders)} orders → {file_name}")

# COMMAND ----------

# 2. PRODUCTS — daily CSV snapshot
import csv
import io

products = generate_products_snapshot(day_offset=day_offset)
products_path = f"{BASE_PATH}/products/products_day{day_offset:03d}.csv"

# Flatten — CSVs don't do nested structures
buffer = io.StringIO()
writer = csv.DictWriter(buffer, fieldnames=products[0].keys())
writer.writeheader()
writer.writerows(products)

dbutils.fs.put(products_path, buffer.getvalue(), overwrite=True)
print(f"✅ Wrote {len(products)} products → {products_path}")

# COMMAND ----------

# 3. CUSTOMERS — weekly Parquet snapshot (only on day 0, 7, 14, ...)
if day_offset % 7 == 0:
    customers = generate_customers_snapshot(day_offset=day_offset)
    customers_path = f"{BASE_PATH}/customers"
    
    # Use Spark to write as Parquet — easiest in Databricks
    df = spark.createDataFrame(customers)
    week_num = day_offset // 7
    df.coalesce(1).write.mode("overwrite").parquet(f"{customers_path}/week_{week_num:03d}")
    print(f"✅ Wrote {len(customers)} customers → week_{week_num:03d}/")
else:
    print(f"⏭️  Skipping customers (only generated weekly; day {day_offset} is not a snapshot day)")

# COMMAND ----------

# 4. CLICKSTREAM — high-volume JSONL with possible corruption
clickstream_path = f"{BASE_PATH}/clickstream"
dbutils.fs.mkdirs(clickstream_path)

for batch_num in range(num_batches * 2):  # Higher frequency than orders
    events = generate_clickstream_batch(day_offset=day_offset, batch_size=clickstream_per_batch)
    file_name = f"clickstream_day{day_offset:03d}_batch{batch_num:02d}_{uuid.uuid4().hex[:8]}.jsonl"
    file_path = f"{clickstream_path}/{file_name}"
    
    # Inject ~0.1% malformed lines for testing
    import random
    lines = []
    for e in events:
        line = json.dumps(e, default=str)
        if random.random() < 0.001:
            line = line[: len(line) // 2]  # Truncate
        lines.append(line)
    
    dbutils.fs.put(file_path, "\n".join(lines), overwrite=True)
    print(f"  ✅ Wrote {len(events)} clickstream events → {file_name}")

# COMMAND ----------

# Verify everything landed
print("\n=== Volume contents ===")
for sub in ["orders", "products", "customers", "clickstream"]:
    files = dbutils.fs.ls(f"{BASE_PATH}/{sub}")
    print(f"\n{sub}: {len(files)} files")
    for f in files[:3]:
        print(f"  {f.name} ({f.size:,} bytes)")
