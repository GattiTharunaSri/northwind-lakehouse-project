# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5.1: Groups and Grants
# MAGIC 
# MAGIC Sets up the access-control structure for the Northwind lakehouse.

# COMMAND ----------
CATALOG = "dbs_main_catalog"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Identify Personas
# MAGIC 
# MAGIC We define four personas. In a real org these map to AD/IdP groups.
# MAGIC We won't actually create users — just establish the patterns.

# COMMAND ----------
# MAGIC %md
# MAGIC | Persona | Description | Access Required |
# MAGIC |---|---|---|
# MAGIC | `northwind_engineers` | Build & maintain pipelines | Full access to all 3 layers |
# MAGIC | `northwind_analysts` | Build dashboards, query Gold | Read-only on Gold + Silver |
# MAGIC | `northwind_executives` | View dashboards only | Read-only on Gold |
# MAGIC | `northwind_pii_readers` | Customer support; can see clear PII | Same as analysts + see unmasked PII |

# COMMAND ----------
# MAGIC %md ## Step 2: Inspect Existing Grants

# COMMAND ----------
# Who currently has access to our catalog?
spark.sql(f"SHOW GRANTS ON CATALOG {CATALOG}").show(truncate=False)

# COMMAND ----------
spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.northwind_gold").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Document the Grants We Would Issue
# MAGIC 
# MAGIC In a real workspace with the groups created, here's what we'd run.
# MAGIC We'll execute these against placeholder groups — they may fail if the
# MAGIC groups don't exist, which is fine for the demonstration. The grants 
# MAGIC themselves are the deliverable.

# COMMAND ----------
# A helper to attempt a grant gracefully — succeeds if group exists, logs if not
def safe_grant(sql: str):
    try:
        spark.sql(sql)
        print(f"✅ {sql}")
    except Exception as e:
        msg = str(e)[:150]
        if "does not exist" in msg or "PRINCIPAL_DOES_NOT_EXIST" in msg:
            print(f"⏭️  Skipped (group not provisioned): {sql[:80]}...")
        else:
            print(f"❌ Failed: {sql[:80]}... → {msg}")

# COMMAND ----------
# MAGIC %md ### Engineers: full access to all layers

# COMMAND ----------
for schema in ["northwind_bronze", "northwind_silver", "northwind_gold", "northwind_silver_dlt"]:
    safe_grant(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO `northwind_engineers`")
    safe_grant(f"GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA {CATALOG}.{schema} TO `northwind_engineers`")

# COMMAND ----------
# MAGIC %md ### Analysts: read-only on Silver + Gold

# COMMAND ----------
safe_grant(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `northwind_analysts`")
for schema in ["northwind_silver", "northwind_gold"]:
    safe_grant(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO `northwind_analysts`")
    safe_grant(f"GRANT SELECT ON SCHEMA {CATALOG}.{schema} TO `northwind_analysts`")

# COMMAND ----------
# MAGIC %md ### Executives: Gold only

# COMMAND ----------
safe_grant(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `northwind_executives`")
safe_grant(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.northwind_gold TO `northwind_executives`")
safe_grant(f"GRANT SELECT ON SCHEMA {CATALOG}.northwind_gold TO `northwind_executives`")

# COMMAND ----------
# MAGIC %md ### PII Readers: same as analysts + the PII unmask permission
# MAGIC We'll define that permission in the next notebook (column masks).

# COMMAND ----------
safe_grant(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `northwind_pii_readers`")
for schema in ["northwind_silver", "northwind_gold"]:
    safe_grant(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{schema} TO `northwind_pii_readers`")
    safe_grant(f"GRANT SELECT ON SCHEMA {CATALOG}.{schema} TO `northwind_pii_readers`")

# COMMAND ----------
# MAGIC %md ## Step 4: Document With Comments
# MAGIC 
# MAGIC Tables should have business descriptions so analysts know what they're looking at.

# COMMAND ----------
spark.sql(f"""
    ALTER TABLE {CATALOG}.northwind_gold.fact_sales
    SET TBLPROPERTIES (
        'comment' = 'Central sales fact. One row per order line. Joined to point-in-time customer and product attributes.'
    )
""")

spark.sql(f"""
    COMMENT ON COLUMN {CATALOG}.northwind_gold.fact_sales.customer_loyalty_tier_at_order_time
    IS 'Customer loyalty tier as of the order date (SCD2 temporal join). May differ from the customer''s current tier.'
""")

spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.northwind_silver.dim_customers
    IS 'SCD Type 2 customer dimension. Use effective_from/effective_to for point-in-time joins. PII columns (email/phone) are SHA-256 hashed.'
""")

print("✅ Added documentation to key tables and columns")

# COMMAND ----------
# Verify the comments are visible
spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.northwind_gold.fact_sales").filter("col_name = 'Comment' OR col_name = ''").show(truncate=False)