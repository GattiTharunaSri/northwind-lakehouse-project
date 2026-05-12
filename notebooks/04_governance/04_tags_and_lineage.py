# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5.4: Tags and Lineage
# MAGIC 
# MAGIC Tags enable discovery, policy automation, and compliance reporting.
# MAGIC Lineage is automatic — we just inspect it.

# COMMAND ----------
CATALOG = "dbs_main_catalog"

# COMMAND ----------
# MAGIC %md ## Step 1: Tag PII tables and columns

# COMMAND ----------
# Tag the PII vault as containing PII
spark.sql(f"""
    ALTER TABLE {CATALOG}.northwind_silver.pii_lookup
    SET TAGS ('pii' = 'true', 'sensitivity' = 'high', 'data_class' = 'restricted')
""")

# Tag individual PII columns
for col in ["email", "phone", "date_of_birth"]:
    spark.sql(f"""
        ALTER TABLE {CATALOG}.northwind_silver.pii_lookup
        ALTER COLUMN {col}
        SET TAGS ('pii' = 'true')
    """)

print("✅ Tagged pii_lookup table and PII columns")

# COMMAND ----------
# Tag Gold tables for discovery
for tbl in ["fact_sales", "customer_lifetime_value", "product_performance", 
            "daily_revenue_by_category", "dim_date"]:
    spark.sql(f"""
        ALTER TABLE {CATALOG}.northwind_gold.{tbl}
        SET TAGS ('layer' = 'gold', 'quality_tier' = 'production', 'domain' = 'retail')
    """)
print("✅ Tagged all Gold tables")

# Tag Silver tables
for tbl in ["dim_customers", "fact_orders", "fact_order_items", "products"]:
    spark.sql(f"""
        ALTER TABLE {CATALOG}.northwind_silver.{tbl}
        SET TAGS ('layer' = 'silver', 'quality_tier' = 'production', 'domain' = 'retail')
    """)
print("✅ Tagged Silver tables")

# COMMAND ----------
# MAGIC %md ## Step 2: Query the System Tables for Tagged Tables

# COMMAND ----------
# UC exposes tag metadata via system.information_schema
spark.sql("""
    SELECT catalog_name, schema_name, table_name, tag_name, tag_value
    FROM system.information_schema.table_tags
    WHERE catalog_name = 'dbs_main_catalog'
    ORDER BY schema_name, table_name, tag_name
""").show(50, truncate=False)

# COMMAND ----------
# Find all PII columns across the catalog — perfect for compliance audits
spark.sql("""
    SELECT catalog_name, schema_name, table_name, column_name, tag_value
    FROM system.information_schema.column_tags
    WHERE tag_name = 'pii' AND tag_value = 'true'
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Inspect Auto-Generated Lineage
# MAGIC 
# MAGIC Unity Catalog auto-captures lineage for every query that reads and writes
# MAGIC tables. You can inspect it via system tables:

# COMMAND ----------
# Recent table-to-table lineage events involving our catalog
spark.sql(f"""
    SELECT 
        source_table_name,
        target_table_name,
        event_time,
        entity_type
    FROM system.access.table_lineage
    WHERE event_time > current_timestamp() - INTERVAL 7 DAYS
      AND (target_table_name LIKE '%northwind%' OR source_table_name LIKE '%northwind%')
    ORDER BY event_time DESC
    LIMIT 20
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Visit the Catalog Explorer
# MAGIC 
# MAGIC The most impressive demo of UC is visual. In the left sidebar:
# MAGIC 
# MAGIC 1. Click **Catalog**
# MAGIC 2. Navigate to `dbs_main_catalog → northwind_gold → fact_sales`
# MAGIC 3. Click the **Lineage** tab
# MAGIC 4. You should see an interactive graph showing:
# MAGIC    - Upstream: silver.fact_orders, silver.fact_order_items, silver.dim_customers, silver.products
# MAGIC    - Downstream: any dashboard widgets querying fact_sales
# MAGIC 
# MAGIC This is **end-to-end lineage**, captured automatically, with zero code.
# MAGIC In real interviews this is a top-3 most-asked feature of Unity Catalog.

# COMMAND ----------
# Screenshot reminder
print("📸 Screenshot the Lineage graph for your portfolio README.")