# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5.3: Row-Level Filtering
# MAGIC 
# MAGIC Row filters control which rows a user sees, in addition to which columns.

# COMMAND ----------
CATALOG = "dbs_main_catalog"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Use Case: Regional Analysts See Only Their Region
# MAGIC 
# MAGIC A US analyst should see US orders. An EU analyst should see EU orders.
# MAGIC A global analyst sees everything. This is fundamental for GDPR data 
# MAGIC localization and for limiting blast radius of overly broad queries.

# COMMAND ----------
# MAGIC %md ## Step 1: Define the Row Filter Function

# COMMAND ----------
# Row filter: takes the value of the filter column, returns BOOLEAN
# (true = include the row, false = hide it)
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.northwind_governance.region_filter(country STRING)
    RETURNS BOOLEAN
    RETURN
        is_account_group_member('northwind_global_analysts')
        OR (is_account_group_member('northwind_us_analysts') AND country = 'US')
        OR (is_account_group_member('northwind_eu_analysts') AND country IN ('DE', 'FR', 'IT', 'ES', 'NL', 'BE'))
        OR is_account_group_member('admins')
""")
print("✅ Created region_filter function")

# COMMAND ----------
# MAGIC %md ## Step 2: Apply the Row Filter to fact_sales

# COMMAND ----------
# Apply filter on the country column
spark.sql(f"""
    ALTER TABLE {CATALOG}.northwind_gold.fact_sales
    SET ROW FILTER {CATALOG}.northwind_governance.region_filter
    ON (customer_country_at_order_time)
""")
print("✅ Row filter applied to fact_sales on customer_country_at_order_time")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: See It in Action
# MAGIC 
# MAGIC As an admin (matching `admins` in the filter), you'll see all rows.
# MAGIC Non-admins outside any analyst group would see ZERO rows.

# COMMAND ----------
# Total visible rows (depends on your group membership)
total = spark.table(f"{CATALOG}.northwind_gold.fact_sales").count()
print(f"Rows visible to you: {total:,}")

# Breakdown by country
spark.sql(f"""
    SELECT customer_country_at_order_time, COUNT(*) as rows
    FROM {CATALOG}.northwind_gold.fact_sales
    GROUP BY customer_country_at_order_time
    ORDER BY rows DESC
    LIMIT 10
""").show()

# COMMAND ----------
# MAGIC %md ## Step 4: Remove the Filter (Optional Cleanup)
# MAGIC 
# MAGIC If the filter is too restrictive while you're still developing, you can drop it:

# COMMAND ----------
# Uncomment to drop the row filter if it gets in your way during development
# spark.sql(f"""
#     ALTER TABLE {CATALOG}.northwind_gold.fact_sales
#     DROP ROW FILTER
# """)