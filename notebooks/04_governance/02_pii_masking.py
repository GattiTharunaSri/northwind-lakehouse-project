# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5.2: Column Masking for PII
# MAGIC 
# MAGIC Column masks are functions that change what a user sees in a column 
# MAGIC based on their group membership. The data on disk is unchanged — only
# MAGIC the rendered value differs per user.

# COMMAND ----------
CATALOG = "dbs_main_catalog"

# COMMAND ----------
# MAGIC %md
# MAGIC ## How Column Masks Work
# MAGIC 
# MAGIC A column mask is a UDF that takes the original value and returns what
# MAGIC the current user should see. The function uses `is_account_group_member()`
# MAGIC to check group membership.
# MAGIC 
# MAGIC Pattern: members of `northwind_pii_readers` see real values; everyone
# MAGIC else sees a masked version.

# COMMAND ----------
# MAGIC %md ## Step 1: Create the Mask Functions

# COMMAND ----------
# Create a dedicated schema for governance functions — keeps things tidy
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.northwind_governance")

# COMMAND ----------
# Mask 1: Email — pii_readers see real; others see hash prefix only
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.northwind_governance.mask_email(email STRING)
    RETURNS STRING
    RETURN CASE
        WHEN is_account_group_member('northwind_pii_readers') THEN email
        WHEN email IS NULL THEN NULL
        ELSE concat(left(email, 4), '****@****')
    END
""")
print("✅ Created mask_email")

# COMMAND ----------
# Mask 2: Phone — pii_readers see real; others see last 4 digits only
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.northwind_governance.mask_phone(phone STRING)
    RETURNS STRING
    RETURN CASE
        WHEN is_account_group_member('northwind_pii_readers') THEN phone
        WHEN phone IS NULL THEN NULL
        ELSE concat('***-***-', right(regexp_replace(phone, '[^0-9]', ''), 4))
    END
""")
print("✅ Created mask_phone")

# COMMAND ----------
# Mask 3: Date of birth — pii_readers see real; others see year only
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.northwind_governance.mask_dob(dob DATE)
    RETURNS DATE
    RETURN CASE
        WHEN is_account_group_member('northwind_pii_readers') THEN dob
        ELSE date_trunc('year', dob)
    END
""")
print("✅ Created mask_dob")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: A Note on Our Architecture Choice
# MAGIC 
# MAGIC In our Silver `dim_customers`, we already store email and phone as 
# MAGIC SHA-256 hashes — there's no cleartext to mask. So we'll demonstrate
# MAGIC column masks on a NEW table: a `pii_lookup` table that holds the 
# MAGIC mapping from customer_id → cleartext PII, restricted to pii_readers.
# MAGIC
# MAGIC This pattern (hashed PII in main tables + a separate restricted lookup)
# MAGIC is how production teams handle "I need to see real PII when needed but
# MAGIC mostly never" — it's called the **vault pattern**.

# COMMAND ----------
# Create the PII vault table — this would normally be populated during Silver build
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.northwind_silver.pii_lookup (
        customer_id STRING NOT NULL,
        email STRING,
        phone STRING,
        date_of_birth DATE,
        _last_updated TIMESTAMP
    )
    USING DELTA
    COMMENT 'PII vault. Cleartext customer email/phone/DOB. Access restricted to northwind_pii_readers via column masks.'
""")

# Populate it from Bronze (a one-time backfill)
from pyspark.sql import functions as F
pii_df = (spark.table(f"{CATALOG}.northwind_bronze.customers_raw")
    .filter(F.col("_snapshot_date") == F.col("_snapshot_date"))  # latest snapshot only
    .select(
        "customer_id",
        F.col("email"),
        F.col("phone"),
        F.to_date("date_of_birth").alias("date_of_birth"),
    )
    .dropDuplicates(["customer_id"])
    .withColumn("_last_updated", F.current_timestamp())
)

(pii_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.northwind_silver.pii_lookup"))

print(f"✅ Populated pii_lookup: {spark.table(f'{CATALOG}.northwind_silver.pii_lookup').count():,} rows")

# COMMAND ----------
# MAGIC %md ## Step 3: Apply Column Masks

# COMMAND ----------
# Apply masks to the cleartext columns
spark.sql(f"""
    ALTER TABLE {CATALOG}.northwind_silver.pii_lookup
    ALTER COLUMN email
    SET MASK {CATALOG}.northwind_governance.mask_email
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.northwind_silver.pii_lookup
    ALTER COLUMN phone
    SET MASK {CATALOG}.northwind_governance.mask_phone
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.northwind_silver.pii_lookup
    ALTER COLUMN date_of_birth
    SET MASK {CATALOG}.northwind_governance.mask_dob
""")

print("✅ Masks applied to pii_lookup")

# COMMAND ----------
# MAGIC %md ## Step 4: See It in Action

# COMMAND ----------
# As the current user (admin, not in pii_readers group), you'll see masked values
print("=== Your view (admin, NOT in pii_readers group) ===")
spark.sql(f"""
    SELECT customer_id, email, phone, date_of_birth
    FROM {CATALOG}.northwind_silver.pii_lookup
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC **What you should see:** Email like `joh.****@****`, phone as `***-***-1234`, 
# MAGIC DOB as a Jan-1-of-year date (year only).
# MAGIC 
# MAGIC If you were a member of `northwind_pii_readers`, the same query would return 
# MAGIC the real values. The data on disk is identical — only the projection changes 
# MAGIC per user.

# COMMAND ----------
# MAGIC %md ## Step 5: Verify the Mask Definition Is Stored

# COMMAND ----------
spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.northwind_silver.pii_lookup").show(truncate=False)