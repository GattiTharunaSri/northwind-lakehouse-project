"""Silver layer for customers — SCD Type 2 implementation.

Pattern: every customer change in Bronze produces a new version row in Silver.
Each version has effective_from/effective_to bounds and an is_current flag.

Reference query:
    SELECT * FROM silver.customers
    WHERE customer_id = 'CUST_000001'
      AND '2025-01-10' BETWEEN effective_from AND effective_to
"""
from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from ._utils import ensure_silver_schema, table_exists

# Columns we hash to detect "did this customer's data actually change?"
# Excludes PII clear-text and audit fields.
CHANGE_DETECT_COLS = [
    "first_name",
    "last_name",
    "email_hash",
    "phone_hash",
    "date_of_birth",
    "address_street",
    "address_city",
    "address_state",
    "address_zip",
    "address_country",
    "loyalty_tier",
    "preferred_language",
]

# A future-far date used as the "still current" upper bound.
# Using max date (not NULL) makes BETWEEN queries work without OR IS NULL.
FAR_FUTURE = "9999-12-31"


def transform_customers(bronze_df: DataFrame) -> DataFrame:
    """Cleanse and conform customer Bronze rows.
    
    Responsibilities:
    - Flatten the address struct (easier to query/index)
    - Mask PII via SHA-256
    - Cast types
    - Compute the row content hash for change detection
    """
    transformed = (bronze_df
        # Flatten address struct
        .withColumn("address_street",  F.col("address.street"))
        .withColumn("address_city",    F.col("address.city"))
        .withColumn("address_state",   F.col("address.state"))
        .withColumn("address_zip",     F.col("address.zip"))
        .withColumn("address_country", F.col("address.country"))
        
        # PII masking: hash email and phone with SHA-256
        # Keep cleartext only in a separate restricted table (we'll create one)
        .withColumn("email_hash", F.sha2(F.lower(F.col("email")), 256))
        .withColumn("phone_hash", F.sha2(F.regexp_replace(F.col("phone"), r"\D", ""), 256))
        
        # Type coercion
        .withColumn("date_of_birth",  F.to_date("date_of_birth"))
        .withColumn("signup_date",    F.to_date("signup_date"))
        .withColumn("_snapshot_date", F.to_date("_snapshot_date"))
        
        # Quality filter — drop rows missing the natural key
        .filter(F.col("customer_id").isNotNull())
    )
    
    # Compute a content hash for change detection
    hash_input = F.concat_ws("||", *[F.col(c).cast("string") for c in CHANGE_DETECT_COLS])
    transformed = transformed.withColumn("_row_hash", F.sha2(hash_input, 256))
    
    # Final shape — only keep what Silver needs
    return transformed.select(
        "customer_id",
        "first_name",
        "last_name",
        "email_hash",
        "phone_hash",
        "date_of_birth",
        "address_street",
        "address_city",
        "address_state",
        "address_zip",
        "address_country",
        "signup_date",
        "loyalty_tier",
        "preferred_language",
        "_snapshot_date",
        "_row_hash",
    )


def build_silver_customers(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    bronze_schema: str = "northwind_bronze",
    silver_schema: str = "northwind_silver",
    target_table: str = "dim_customers",
) -> str:
    """Build the SCD Type 2 customers dimension.
    
    The two-step MERGE pattern:
    
    Step A: For customers whose data CHANGED in the latest snapshot,
            insert a "phantom NULL" key into the source. This forces
            the MERGE to: (1) close the existing current row,
            and (2) insert the new current row.
    
    Step B: A second pass would handle deletes if needed (we skip).
    
    This gives us:
    - Existing rows that didn't change: untouched
    - Existing rows that changed: effective_to set to yesterday, is_current = false
    - New version rows: inserted with is_current = true, effective_from = today
    - Brand new customers: inserted as the first version
    """
    bronze_table = f"{catalog}.{bronze_schema}.customers_raw"
    silver_table = f"{catalog}.{silver_schema}.{target_table}"
    
    print(f"🔧 Building Silver customers (SCD2): {silver_table}")
    
    ensure_silver_schema(spark, catalog, silver_schema)
    
    # 1. Read Bronze and apply transformations
    bronze_df = spark.table(bronze_table)
    silver_df = transform_customers(bronze_df)
    
    # 2. From all snapshots, take only the LATEST observation per customer.
    # Bronze contains every weekly snapshot; we want the most recent state
    # of each customer to compare against Silver.
    from pyspark.sql.window import Window
    latest_window = Window.partitionBy("customer_id").orderBy(F.desc("_snapshot_date"))
    
    latest_per_customer = (silver_df
        .withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn"))
    
    print(f"   Bronze rows total:    {bronze_df.count():,}")
    print(f"   Distinct customers:   {latest_per_customer.count():,}")
    
    # 3. First-time creation — every Bronze row is a separate SCD2 version
    if not table_exists(spark, silver_table):
        # Build initial SCD2: each historical snapshot becomes a version row.
        # Use LAG to compute effective_from from the prior snapshot, and LEAD
        # to compute effective_to from the next snapshot.
        from pyspark.sql.window import Window
        customer_window = Window.partitionBy("customer_id").orderBy("_snapshot_date")
        
        initial_scd2 = (silver_df
            # First, deduplicate where snapshots produced identical row hashes
            # (no change between two consecutive weeks → no new version needed)
            .withColumn("_prev_hash", F.lag("_row_hash").over(customer_window))
            .filter((F.col("_prev_hash").isNull()) | (F.col("_row_hash") != F.col("_prev_hash")))
            .drop("_prev_hash")
            
            # Compute effective_from / effective_to / is_current
            .withColumn("effective_from", F.col("_snapshot_date"))
            .withColumn("effective_to",
                F.coalesce(
                    F.date_sub(F.lead("_snapshot_date").over(customer_window), 1),
                    F.lit(FAR_FUTURE).cast(DateType())
                )
            )
            .withColumn("is_current", F.col("effective_to") == F.lit(FAR_FUTURE).cast(DateType()))
            
            # Surrogate key: monotonic ID makes each version uniquely identifiable
            .withColumn("customer_version_id", F.expr("uuid()"))
            
            # Audit columns
            .withColumn("_silver_processed_at", F.current_timestamp())
        )
        
        (initial_scd2.write
            .format("delta")
            .mode("overwrite")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(silver_table))
        
        version_count = spark.table(silver_table).count()
        current_count = spark.table(silver_table).filter(F.col("is_current") == True).count()
        print(f"✅ Initial build: {version_count:,} version rows, {current_count:,} current customers")
        spark.sql(f"OPTIMIZE {silver_table}")
        return silver_table
    
    # 4. Incremental SCD2 update — the famous "two-step MERGE"
    
    # Pull the existing CURRENT rows from Silver
    current_silver = (spark.table(silver_table)
        .filter(F.col("is_current") == True)
        .select("customer_id", F.col("_row_hash").alias("current_hash")))
    
    # Find customers whose latest Bronze snapshot has a different hash than their current Silver row
    changed = (latest_per_customer
        .join(current_silver, on="customer_id", how="left")
        .filter(
            (F.col("current_hash").isNull()) |  # brand new customer
            (F.col("_row_hash") != F.col("current_hash"))  # data changed
        )
        .drop("current_hash"))
    
    new_or_changed_count = changed.count()
    print(f"   New or changed customers: {new_or_changed_count:,}")
    
    if new_or_changed_count == 0:
        print("✅ No changes — Silver customers is up to date")
        return silver_table
    
    # The two-step MERGE trick: prepare a staging DF with TWO rows per changed customer
    # Row A: the existing key (closes the current row by setting effective_to/is_current)
    # Row B: the new key (NULL natural key → triggers INSERT of new version)
    
    # Step 1: Build "closing" rows — match existing on customer_id to expire them
    closing_rows = changed.select(
        F.col("customer_id").alias("merge_key"),
        F.col("customer_id"),
        # ... all other cols, but they won't be used in UPDATE
        *[F.col(c) for c in [
            "first_name", "last_name", "email_hash", "phone_hash", "date_of_birth",
            "address_street", "address_city", "address_state", "address_zip", "address_country",
            "signup_date", "loyalty_tier", "preferred_language", "_snapshot_date", "_row_hash"
        ]]
    )
    
    # Step 2: Build "inserting" rows — null merge_key forces NOT MATCHED → INSERT
    inserting_rows = changed.select(
        F.lit(None).cast("string").alias("merge_key"),
        F.col("customer_id"),
        *[F.col(c) for c in [
            "first_name", "last_name", "email_hash", "phone_hash", "date_of_birth",
            "address_street", "address_city", "address_state", "address_zip", "address_country",
            "signup_date", "loyalty_tier", "preferred_language", "_snapshot_date", "_row_hash"
        ]]
    )
    
    staging = closing_rows.unionByName(inserting_rows)
    staging.createOrReplaceTempView("scd2_customers_staging")
    
    # The MERGE
    # When matched (closing_row): expire the row
    # When not matched (inserting_row): insert as new current version
    merge_sql = f"""
    MERGE INTO {silver_table} target
    USING scd2_customers_staging source
        ON  target.customer_id = source.merge_key
        AND target.is_current = true
    WHEN MATCHED THEN UPDATE SET
        target.effective_to = date_sub(source._snapshot_date, 1),
        target.is_current = false,
        target._silver_processed_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
        customer_id, first_name, last_name, email_hash, phone_hash, date_of_birth,
        address_street, address_city, address_state, address_zip, address_country,
        signup_date, loyalty_tier, preferred_language, _snapshot_date, _row_hash,
        effective_from, effective_to, is_current, customer_version_id, _silver_processed_at
    ) VALUES (
        source.customer_id, source.first_name, source.last_name, source.email_hash,
        source.phone_hash, source.date_of_birth, source.address_street, source.address_city,
        source.address_state, source.address_zip, source.address_country, source.signup_date,
        source.loyalty_tier, source.preferred_language, source._snapshot_date, source._row_hash,
        source._snapshot_date, cast('{FAR_FUTURE}' as date), true, uuid(), current_timestamp()
    )
    """
    
    result = spark.sql(merge_sql)
    result.show(truncate=False)
    
    spark.sql(f"OPTIMIZE {silver_table}")
    print(f"✅ SCD2 merge complete")
    return silver_table