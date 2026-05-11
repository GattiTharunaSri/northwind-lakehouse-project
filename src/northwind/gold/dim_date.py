"""Gold layer: dim_date — a calendar dimension.

This is a one-time build. Generates ~10 years of date rows with
business-relevant attributes (fiscal quarter, day-of-week, etc.).
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date


def build_dim_date(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    gold_schema: str = "northwind_gold",
    start_date: str = "2020-01-01",
    end_date: str = "2030-12-31",
) -> str:
    """Build the calendar dimension.
    
    Idempotent: always produces the same rows for the same date range.
    """
    target_table = f"{catalog}.{gold_schema}.dim_date"
    
    print(f"📅 Building {target_table} from {start_date} to {end_date}")
    
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")
    
    # Generate one row per date using sequence + explode
    # Spark's sequence function generates an array of all dates in range
    dim_date = (
        spark.sql(f"""
            SELECT explode(sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )) AS date_key
        """)
        .withColumn("year",            F.year("date_key"))
        .withColumn("quarter",         F.quarter("date_key"))
        .withColumn("month",           F.month("date_key"))
        .withColumn("month_name",      F.date_format("date_key", "MMMM"))
        .withColumn("day_of_month",    F.dayofmonth("date_key"))
        .withColumn("day_of_week",     F.dayofweek("date_key"))  # 1=Sunday, 7=Saturday
        .withColumn("day_name",        F.date_format("date_key", "EEEE"))
        .withColumn("week_of_year",    F.weekofyear("date_key"))
        .withColumn(
            "is_weekend",
            F.col("day_of_week").isin(1, 7)
        )
        .withColumn(
            "fiscal_quarter",
            # Assume fiscal year starts in Feb (common in retail)
            F.concat(
                F.lit("FY"),
                F.year("date_key"),
                F.lit("-Q"),
                F.when(F.col("month").isin(2, 3, 4),    F.lit("1"))
                 .when(F.col("month").isin(5, 6, 7),    F.lit("2"))
                 .when(F.col("month").isin(8, 9, 10),   F.lit("3"))
                 .otherwise(F.lit("4"))
            )
        )
        .withColumn("is_month_start",  F.dayofmonth("date_key") == 1)
        .withColumn("is_month_end",    F.col("date_key") == F.last_day("date_key"))
        .withColumn(
            "is_holiday_us",
            # Simple US holiday flags (Christmas, Thanksgiving-ish, July 4, New Year)
            (F.col("month") == 12) & (F.col("day_of_month") == 25) |
            (F.col("month") == 7)  & (F.col("day_of_month") == 4)  |
            (F.col("month") == 1)  & (F.col("day_of_month") == 1)
        )
        .withColumn("date_iso",        F.date_format("date_key", "yyyy-MM-dd"))
    )
    
    (dim_date.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table))
    
    count = spark.table(target_table).count()
    print(f"✅ {target_table}: {count:,} dates")
    
    return target_table