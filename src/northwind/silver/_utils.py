"""Shared utilities for Silver layer transformations."""
from __future__ import annotations

from pyspark.sql import SparkSession


def ensure_silver_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the Silver schema if it doesn't already exist.
    
    Idempotent — safe to call on every run.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"✅ Schema ready: {catalog}.{schema}")


def table_exists(spark: SparkSession, full_table_name: str) -> bool:
    """Check if a table exists in Unity Catalog."""
    try:
        spark.table(full_table_name)
        return True
    except Exception:
        return False