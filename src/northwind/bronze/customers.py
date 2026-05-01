"""Bronze ingestion for customers source (Parquet weekly snapshots)."""
from __future__ import annotations

from pyspark.sql import SparkSession
from .ingestion import build_autoloader_stream, add_ingestion_metadata, write_bronze_stream


def ingest_customers(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    schema: str = "northwind_bronze",
    table: str = "customers_raw",
):
    """Run incremental Bronze ingestion for customers.
    
    Source: Parquet files in /Volumes/.../landing/customers/week_NNN/
    Recursive lookup is essential here.
    """
    base_path = f"/Volumes/{catalog}/{schema}/landing/customers"
    schema_location = f"/Volumes/{catalog}/{schema}/_internal/_schemas/customers"
    checkpoint_location = f"/Volumes/{catalog}/{schema}/_internal/_checkpoints/bronze_customers"
    target_table = f"{catalog}.{schema}.{table}"
    
    print(f"📥 Ingesting customers → {target_table}")
    
    raw_stream = build_autoloader_stream(
        spark=spark,
        source_path=base_path,
        file_format="parquet",
        schema_location=schema_location,
    )
    
    enriched = add_ingestion_metadata(raw_stream)
    
    query = write_bronze_stream(
        df=enriched,
        target_table=target_table,
        checkpoint_location=checkpoint_location,
        trigger_mode="availableNow",
    )
    query.awaitTermination()
    print(f"✅ Customers ingestion complete")
    return target_table