"""Bronze ingestion for orders source (JSONL files)."""
from __future__ import annotations

from pyspark.sql import SparkSession
from .ingestion import build_autoloader_stream, add_ingestion_metadata, write_bronze_stream


def ingest_orders(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    schema: str = "northwind_bronze",
    table: str = "orders_raw",
):
    """Run incremental Bronze ingestion for orders.
    
    Source: JSONL files in /Volumes/.../landing/orders/
    Target: Delta table {catalog}.{schema}.{table}
    Trigger: availableNow (batch — runs once, processes new files, exits)
    """
    base_path = f"/Volumes/{catalog}/{schema}/landing/orders"
    schema_location = f"/Volumes/{catalog}/{schema}/_internal/_schemas/orders"
    checkpoint_location = f"/Volumes/{catalog}/{schema}/_internal/_checkpoints/bronze_orders"
    target_table = f"{catalog}.{schema}.{table}"
    
    print(f"📥 Ingesting orders → {target_table}")
    print(f"   Source:     {base_path}")
    print(f"   Schema loc: {schema_location}")
    print(f"   Checkpoint: {checkpoint_location}")
    
    # Step 1: Auto Loader stream
    raw_stream = build_autoloader_stream(
        spark=spark,
        source_path=base_path,
        file_format="json",
        schema_location=schema_location,
        schema_evolution_mode="addNewColumns",
    )
    
    # Step 2: Add lineage metadata
    enriched = add_ingestion_metadata(raw_stream)
    
    # Step 3: Write to Bronze with availableNow trigger
    query = write_bronze_stream(
        df=enriched,
        target_table=target_table,
        checkpoint_location=checkpoint_location,
        trigger_mode="availableNow",
    )
    
    # Block until the once-only stream completes
    query.awaitTermination()
    print(f"✅ Orders ingestion complete")
    
    return target_table