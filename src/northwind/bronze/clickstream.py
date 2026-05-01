"""Bronze ingestion for clickstream source (high-volume JSONL).

This is our one TRUE streaming source — we use processingTime trigger.
For the project we'll still use availableNow to keep costs down,
but the code is ready to flip to streaming.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from .ingestion import build_autoloader_stream, add_ingestion_metadata, write_bronze_stream


def ingest_clickstream(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    schema: str = "northwind_bronze",
    table: str = "clickstream_raw",
    streaming: bool = False,
):
    base_path = f"/Volumes/{catalog}/{schema}/landing/clickstream"
    schema_location = f"/Volumes/{catalog}/{schema}/_internal/_schemas/clickstream"
    checkpoint_location = f"/Volumes/{catalog}/{schema}/_internal/_checkpoints/bronze_clickstream"
    target_table = f"{catalog}.{schema}.{table}"
    
    print(f"📥 Ingesting clickstream → {target_table} (streaming={streaming})")
    
    raw_stream = build_autoloader_stream(
        spark=spark,
        source_path=base_path,
        file_format="json",
        schema_location=schema_location,
    )
    
    enriched = add_ingestion_metadata(raw_stream)
    
    if streaming:
        query = write_bronze_stream(
            df=enriched,
            target_table=target_table,
            checkpoint_location=checkpoint_location,
            trigger_mode="processingTime",
            trigger_value="1 minute",
        )
        # Don't await — return so caller can manage lifecycle
        return query
    else:
        query = write_bronze_stream(
            df=enriched,
            target_table=target_table,
            checkpoint_location=checkpoint_location,
            trigger_mode="availableNow",
        )
        query.awaitTermination()
        print(f"✅ Clickstream ingestion complete")
        return target_table