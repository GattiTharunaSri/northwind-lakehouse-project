"""Generic Auto Loader ingestion utilities.

The core idea: every Bronze table follows the same pattern, so we encapsulate it.
Sources differ only in path, format, and a few options.
"""
from __future__ import annotations

from typing import Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def build_autoloader_stream(
    spark: SparkSession,
    source_path: str,
    file_format: str,
    schema_location: str,
    schema_evolution_mode: str = "addNewColumns",
    extra_options: dict[str, Any] | None = None,
) -> DataFrame:
    """Build an Auto Loader streaming DataFrame.
    
    Args:
        source_path: Where files land, e.g. /Volumes/.../landing/orders
        file_format: 'json', 'csv', 'parquet'
        schema_location: Where Auto Loader stores inferred schema versions.
            Must be a stable path — losing it means re-inference.
        schema_evolution_mode: How to handle new columns.
            - 'addNewColumns' (recommended): adds columns, restarts stream
            - 'rescue': new columns go into _rescued_data
            - 'failOnNewColumns': fails the stream
            - 'none': ignores new columns
        extra_options: Format-specific options (e.g. CSV header, multiline JSON)
    """
    reader = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)
        # Capture malformed records instead of failing
        .option("rescuedDataColumn", "_rescued_data")
        # Walk subdirectories — needed for our customers/week_*/ layout
        .option("recursiveFileLookup", "true")
    )
    
    if extra_options:
        for key, value in extra_options.items():
            reader = reader.option(key, value)
    
    return reader.load(source_path)


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Attach lineage metadata required for every Bronze table.
    
    These columns are non-negotiable in production — they answer:
    - Which file did this row come from?  (debugging)
    - When did we ingest it?              (SLA tracking)
    - What's the file modification time?  (late-arriving data detection)
    """
    return (df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_modified", F.col("_metadata.file_modification_time"))
        .withColumn("_source_file_size", F.col("_metadata.file_size"))
    )


def write_bronze_stream(
    df: DataFrame,
    target_table: str,
    checkpoint_location: str,
    trigger_mode: str = "availableNow",
    trigger_value: str | None = None,
):
    """Write a streaming DataFrame to a Bronze Delta table.
    
    Args:
        df: The streaming DataFrame to write
        target_table: Three-part name like 'catalog.schema.table'
        checkpoint_location: Where stream state is saved.
            CRITICAL: One unique checkpoint per (source, target) pair.
            Sharing checkpoints = corrupted state.
        trigger_mode: 'availableNow' for batch, 'processingTime' for streaming
        trigger_value: For processingTime, e.g. '1 minute'
    """
    writer = (df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        # mergeSchema lets the table grow when new columns appear in source
        .option("mergeSchema", "true")
        .outputMode("append")
        .toTable(target_table)
    )
    
    if trigger_mode == "availableNow":
        return df.writeStream.format("delta") \
            .option("checkpointLocation", checkpoint_location) \
            .option("mergeSchema", "true") \
            .outputMode("append") \
            .trigger(availableNow=True) \
            .toTable(target_table)
    elif trigger_mode == "processingTime":
        return df.writeStream.format("delta") \
            .option("checkpointLocation", checkpoint_location) \
            .option("mergeSchema", "true") \
            .outputMode("append") \
            .trigger(processingTime=trigger_value or "1 minute") \
            .toTable(target_table)
    else:
        raise ValueError(f"Unknown trigger_mode: {trigger_mode}")