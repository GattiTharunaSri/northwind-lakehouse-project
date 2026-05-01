"""Bronze ingestion for products source (CSV daily snapshots)."""
from __future__ import annotations

from pyspark.sql import SparkSession
from .ingestion import build_autoloader_stream, add_ingestion_metadata, write_bronze_stream


def ingest_products(
    spark: SparkSession,
    catalog: str = "dbs_main_catalog",
    schema: str = "northwind_bronze",
    table: str = "products_raw",
):
    """Run incremental Bronze ingestion for products.
    
    Source: CSV files in /Volumes/.../landing/products/
    Note: We rely on inference for now. In production you'd often pin a schema.
    """
    base_path = f"/Volumes/{catalog}/{schema}/landing/products"
    schema_location = f"/Volumes/{catalog}/{schema}/_internal/_schemas/products"
    checkpoint_location = f"/Volumes/{catalog}/{schema}/_internal/_checkpoints/bronze_products"
    target_table = f"{catalog}.{schema}.{table}"
    
    print(f"📥 Ingesting products → {target_table}")
    
    raw_stream = build_autoloader_stream(
        spark=spark,
        source_path=base_path,
        file_format="csv",
        schema_location=schema_location,
        extra_options={
            "header": "true",
            "inferSchema": "true",
            # CSVs benefit from sampling more rows for accurate type inference
            "cloudFiles.schemaHints": "price double, cost double, weight_kg double, is_active boolean",
        },
    )
    
    enriched = add_ingestion_metadata(raw_stream)
    
    query = write_bronze_stream(
        df=enriched,
        target_table=target_table,
        checkpoint_location=checkpoint_location,
        trigger_mode="availableNow",
    )
    query.awaitTermination()
    print(f"✅ Products ingestion complete")
    return target_table