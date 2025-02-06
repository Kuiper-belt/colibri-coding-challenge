"""
Ingestion Layer ETL Module

This module is responsible for transforming raw wind turbine data into a structured format
suitable for downstream processing. The ingestion layer serves as an intermediate step that
ingests raw CSV data, ensures schema integrity by explicitly casting columns to their
expected data types, and appends metadata for traceability.

The ETL process consists of:
    - Reading raw data from CSV files.
    - Applying transformations to enforce schema and add metadata.
    - Writing the transformed data to the ingestion PostgreSQL table.

Functions:
    ingestion_layer_transform: Applies transformations to the raw DataFrame.
    execute: Executes the ETL pipeline for the ingestion layer.

Dependencies:
- PySpark
- Custom utility modules: `spark_etl`, `db_utils`, `config_loader`, `ingestion_layer_operations`.
"""

import logging
from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame
from src.utils.spark_etl import etl
from src.utils.postgresql_db import get_postgresql_options
from src.config.config_loader import load_config
from src.utils.ingestion_layer_operations import cast_ingestion_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingestion_layer_transform(df: DataFrame, schema: dict[str, str]) -> DataFrame:
    """
    Transforms raw data for the ingestion layer.

    The transformation process includes:
      1. Casting columns to their expected data types for consistency.
      2. Adding a metadata column to track when the record was processed.

    Args:
        df (DataFrame): The raw input DataFrame to be transformed.
        schema (dict[str, str]): The schema to apply to the DataFrame.

    Returns:
        DataFrame: The transformed DataFrame with the expected schema and metadata.

    Raises:
        Exception: If any error occurs during the transformation process.
    """
    try:
        logger.info("Starting ingestion layer transformation.")

        # Enforce schema using the utility function
        logger.info("Casting columns to the ingestion schema.")
        df = cast_ingestion_schema(df, schema, skip_columns=["metadata_datetime_created"])

        # Add metadata column
        logger.info("Adding metadata column to track record creation time.")
        df = df.withColumn("metadata_datetime_created", current_timestamp().cast("TIMESTAMP"))

        logger.info("ingestion layer transformation completed successfully.")
        return df

    except Exception as e:
        logger.error("An error occurred during ingestion layer transformation: %s", e)
        raise

def execute(date_filter_config: dict = None) -> None:
    """
    Executes the ETL pipeline for the ingestion layer.

    This function performs the end-to-end ETL process, reading raw CSV data,
    transforming it into the ingestion schema, and writing it to the ingestion PostgreSQL table.

    Args:
        date_filter_config (dict, optional): A dictionary for date filtering parameters with keys:
            - filter_column: The column name to filter on.
            - start_date: The starting date for the filter.
            - end_date: The ending date for the filter.

    Returns:
        None

    Raises:
        Exception: If any error occurs during the ETL process.
    """
    try:
        logger.info("Starting ETL pipeline execution for the ingestion layer.")

        # Load environment-specific configuration
        config = load_config("dev")
        etl_cfg = config["etl_config"]

        # Retrieve raw dataset and ingestion layer configurations
        raw_dataset_config = etl_cfg["raw_data"]
        ingestion_layer_config = etl_cfg["ingestion_layer_config"]

        # Retrieve schema from config
        schema = config["schemas"]["ingestion"]

        # Extract database and table information
        database = config["pgsql_database"]
        table = ingestion_layer_config["table"]

        # Retrieve PostgreSQL options
        postgresql_options = get_postgresql_options(database, table, env="dev")

        # Merge PostgreSQL options with ingestion layer configuration
        writer_dict = {**ingestion_layer_config, **postgresql_options}

        logger.info(f"Final writer configuration: {writer_dict}")

        # Perform ETL
        logger.info("Starting ETL from raw to ingestion with optional date filtering...")
        etl(
            reader_dict=raw_dataset_config,
            writer_dict=writer_dict,
            date_filter_config=date_filter_config,
            transform_func=lambda df: ingestion_layer_transform(df, schema),
            env="dev"
        )
        logger.info("ETL pipeline execution for the ingestion layer completed successfully.")

    except Exception as e:
        logger.error("An error occurred during the ETL pipeline execution: %s", e)
        raise