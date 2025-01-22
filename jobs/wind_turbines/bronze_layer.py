"""
Bronze Layer ETL Module

This module is responsible for transforming raw wind turbine data into a structured format
suitable for downstream processing. The bronze layer serves as an intermediate step that
ingests raw CSV data, ensures the schema integrity by explicitly casting columns to their
expected data types, and appends metadata for traceability.

The ETL process consists of:
    - Reading raw data from CSV files.
    - Applying transformations to enforce schema and add metadata.
    - Writing the transformed data to the bronze PostgreSQL table.

Functions:
    bronze_layer_transform: Applies transformations to the raw DataFrame.
    execute: Executes the ETL pipeline for the bronze layer.

Modules:
    - pyspark.sql: Provides the Spark DataFrame and transformation functions.
    - utils.spark_etl: Contains the generic ETL framework for reading, transforming, and writing data.
    - utils.db_utils: Utility functions for PostgreSQL configurations.
    - config.config_loader: Loads the environment-specific configuration.
"""

import logging
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, TimestampType, DoubleType, IntegerType
from utils.spark_etl import etl
from utils.db_utils import get_postgresql_options
from config.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def bronze_layer_transform(df: DataFrame) -> DataFrame:
    """
    Transforms raw data for the bronze layer.

    The transformation process includes:
      1. Casting columns to their expected data types for consistency.
      2. Adding a metadata column to track when the record was processed.

    Args:
        df (DataFrame): The raw input DataFrame to be transformed.

    Returns:
        DataFrame: The transformed DataFrame with the expected schema and metadata.

    Raises:
        Exception: If any error occurs during the transformation process.
    """
    try:
        logger.info("Starting bronze layer transformation.")

        # Explicitly cast columns to their expected data types
        df = (
            df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
              .withColumn("turbine_id", col("turbine_id").cast(StringType()))
              .withColumn("wind_speed", col("wind_speed").cast(DoubleType()))
              .withColumn("wind_direction", col("wind_direction").cast(IntegerType()))
              .withColumn("power_output", col("power_output").cast(DoubleType()))
        )
        logger.info("Column casting completed.")

        # Add metadata column
        logger.info("Adding metadata column to track record creation time.")
        df = df.withColumn("metadata_datetime_created", current_timestamp())

        logger.info("Bronze layer transformation completed successfully.")
        return df

    except Exception as e:
        logger.error("An error occurred during bronze layer transformation: %s", e)
        raise


def execute(date_filter_config: dict = None) -> None:
    """
    Executes the ETL pipeline for the bronze layer.

    This function performs the end-to-end ETL process, reading raw CSV data,
    transforming it into the bronze schema, and writing it to the bronze PostgreSQL table.

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
        logger.info("Starting ETL pipeline execution for the bronze layer.")

        # Load environment-specific configuration
        config = load_config("dev")
        etl_cfg = config["etl_config"]

        # Retrieve raw dataset and bronze layer configurations
        raw_dataset_config = etl_cfg["raw_data"]
        bronze_layer_config = etl_cfg["bronze_layer_config"]

        # Extract database and table information
        database = config["pgsql_database"]
        table = bronze_layer_config["table"]

        # Retrieve PostgreSQL options
        postgresql_options = get_postgresql_options(database, table, env="dev")

        # Merge PostgreSQL options with bronze layer configuration
        writer_dict = {**bronze_layer_config, **postgresql_options}

        logger.info(f"Final writer configuration: {writer_dict}")

        # Perform ETL
        logger.info("Starting ETL from raw to bronze with optional date filtering...")
        etl(
            reader_dict=raw_dataset_config,
            writer_dict=writer_dict,
            date_filter_config=date_filter_config,
            transform_func=bronze_layer_transform,
            env="dev"
        )
        logger.info("ETL pipeline (raw -> bronze) completed successfully.")

    except Exception as e:
        logger.error("An error occurred during the ETL pipeline execution: %s", e)
        raise
