"""
Bronze Layer ETL Module

This module is responsible for transforming raw wind turbine data into a structured format
suitable for downstream processing. The bronze layer serves as an intermediate step that
ingests raw CSV data, ensures schema integrity by explicitly casting columns to their
expected data types, and appends metadata for traceability.

The ETL process consists of:
    - Reading raw data from CSV files.
    - Applying transformations to enforce schema and add metadata.
    - Writing the transformed data to the bronze PostgreSQL table.

Functions:
    bronze_layer_transform: Applies transformations to the raw DataFrame.
    execute: Executes the ETL pipeline for the bronze layer.

Dependencies:
- PySpark
- Custom utility modules: `spark_etl`, `db_utils`, `config_loader`, `bronze_layer_operations`.
"""

import logging
from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame
from utils.spark_etl import etl
from utils.db_utils import get_postgresql_options
from config.config_loader import load_config
from utils.bronze_layer_operations import cast_bronze_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def bronze_layer_transform(df: DataFrame, schema: dict[str, str]) -> DataFrame:
    """
    Transforms raw data for the bronze layer.

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
        logger.info("Starting bronze layer transformation.")

        # Enforce schema using the utility function
        logger.info("Casting columns to the bronze schema.")
        df = cast_bronze_schema(df, schema, skip_columns=["metadata_datetime_created"])

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

        # Retrieve schema from config
        schema = config["schemas"]["bronze"]

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
            transform_func=lambda df: bronze_layer_transform(df, schema),
            env="dev"
        )
        logger.info("ETL pipeline (raw -> bronze) completed successfully.")

    except Exception as e:
        logger.error("An error occurred during the ETL pipeline execution: %s", e)
        raise

if __name__ == '__main__':
    # Example date filter configuration
    test_date_filter_config = {
        'filter_column': 'timestamp',
        'start_date': '2022-03-01'
    }
    execute(date_filter_config=test_date_filter_config)