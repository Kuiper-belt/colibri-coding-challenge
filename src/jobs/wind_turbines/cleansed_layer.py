"""
Cleansed Layer Transformation Module

This module processes wind turbine data from the ingestion layer, cleaning and validating it
to create a refined cleansed layer dataset. The cleansed layer transformation includes casting
columns to the correct schema, applying predefined and dynamic validation rules, and removing
duplicate records to ensure data consistency.

Functions:
    cleansed_layer_transform: Cleans and transforms the input DataFrame for the cleansed layer.
    execute: Executes the ETL process for the cleansed layer.

Dependencies:
- PySpark
- Custom utility modules: `spark_etl`, `db_utils`, `config_loader`, `cleansed_layer_operations`.
"""

import logging
from pyspark.sql import DataFrame
from src.utils.spark_etl import etl
from src.utils.postgresql_db import get_postgresql_options
from src.config.config_loader import load_config
from src.utils.cleansed_layer_operations import cast_cleansed_schema, get_conditions, log_validation_statistics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define default filtering conditions
DEFAULT_CLEANSED_CONDITIONS = [
    "wind_speed BETWEEN 0 AND 25",
    "wind_direction BETWEEN 0 AND 360",
    "power_output >= 0"
]

def cleansed_layer_transform(df: DataFrame, schema: dict[str, str], dynamic_conditions: list[str] = None) -> DataFrame:
    """
    Cleans and transforms the input DataFrame for the cleansed layer.

    This transformation involves:
      1. Casting columns to the cleansed schema as defined in the configuration.
      2. Filtering data using predefined and dynamic conditions to ensure validity.
      3. Logging statistics for each validation step.
      4. Removing duplicate records to maintain unique entries.

    Args:
        df (DataFrame): The input DataFrame from the ingestion layer.
        schema (dict[str, str]): A dictionary defining the cleansed layer schema.
        dynamic_conditions (list[str], optional): Additional dynamic conditions for validation.

    Returns:
        DataFrame: A transformed DataFrame meeting the cleansed layer specifications.

    Raises:
        Exception: If any error occurs during the transformation process.
    """
    try:
        logger.info("Starting cleansed layer transformation.")

        # Initial row count
        original_count = df.count()

        # Cast columns to the cleansed schema
        logger.info("Casting columns to the cleansed schema.")
        df = cast_cleansed_schema(df, schema)

        # Apply filtering conditions
        logger.info("Applying validation conditions.")
        combined_condition = get_conditions(
            conditions=DEFAULT_CLEANSED_CONDITIONS,
            df=df,
            dynamic_conditions=dynamic_conditions
        )
        df = df.filter(combined_condition)
        log_validation_statistics(df, original_count, "Filter invalid rows based on conditions")

        # Remove duplicate records
        logger.info("Removing duplicate records.")
        original_count = df.count()
        df = df.dropDuplicates()
        log_validation_statistics(df, original_count, "Remove duplicate rows")

        logger.info("Cleansed layer transformation completed successfully.")
        return df

    except Exception as e:
        logger.error("An error occurred during cleansed layer transformation: %s", e)
        raise

def execute(date_filter_config=None):
    """
    Executes the ETL process for the cleansed layer.

    The function orchestrates the extraction of raw data from the ingestion layer,
    transforms it using the `cleansed_layer_transform` function, and loads it into
    the cleansed layer table.

    Args:
        date_filter_config (dict, optional): A dictionary specifying date filtering
        criteria with keys such as:
            - filter_column: The column to filter on.
            - start_date: The start date for filtering.
            - end_date: The end date for filtering.

    Returns:
        None

    Raises:
        Exception: If any error occurs during the ETL execution.
    """
    try:
        logger.info("Starting ETL pipeline execution for the cleansed layer.")

        # Load configuration
        config = load_config("dev")
        etl_cfg = config["etl_config"]

        # Retrieve configurations for ingestion and cleansed layers
        ingestion_layer_config = etl_cfg.get("ingestion_layer_config", {})
        cleansed_layer_config = etl_cfg.get("cleansed_layer_config", {})

        # Retrieve schema from config
        schema = config["schemas"]["cleansed"]

        # Extract database name
        database = config["pgsql_database"]

        # Log PostgreSQL options for debugging
        reader_postgresql_options = get_postgresql_options(database, ingestion_layer_config["table"], env="dev")
        logger.info("PostgreSQL options (reader): %s", reader_postgresql_options)

        writer_postgresql_options = get_postgresql_options(database, cleansed_layer_config["table"], env="dev")
        logger.info("PostgreSQL options (writer): %s", writer_postgresql_options)

        # Merge options into reader and writer configurations
        reader_dict = {**ingestion_layer_config, **reader_postgresql_options}
        logger.info("Final reader configuration: %s", reader_dict)

        writer_dict = {**cleansed_layer_config, **writer_postgresql_options}
        logger.info("Final writer configuration: %s", writer_dict)

        # Perform ETL
        logger.info("Starting ETL from ingestion to cleansed...")
        etl(
            reader_dict=reader_dict,
            writer_dict=writer_dict,
            date_filter_config=date_filter_config,
            transform_func=lambda df: cleansed_layer_transform(df, schema)
        )

        logger.info("ETL pipeline execution for the cleansed layer completed successfully.")

    except Exception as e:
        logger.error("An error occurred during the ETL pipeline execution: %s", e)
        raise