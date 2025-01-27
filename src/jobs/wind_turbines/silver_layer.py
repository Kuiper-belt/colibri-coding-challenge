"""
Silver Layer Transformation Module

This module processes wind turbine data from the bronze layer, cleaning and validating it
to create a refined silver layer dataset. The silver layer transformation includes casting
columns to the correct schema, applying predefined and dynamic validation rules, and removing
duplicate records to ensure data consistency.

Functions:
    silver_layer_transform: Cleans and transforms the input DataFrame for the silver layer.
    execute: Executes the ETL process for the silver layer.

Dependencies:
- PySpark
- Custom utility modules: `spark_etl`, `db_utils`, `config_loader`, `silver_layer_operations`.
"""

import logging
from pyspark.sql import DataFrame
from src.utils.spark_etl import etl
from src.utils.db_utils import get_postgresql_options
from src.config.config_loader import load_config
from src.utils.silver_layer_operations import cast_silver_schema, get_conditions, log_validation_statistics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define default filtering conditions
DEFAULT_SILVER_CONDITIONS = [
    "wind_speed BETWEEN 0 AND 25",
    "wind_direction BETWEEN 0 AND 360",
    "power_output >= 0"
]

def silver_layer_transform(df: DataFrame, schema: dict[str, str], dynamic_conditions: list[str] = None) -> DataFrame:
    """
    Cleans and transforms the input DataFrame for the silver layer.

    This transformation involves:
      1. Casting columns to the silver schema as defined in the configuration.
      2. Filtering data using predefined and dynamic conditions to ensure validity.
      3. Logging statistics for each validation step.
      4. Removing duplicate records to maintain unique entries.

    Args:
        df (DataFrame): The input DataFrame from the bronze layer.
        schema (dict[str, str]): A dictionary defining the silver layer schema.
        dynamic_conditions (list[str], optional): Additional dynamic conditions for validation.

    Returns:
        DataFrame: A transformed DataFrame meeting the silver layer specifications.

    Raises:
        Exception: If any error occurs during the transformation process.
    """
    try:
        logger.info("Starting silver layer transformation.")

        # Initial row count
        original_count = df.count()

        # Cast columns to the silver schema
        logger.info("Casting columns to the silver schema.")
        df = cast_silver_schema(df, schema)

        # Apply filtering conditions
        logger.info("Applying validation conditions.")
        combined_condition = get_conditions(
            conditions=DEFAULT_SILVER_CONDITIONS,
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

        logger.info("Silver layer transformation completed successfully.")
        return df

    except Exception as e:
        logger.error("An error occurred during silver layer transformation: %s", e)
        raise

def execute(date_filter_config=None):
    """
    Executes the ETL process for the silver layer.

    The function orchestrates the extraction of raw data from the bronze layer,
    transforms it using the `silver_layer_transform` function, and loads it into
the silver layer table.

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
        logger.info("Starting ETL pipeline execution for the silver layer.")

        # Load configuration
        config = load_config("dev")
        etl_cfg = config["etl_config"]

        # Retrieve configurations for bronze and silver layers
        bronze_layer_config = etl_cfg.get("bronze_layer_config", {})
        silver_layer_config = etl_cfg.get("silver_layer_config", {})

        # Retrieve schema from config
        schema = config["schemas"]["silver"]

        # Extract database name
        database = config["pgsql_database"]

        # Log PostgreSQL options for debugging
        reader_postgresql_options = get_postgresql_options(database, bronze_layer_config["table"], env="dev")
        logger.info("PostgreSQL options (reader): %s", reader_postgresql_options)

        writer_postgresql_options = get_postgresql_options(database, silver_layer_config["table"], env="dev")
        logger.info("PostgreSQL options (writer): %s", writer_postgresql_options)

        # Merge options into reader and writer configurations
        reader_dict = {**bronze_layer_config, **reader_postgresql_options}
        logger.info("Final reader configuration: %s", reader_dict)

        writer_dict = {**silver_layer_config, **writer_postgresql_options}
        logger.info("Final writer configuration: %s", writer_dict)

        # Perform ETL
        etl(
            reader_dict=reader_dict,
            writer_dict=writer_dict,
            date_filter_config=date_filter_config,
            transform_func=lambda df: silver_layer_transform(df, schema)
        )

        logger.info("ETL pipeline execution for the silver layer completed successfully.")

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