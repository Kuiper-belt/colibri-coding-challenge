"""
Silver Layer Transformation Module

This module processes wind turbine data from the bronze layer, cleaning and validating it 
to create a refined silver layer dataset. The silver layer transformation includes casting 
columns to the correct schema, applying predefined validation rules (SILVER_CONDITIONS), 
and removing duplicate records to ensure data consistency.

The module integrates with the ETL pipeline to extract data from the bronze layer, transform 
it into the silver schema, and load the transformed data into the silver layer table.

Functions:
    silver_layer_transform: Cleans and transforms the input DataFrame for the silver layer.
    execute: Executes the ETL process for the silver layer.
"""

import logging
from pyspark.sql import DataFrame
from utils.spark_etl import etl
from utils.db_utils import get_postgresql_options
from config.config_loader import load_config
from utils.silver_layer_operations import cast_columns, get_conditions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define filtering conditions
SILVER_CONDITIONS = [
    "wind_speed BETWEEN 0 AND 25",
    "wind_direction BETWEEN 0 AND 360",
    "power_output >= 0"
]

def silver_layer_transform(df: DataFrame, schema: dict[str, str]) -> DataFrame:
    """
    Cleans and transforms the input DataFrame for the silver layer.

    The transformation process involves:
      1. Casting columns to the 'silver' schema as defined in the configuration.
      2. Filtering data using predefined conditions (SILVER_CONDITIONS) to ensure validity.
      3. Removing duplicate records to maintain unique entries.

    Args:
        df (DataFrame): The input DataFrame from the bronze layer.
        schema (dict[str, str]): A dictionary defining the silver layer schema.

    Returns:
        DataFrame: A transformed DataFrame meeting the silver layer specifications.

    Raises:
        Exception: If any error occurs during the transformation process.
    """
    try:
        logger.info("Starting silver layer transformation.")

        # Cast columns using the provided schema
        logger.info("Casting columns to schema: %s", schema)
        df = cast_columns(df, schema)

        # Apply filtering conditions
        logger.info("Applying filtering conditions.")
        combined_condition = get_conditions(SILVER_CONDITIONS, df)
        df = df.filter(combined_condition)

        # Remove duplicates
        logger.info("Removing duplicates.")
        df = df.dropDuplicates()

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