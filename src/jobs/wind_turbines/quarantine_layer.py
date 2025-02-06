"""
This module defines the ETL (Extract, Transform, Load) process for the "quarantine layer" of the wind turbine data pipeline.
The quarantine layer is responsible for identifying and isolating records that do not meet the specified conditions 
(DEFAULT_CLEANSED_CONDITIONS). This ensures that only valid data progresses to the next stages of the pipeline.

The module includes two primary functions:
1. `quarantine_layer_transform`: Handles data transformation by:
   - Casting columns to the desired schema.
   - Applying conditions to isolate invalid records.
   - Removing duplicates to ensure data integrity.
2. `execute`: Manages the ETL pipeline for the quarantine layer by:
   - Loading configurations for database connections and schemas.
   - Invoking the transformation function.
   - Saving the processed data to the quarantine layer table.

Key Components:
- Integration with Spark for distributed data processing.
- Configurable via external JSON files to support multiple environments (e.g., dev, test, prod).
- Logging at each step for transparency and debugging.

Dependencies:
- PySpark
- Custom utility modules: `spark_etl`, `db_utils`, `config_loader`, `cleansed_layer_operations`.
"""

import logging
from pyspark.sql import DataFrame
from src.utils.spark_etl import etl
from src.utils.postgresql_db import get_postgresql_options
from src.config.config_loader import load_config
from src.utils.cleansed_layer_operations import cast_cleansed_schema, get_conditions
from .cleansed_layer import DEFAULT_CLEANSED_CONDITIONS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def quarantine_layer_transform(df: DataFrame, schema: dict[str, str]) -> DataFrame:
    """
    Transforms the input DataFrame for the quarantine layer by isolating invalid records.

    This function applies the following steps:
    1. Casts columns to the expected schema using the `cast_cleansed_schema` utility.
    2. Filters records that do not satisfy the `DEFAULT_CLEANSED_CONDITIONS`.
    3. Removes duplicate records for data integrity.

    Args:
        df (DataFrame): The input PySpark DataFrame to be processed.
        schema (dict[str, str]): A dictionary mapping column names to their expected data types.

    Returns:
        DataFrame: A transformed PySpark DataFrame containing only invalid records.

    Raises:
        Exception: If any error occurs during the transformation process.
    """
    try:
        logger.info("Starting quarantine layer transformation.")

        # Cast columns using the provided schema
        logger.info("Casting columns to schema: %s", schema)
        df = cast_cleansed_schema(df, schema)

        # Combine filtering conditions for records that do not meet cleansed conditions
        logger.info("Applying filtering conditions to quarantine invalid records.")
        combined_condition = get_conditions(DEFAULT_CLEANSED_CONDITIONS, df, no_nulls=True)
        df = df.filter(~combined_condition)  # Negate the conditions to isolate invalid records

        # Drop duplicates
        logger.info("Removing duplicates.")
        df = df.dropDuplicates()

        logger.info("Quarantine layer transformation completed successfully.")
        return df

    except Exception as e:
        logger.error("An error occurred during quarantine layer transformation: %s", e)
        raise

def execute(date_filter_config=None):
    """
    Executes the ETL pipeline for the quarantine layer.

    This function orchestrates the ETL process, including:
    - Loading configuration settings from external JSON files.
    - Extracting data from the ingestion layer.
    - Transforming the data using `quarantine_layer_transform`.
    - Loading the processed data into the quarantine layer table.

    Args:
        date_filter_config (dict, optional): Configuration for date-based filtering, containing:
            - `filter_column` (str): Column name used for filtering (e.g., "timestamp").
            - `start_date` (str): Start date for filtering (inclusive).
            - `end_date` (str): End date for filtering (inclusive).

    Returns:
        None

    Raises:
        Exception: If any error occurs during the ETL pipeline execution.
    """
    try:
        logger.info("Starting ETL pipeline execution for the quarantine layer.")

        # Load configuration
        config = load_config("dev")
        etl_cfg = config["etl_config"]

        # Retrieve configurations for ingestion and quarantine layers
        ingestion_layer_config = etl_cfg.get("ingestion_layer_config", {})
        quarantine_layer_config = etl_cfg.get("quarantine_layer_config", {})

        # Retrieve schema from config
        schema = config["schemas"]["cleansed"]

        # Extract database name
        database = config["pgsql_database"]

        # Log PostgreSQL options for debugging
        reader_postgresql_options = get_postgresql_options(database, ingestion_layer_config["table"], env="dev")
        logger.info("PostgreSQL options (reader): %s", reader_postgresql_options)

        writer_postgresql_options = get_postgresql_options(database, quarantine_layer_config["table"], env="dev")
        logger.info("PostgreSQL options (writer): %s", writer_postgresql_options)

        # Merge options into reader and writer configurations
        reader_dict = {**ingestion_layer_config, **reader_postgresql_options}
        logger.info("Final reader configuration: %s", reader_dict)

        writer_dict = {**quarantine_layer_config, **writer_postgresql_options}
        logger.info("Final writer configuration: %s", writer_dict)

        # Perform ETL
        logger.info("Starting ETL from ingestion to quarantine...")
        etl(
            reader_dict=reader_dict,
            writer_dict=writer_dict,
            date_filter_config=date_filter_config,
            transform_func=lambda df: quarantine_layer_transform(df, schema)
        )

        logger.info("ETL pipeline execution for the quarantine layer completed successfully.")

    except Exception as e:
        logger.error("An error occurred during the ETL pipeline execution: %s", e)
        raise