"""
Curated Layer Transformation Module

This module aggregates and analyzes wind turbine data to produce a refined dataset
for the curated layer. The curated layer contains summary statistics and flags anomalous
data for further analysis. The transformation steps include calculating mean, minimum,
maximum, and identifying anomalies based on standard deviation thresholds.

The module is designed to integrate into an ETL pipeline that extracts data from
the cleansed layer, processes it into the curated schema, and loads it into the curated layer table.

Functions:
    curated_layer_transform: Aggregates and transforms the cleansed layer DataFrame for the curated layer.
    execute: Executes the ETL process for the curated layer.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, mean, max, min, stddev_pop, when
from src.utils.spark_etl import etl
from src.utils.postgresql_db import get_postgresql_options
from src.config.config_loader import load_config
from src.utils.curated_layer_operations import cast_curated_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def curated_layer_transform(df: DataFrame, schema: dict[str, str]) -> DataFrame:
    """
    Aggregates and transforms the cleansed layer DataFrame for the curated layer.

    The transformation process includes:
      1. Extracting the date from the timestamp.
      2. Calculating daily summary statistics (mean, min, max) for each turbine.
      3. Identifying anomalous turbines based on a threshold of two standard deviations.
      4. Casting the resulting columns to match the curated layer schema.

    Args:
        df (DataFrame): The input DataFrame from the cleansed layer.
        schema (dict[str, str]): A dictionary defining the curated layer schema.

    Returns:
        DataFrame: A transformed DataFrame meeting the curated layer specifications.

    Raises:
        Exception: If any error occurs during the transformation process.
    """
    try:
        logger.info("Starting curated layer transformation.")

        # Derive the 'date' column from the 'timestamp' column
        logger.info("Deriving 'date' column from 'timestamp'.")
        df = df.withColumn('date', col('timestamp').cast('date'))

        # Calculate summary statistics
        logger.info("Calculating daily summary statistics.")
        df = (
            df.withColumn('date', col('timestamp').cast('date'))
              .groupBy('turbine_id', 'date')
              .agg(
                  mean('power_output').alias('mean_power_output'),
                  min('power_output').alias('min_power_output'),
                  max('power_output').alias('max_power_output'),
                  stddev_pop('power_output').alias('stddev_power_output')
              )
        )

        # Identify anomalies
        logger.info("Flagging anomalous power output.")
        df = df.withColumn(
            'anomalous_power_output',
            when(
                (col('max_power_output') > col('mean_power_output') + 2 * col('stddev_power_output')) |
                (col('min_power_output') < col('mean_power_output') - 2 * col('stddev_power_output')),
                True
            ).otherwise(False)
        ).drop('stddev_power_output')

        # Cast columns to the specified schema
        logger.info("Casting columns to schema: %s", schema)
        df = cast_curated_schema(df, schema)

        logger.info("Curated layer transformation completed successfully.")
        return df

    except Exception as e:
        logger.error("An error occurred during curated layer transformation: %s", e)
        raise

def execute(date_filter_config=None):
    """
    Executes the ETL process for the curated layer.

    This function orchestrates the extraction of processed data from the cleansed layer,
    transforms it using the `curated_layer_transform` function, and loads it into the
    curated layer table.

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
        logger.info("Starting ETL pipeline execution for the curated layer.")

        # Load configuration
        config = load_config("dev")
        etl_cfg = config["etl_config"]

        # Retrieve configurations for cleansed and curated layers
        cleansed_layer_config = etl_cfg.get("cleansed_layer_config", {})
        curated_layer_config = etl_cfg.get("curated_layer_config", {})

        # Retrieve schema from config
        schema = config["schemas"]["curated"]

        # Extract database name
        database = config["pgsql_database"]

        # Log PostgreSQL options for debugging
        reader_postgresql_options = get_postgresql_options(database, cleansed_layer_config["table"], env="dev")
        logger.info("PostgreSQL options (reader): %s", reader_postgresql_options)

        writer_postgresql_options = get_postgresql_options(database, curated_layer_config["table"], env="dev")
        logger.info("PostgreSQL options (writer): %s", writer_postgresql_options)

        # Merge options into reader and writer configurations
        reader_dict = {**cleansed_layer_config, **reader_postgresql_options}
        logger.info("Final reader configuration: %s", reader_dict)

        writer_dict = {**curated_layer_config, **writer_postgresql_options}
        logger.info("Final writer configuration: %s", writer_dict)

        # Perform ETL
        logger.info("Starting ETL from cleansed to curated...")
        etl(
            reader_dict=reader_dict,
            writer_dict=writer_dict,
            date_filter_config=date_filter_config,
            transform_func=lambda df: curated_layer_transform(df, schema)
        )

        logger.info("ETL pipeline execution for the curated layer completed successfully.")

    except Exception as e:
        logger.error("An error occurred during the ETL pipeline execution: %s", e)
        raise