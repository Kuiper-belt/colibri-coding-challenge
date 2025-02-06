"""
Ingestion Layer Operations Module

This module contains utility functions for handling operations in the ingestion layer of an ETL pipeline.
The ingestion layer is responsible for ingesting and preparing raw data for further processing. It ensures
data consistency by enforcing schema integrity and providing detailed logging for debugging.

Key Functions:
- `cast_ingestion_schema`: Casts DataFrame columns to a specified schema, ensuring compatibility with downstream layers.

Dependencies:
- PySpark: For distributed data processing and schema enforcement.
- Logging: For runtime insights and debugging.
"""

import os
from glob import glob
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cast_ingestion_schema(df: DataFrame, schema: dict[str, str], skip_columns: list[str] = None) -> DataFrame:
    """
    Casts the DataFrame columns to the specified schema for the ingestion layer.

    This function ensures that all columns in the DataFrame conform to the expected data types
    as defined in the schema. It performs validation to ensure all required columns exist, except
    for those explicitly marked to be added later. Detailed logging is provided to track the
    transformation process.

    Args:
        df (DataFrame): The input PySpark DataFrame to be cast.
        schema (dict[str, str]): A dictionary defining the target schema, where keys are column
            names and values are the desired data types (e.g., {"column1": "string"}).
        skip_columns (list[str], optional): A list of column names to exclude from validation.
            These columns are expected to be added later in the ETL pipeline.

    Returns:
        DataFrame: A DataFrame with columns cast to the specified types, ready for downstream processing.

    Raises:
        ValueError: If any required column (not in `skip_columns`) is missing from the DataFrame.
        TypeError: If a column's data type cannot be cast as specified in the schema.
    """
    skip_columns = skip_columns or []

    # Log the input schema and DataFrame schema for debugging
    logger.info("Input schema: %s", schema)
    logger.info("Input DataFrame schema: %s", df.dtypes)

    # Validate schema
    missing_columns = [column for column in schema if column not in df.columns and column not in skip_columns]
    if missing_columns:
        raise ValueError(f"The following columns are missing from the DataFrame: {missing_columns}")

    # Mapping for PySpark-compatible types
    pyspark_type_map = {
        "DOUBLE PRECISION": "DOUBLE",  # Map PostgreSQL DOUBLE PRECISION to PySpark DOUBLE
        "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP"  # Example of additional mappings
    }

    for column, data_type in schema.items():
        if column in skip_columns:
            logger.info("Skipping column '%s' as it will be added later.", column)
            continue

        if column not in df.columns:
            logger.error("Column '%s' is missing from the DataFrame.", column)
            raise ValueError(f"Column '{column}' is missing from the DataFrame.")

        # Translate PostgreSQL type to PySpark-compatible type
        data_type = pyspark_type_map.get(data_type.upper(), data_type)

        # Log the data type before casting
        actual_type = dict(df.dtypes).get(column)
        logger.info("Column '%s': current type '%s', target type '%s'.", column, actual_type, data_type)

        try:
            df = df.withColumn(column, col(column).cast(data_type))
            logger.info("Successfully cast column '%s' to type '%s'.", column, data_type)
        except Exception as e:
            logger.error("Failed to cast column '%s' to type '%s': %s", column, data_type, e)
            raise TypeError(f"Failed to cast column '{column}' to type '{data_type}': {e}")

    # Log the resulting DataFrame schema
    logger.info("Resulting DataFrame schema: %s", df.dtypes)
    logger.info("All columns cast to the specified schema successfully.")
    return df

def get_all_csv_files(directory: str) -> list:
    """
    Retrieve all CSV files from the given directory.

    Args:
        directory (str): The folder path containing CSV files.

    Returns:
        list: A list of absolute file paths formatted for Spark (file:///E:/path/to/file.csv).

    Raises:
        FileNotFoundError: If no CSV files are found in the specified directory.
    """
    files = glob(os.path.join(directory, "*.csv"))

    if not files:
        raise FileNotFoundError(f"No CSV files found in directory: {directory}")

    # Convert Windows-style paths to Spark-compatible format (file:///E:/...)
    return [f"file://{os.path.abspath(f).replace(os.sep, '/')}" for f in files]