"""
Curated Layer Operations Module

This module provides utility functions for processing data in the curated layer of the ETL pipeline.
The curated layer aggregates and refines data from the cleansed layer, ensuring high data quality
for advanced analytics and reporting.

Key Functions:
- `cast_curated_schema`: Ensures data consistency by casting DataFrame columns to specified types.

Dependencies:
- PySpark: Required for distributed data processing.
- Logging: Used to monitor schema transformation steps.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def cast_curated_schema(df: DataFrame, schema: dict[str, str]) -> DataFrame:
    """
    Casts the DataFrame columns to the specified schema for the curated layer.

    This function ensures that all columns in the DataFrame conform to the expected data types
    defined in the schema. If any required column is missing, an error is raised.

    Args:
        df (DataFrame): The input DataFrame.
        schema (dict[str, str]): A dictionary where keys are column names and values are their desired data types.

    Returns:
        DataFrame: A DataFrame with columns cast to the specified types.

    Raises:
        ValueError: If any column in the schema is missing from the DataFrame.
        TypeError: If `schema` is not a dictionary.
    """
    # Validate schema type
    if not isinstance(schema, dict):
        raise TypeError("Schema must be a dictionary with column names as keys and data types as values.")

    # Validate schema
    missing_columns = [col for col in schema.keys() if col not in df.columns]
    if missing_columns:
        raise ValueError(f"The following columns are missing from the DataFrame: {missing_columns}")

    # Mapping PostgreSQL types to PySpark types
    pyspark_type_map = {
        "DOUBLE PRECISION": "DOUBLE",  # Map PostgreSQL DOUBLE PRECISION to PySpark DOUBLE
        "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
        "BOOLEAN": "BOOLEAN",
    }

    for column, data_type in schema.items():
        logger.info(f"Casting column '{column}' to type '{data_type}'.")

        # Convert PostgreSQL-specific types to PySpark-compatible types
        data_type = pyspark_type_map.get(data_type.upper(), data_type.rstrip(' NOT NULL').lower())

        df = df.withColumn(column, col(column).cast(data_type))

    logger.info("All columns cast to the specified schema successfully.")
    return df
