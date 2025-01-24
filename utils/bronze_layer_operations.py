"""
Utility functions for operations in the bronze layer of the ETL pipeline.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def cast_bronze_schema(df: DataFrame, schema: dict[str, str]) -> DataFrame:
    """
    Casts the DataFrame columns to the specified schema for the bronze layer.

    Args:
        df (DataFrame): The input DataFrame.
        schema (dict[str, str]): The schema to cast the DataFrame columns to.

    Returns:
        DataFrame: A DataFrame with columns cast to the specified types.

    Raises:
        ValueError: If any column in the schema is missing from the DataFrame.
    """
    missing_columns = [column for column in schema if column not in df.columns]
    if missing_columns:
        raise ValueError(f"The following columns are missing from the DataFrame: {missing_columns}")

    for column, data_type in schema.items():
        logger.info(f"Casting column '{column}' to type '{data_type}'.")
        df = df.withColumn(column, col(column).cast(data_type))

    logger.info("All columns cast to the specified schema successfully.")
    return df
