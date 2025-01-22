"""
Utility functions for operations in the gold layer of the ETL pipeline.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def cast_gold_schema(df: DataFrame, schema: dict[str, str]) -> DataFrame:
    """
    Casts the DataFrame columns to the specified schema for the gold layer.

    Args:
        df (DataFrame): The input DataFrame.
        schema (dict[str, str]): The schema to cast the DataFrame columns to.

    Returns:
        DataFrame: A DataFrame with columns cast to the specified types.
    """
    for column, data_type in schema.items():
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame.")
        df = df.withColumn(column, col(column).cast(data_type))
    return df
