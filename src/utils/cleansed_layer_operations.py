"""
Cleansed Layer Operations Module

This module contains utility functions for performing data transformations and validations
specific to the cleansed layer of a data pipeline. The cleansed layer is responsible for
cleaning and refining data to ensure its quality and usability for downstream processes.

Key Functions:
- `cast_cleansed_schema`: Casts columns of a DataFrame to specified data types based on a schema.
- `get_conditions`: Builds a combined filtering expression using predefined and dynamic conditions.
- `log_validation_statistics`: Logs the number of rows removed and remaining after a validation step.

Dependencies:
- PySpark for distributed data processing.
- Logging for capturing runtime information.
- Optional integration with external configurations for dynamic conditions.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, count
import logging
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cast_cleansed_schema(df: DataFrame, schema: dict[str, str]) -> DataFrame:
    """
    Casts the DataFrame columns to the specified schema for the cleansed layer.

    This function ensures that all columns in the DataFrame conform to the expected data types
    defined in the schema. It validates the schema, identifies any missing columns, and logs
    the casting process for transparency.

    Args:
        df (DataFrame): The input PySpark DataFrame to be cast.
        schema (dict[str, str]): A dictionary mapping column names to their desired data types.
            Example: {"column_name": "data_type"}

    Returns:
        DataFrame: A DataFrame with columns cast to the specified types.

    Raises:
        ValueError: If any column in the schema is missing from the DataFrame.
        TypeError: If the schema is not a dictionary.
    """
    # Validate schema type
    if not isinstance(schema, dict):
        raise TypeError("Schema must be a dictionary with column names as keys and data types as values.")

    # Validate schema
    missing_columns = [column for column in schema if column not in df.columns]
    if missing_columns:
        raise ValueError(f"The following columns are missing from the DataFrame: {missing_columns}")

    # Mapping PostgreSQL types to PySpark types
    pyspark_type_map = {
        "DOUBLE PRECISION": "DOUBLE",  # Map PostgreSQL DOUBLE PRECISION to PySpark DOUBLE
        "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP"
    }

    for column, data_type in schema.items():
        logger.info(f"Casting column '{column}' to type '{data_type}'.")

        # Convert VARCHAR types to PySpark STRING
        if data_type.startswith('VARCHAR'):
            data_type = 'string'

        # Convert PostgreSQL-specific types to PySpark-compatible types
        data_type = pyspark_type_map.get(data_type.upper(), data_type.rstrip(' NOT NULL').lower())

        df = df.withColumn(column, col(column).cast(data_type))

    logger.info("All columns cast to the specified schema successfully.")
    return df

def get_conditions(conditions: list[str], df: Optional[DataFrame] = None, no_nulls: Optional[bool] = True, dynamic_conditions: Optional[list[str]] = None) -> expr:
    """
    Combines filtering conditions for the cleansed layer.

    This function creates a combined filtering expression using predefined conditions, optional
    additional conditions from a configuration, and an optional check for null values in the DataFrame.

    Args:
        conditions (list[str]): A list of string conditions to be combined using logical AND.
            Example: ["column1 > 0", "column2 < 100"]
        df (Optional[DataFrame]): A PySpark DataFrame to check for NULL columns.
        no_nulls (Optional[bool]): Whether to include a built-in condition to filter out rows
            with NULL values. Defaults to True.
        dynamic_conditions (Optional[list[str]]): Additional conditions from a configuration file
            or external source to be included in the combined expression.

    Returns:
        expr: A PySpark expression combining all specified conditions.
    """
    combined_conditions_list = conditions.copy()

    if dynamic_conditions:
        logger.info("Adding dynamic conditions to the filter.")
        combined_conditions_list.extend(dynamic_conditions)

    combined_condition = expr(" AND ".join(combined_conditions_list))

    if no_nulls and df:
        no_nulls_condition = expr(" AND ".join([f"{column} IS NOT NULL" for column in df.columns]))
        combined_condition = (combined_condition & no_nulls_condition)

    return combined_condition

def log_validation_statistics(df: DataFrame, original_count: int, step_description: str) -> None:
    """
    Logs statistics for a specific validation step.

    This function calculates the number of rows removed during a validation step and logs
    the remaining row count for transparency and debugging purposes.

    Args:
        df (DataFrame): The PySpark DataFrame after the validation step.
        original_count (int): The count of rows before the validation step.
        step_description (str): A brief description of the validation step being logged.

    Example:
        log_validation_statistics(df, 1000, "Filter rows with invalid wind speed")

    Returns:
        None
    """
    new_count = df.count()
    rows_removed = original_count - new_count
    logger.info(f"{step_description}: {rows_removed} rows removed, {new_count} rows remaining.")
