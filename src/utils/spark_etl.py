"""
This module provides a comprehensive framework to orchestrate ETL (Extract, Transform, Load)
processes using Apache Spark. It defines classes for reading and writing data using Spark's DataFrame
API, applying optional data transformations and date filtering, and managing the Spark session lifecycle.
Dynamic configuration loading is supported to adapt to various environments (e.g. dev, prod, test).

Key components include:
    - DataSet: Base class representing a dataset with a specific file format and associated options.
    - Reader: Subclass of DataSet for extracting data into a Spark DataFrame from diverse sources.
    - Writer: Subclass of DataSet for writing a Spark DataFrame to a target destination.
    - filter_ingestion_period: Utility function to filter a DataFrame based on a date range.
    - etl: Function to orchestrate the complete ETL process, integrating extraction, optional filtering,
           transformation, and loading of data.

The module leverages a custom SparkSessionManager for managing Spark sessions and a dynamic configuration
loader to retrieve settings tailored to the target environment.
"""

import os
import logging
from typing import Callable, Optional, Any
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, DateType
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException  # For Spark-specific exceptions

from src.config.config_loader import load_config

# Importing SparkSessionManager from spark_session_manager module
from src.utils.spark_session_manager import SparkSessionManager

# Configure the root logger for this module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataSet:
    """
    Base class representing a dataset with a specified file format and associated options.

    This class encapsulates the file format (e.g., CSV, Parquet, JDBC) and holds additional options
    required by Spark during read/write operations.
    """
    def __init__(self, format: str, options: Optional[dict[str, Any]] = None) -> None:
        """
        Initialise the DataSet instance.

        Args:
            format (str): The file format (e.g., 'csv', 'parquet', 'jdbc') used by Spark.
            options (Optional[dict[str, Any]]): Additional Spark options such as header, inferSchema, etc.
                                                Defaults to an empty dictionary if not provided.
        """
        self.format = format
        self.options = options or {}


class Reader(DataSet):
    """
    Subclass of DataSet to read data into a Spark DataFrame.

    The Reader class supports multiple data sources and file formats. It can be configured via a dictionary,
    optionally applying a user-defined schema during the read process.
    """
    def __init__(
        self,
        format: str,
        options: Optional[dict[str, Any]] = None,
        schema: Optional[StructType] = None,
    ) -> None:
        """
        Initialise the Reader instance.

        Args:
            format (str): The format of the dataset (e.g., 'csv', 'parquet', 'jdbc').
            options (Optional[dict[str, Any]]): Spark read options such as path, header, etc.
            schema (Optional[StructType]): An optional user-defined schema to enforce during reading.
        """
        super().__init__(format, options)
        self.schema = schema

    @classmethod
    def get_reader(cls, reader_dict: dict[str, Any]) -> "Reader":
        """
        Create a Reader instance from a configuration dictionary.

        The configuration dictionary can include keys such as 'format', 'path', 'options' (or 'read_options'),
        and 'schema'. If 'format' is missing, it attempts to infer it from the file extension of the 'path'.

        Args:
            reader_dict (dict[str, Any]): Dictionary containing reader configuration.

        Returns:
            Reader: A configured Reader instance.

        Raises:
            ValueError: If neither 'format' nor a 'path' to infer it is provided, or if required options
                        (such as 'url' for JDBC) are missing.
        """
        # Retrieve the options from the dictionary. It could be under "read_options" or "options".
        options = reader_dict.get("read_options") or reader_dict.get("options", reader_dict.copy())

        # Retrieve the file format from the configuration.
        format_ = reader_dict.get("format")
        if not format_:
            # If format is missing, attempt to infer it from the file extension in the 'path'.
            path = options.get("path")
            if not path:
                raise ValueError(
                    "No 'format' specified and no 'path' found in options. Unable to infer format."
                )
            format_ = path.split(".")[-1]  # Infer format from the file extension.

        # For JDBC format, ensure that the 'url' is provided in options.
        if format_ == "jdbc" and "url" not in options:
            raise ValueError("Missing 'url' in options for JDBC format.")

        # Optionally extract the schema from the configuration.
        schema = reader_dict.get("schema")
        # Log the extracted options for debugging.
        logger.info("Options extracted for Reader: %s", options)
        return cls(format_, options, schema)

    def extract(self) -> DataFrame:
        """
        Extract data from the source into a Spark DataFrame.

        Uses SparkSessionManager to obtain an active SparkSession, applies the read options and schema
        (if provided), and loads the data into a DataFrame. Also logs discrepancies between the expected
        and actual schema if a schema is provided.

        Returns:
            DataFrame: A Spark DataFrame containing the extracted data.

        Raises:
            RuntimeError: If no active SparkSession is available.
            ValueError: If critical options (e.g., 'url' for JDBC) are missing.
            AnalysisException: For Spark-specific errors during data extraction.
            Exception: For any other errors encountered during extraction.
        """
        # Retrieve or create the Spark session using SparkSessionManager.
        spark = SparkSessionManager.get_spark_session(app_name="ReaderExtract")
        if not spark:
            raise RuntimeError("No active SparkSession. Ensure SparkSession is started.")

        try:
            # Log the options that will be passed to Spark.
            logger.info("Reader options passed to Spark: %s", self.options)

            # Validate that critical options are present (e.g. 'url' for JDBC format).
            if self.format == "jdbc" and "url" not in self.options:
                raise ValueError("Missing 'url' in reader options for JDBC format.")

            # Create the Spark reader with the specified format and options.
            reader = spark.read.format(self.format).options(**self.options)
            logger.info("Reader options before loading data: %s", self.options)

            # Apply the provided schema if available.
            if self.schema:
                reader = reader.schema(self.schema)

            # Load the data into a DataFrame.
            df = reader.load()

            # If a schema was provided, compare the actual schema with the expected schema.
            if self.schema:
                actual_fields = [f"{f.name}:{f.dataType}" for f in df.schema.fields]
                expected_fields = [f"{f.name}:{f.dataType}" for f in self.schema.fields]
                if actual_fields != expected_fields:
                    logger.warning(
                        "The actual DataFrame schema does not fully match the provided schema.\n"
                        "Expected: %s\nGot: %s",
                        expected_fields,
                        actual_fields,
                    )
            return df

        # Handle Spark-specific errors.
        except AnalysisException as ae:
            logger.error("Spark analysis error during extract: %s", ae)
            raise
        # Handle any other exceptions that occur during extraction.
        except Exception as e:
            logger.error("Generic error during data extraction: %s", e)
            if self.format == "jdbc" and not self.options.get("url"):
                raise ValueError("Missing 'url' in reader options for JDBC format.")
            raise

class Writer(DataSet):
    """
    Subclass of DataSet to write a Spark DataFrame to a destination.

    The Writer class supports various file formats and write modes, allowing data to be written to a target
    location using the specified configuration.
    """
    def __init__(
        self, format: str, options: Optional[dict[str, Any]] = None, mode: str = "error"
    ) -> None:
        """
        Initialise the Writer instance.

        Args:
            format (str): The output format (e.g., 'csv', 'parquet', 'jdbc').
            options (Optional[dict[str, Any]]): Spark write options (e.g., path, header, delimiter).
            mode (str): The write mode, which determines behavior when data exists. Options include
                        'append', 'overwrite', 'error', or 'ignore'. Defaults to 'error'.
        """
        super().__init__(format, options)
        self.mode = mode  # Write mode to handle existing data at the destination.

    @classmethod
    def get_writer(cls, writer_dict: dict[str, Any]) -> "Writer":
        """
        Create a Writer instance from a configuration dictionary.

        The configuration dictionary should contain keys such as 'format', 'path', and optionally 'mode'
        and 'write_options'. The method validates the presence of required parameters and returns a Writer
        instance.

        Args:
            writer_dict (dict[str, Any]): Dictionary containing writer configuration.

        Returns:
            Writer: A configured Writer instance.

        Raises:
            ValueError: If the 'format' key is missing from the configuration.
        """
        # Extract writer options; may be under "write_options" or provided directly.
        options = writer_dict.get("write_options") or writer_dict
        format_ = writer_dict.get("format")
        if not format_:
            raise ValueError("No 'format' specified in writer_dict. Cannot initialize Writer.")
        # Retrieve the write mode; default is 'error' if not provided.
        mode = writer_dict.get("mode", "error")
        return cls(format_, options, mode)

    def load(self, df: DataFrame) -> None:
        """
        Write the provided Spark DataFrame to the destination.

        Utilises the configured output format, options, and write mode to save the DataFrame.
        Logs relevant options before writing and handles any Spark-specific or general exceptions.

        Args:
            df (DataFrame): The Spark DataFrame to be written.

        Raises:
            AnalysisException: For Spark-specific write errors.
            Exception: For any other errors during the write process.
        """
        try:
            # Log the writer options for debugging.
            logger.info("Writer options before save: %s", self.options)
            (
                df.write.format(self.format)  # Specify the format to write the data.
                .options(**self.options)       # Apply additional writer options.
                .mode(self.mode)               # Set the write mode.
                .save()                        # Save the DataFrame.
            )
            logger.info("Data successfully written to destination.")
        # Handle Spark-specific exceptions.
        except AnalysisException as ae:
            logger.error("Spark analysis error during write: %s", ae)
            raise
        # Handle any other exceptions that occur during the write process.
        except Exception as e:
            logger.error("Failed to write data: %s", e)
            raise

def filter_ingestion_period(
    df: DataFrame,
    filter_column: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> DataFrame:
    """
    Filter a Spark DataFrame based on a date range for a specified column.

    Casts the designated column to a DateType and filters rows that fall between the provided start
    and end dates. If no end date is provided, the current date is used; if no start date is provided,
    it defaults to the end date, effectively filtering for a single day.

    Args:
        df (DataFrame): The Spark DataFrame to filter.
        filter_column (str): The name of the column containing date values.
        start_date (Optional[str]): The start date in 'YYYY-MM-DD' format. Defaults to None.
        end_date (Optional[str]): The end date in 'YYYY-MM-DD' format. Defaults to None.

    Returns:
        DataFrame: A Spark DataFrame containing only the rows within the specified date range.

    Raises:
        AnalysisException: For Spark-specific errors during filtering.
        Exception: For any other errors encountered during the filtering process.
    """
    try:
        # Convert end_date to a date object, defaulting to the current date if not provided.
        end_date_dt = (
            datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else datetime.now().date()
        )
        # Convert start_date to a date object, defaulting to end_date if not provided.
        start_date_dt = (
            datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else end_date_dt
        )
        # Filter the DataFrame by casting the filter column to DateType and applying a between condition.
        df_filtered = df.filter(
            col(filter_column).cast(DateType()).between(start_date_dt, end_date_dt)
        )
        logger.info(
            "Data successfully filtered by date from %s to %s.",
            start_date_dt,
            end_date_dt,
        )
        return df_filtered
    except AnalysisException as ae:
        logger.error("Spark analysis error during date filtering: %s", ae)
        raise
    except Exception as e:
        logger.error("Error during date filtering: %s", e)
        raise

def etl(
    reader_dict: dict[str, Any],
    writer_dict: dict[str, Any],
    date_filter_config: Optional[dict[str, str]] = None,
    transform_func: Optional[Callable[[DataFrame], DataFrame]] = None,
    env: str = None,  # If None, load_config will detect the environment
) -> None:
    """
    Orchestrate an end-to-end ETL (Extract, Transform, Load) process using Apache Spark.

    This function coordinates the complete ETL pipeline by:
        1. Loading dynamic configuration and retrieving a Spark session.
        2. Using a Reader to extract data into a Spark DataFrame.
        3. Optionally applying a date filter based on the provided configuration.
        4. Optionally applying a transformation function to modify the DataFrame.
        5. Using a Writer to write the processed data to the specified destination.
        6. Stopping the Spark session upon completion.

    Args:
        reader_dict (dict[str, Any]): Configuration dictionary for the Reader.
        writer_dict (dict[str, Any]): Configuration dictionary for the Writer.
        date_filter_config (Optional[dict[str, str]]): Optional dictionary with keys 'filter_column',
            'start_date', and 'end_date' for filtering the data.
        transform_func (Optional[Callable[[DataFrame], DataFrame]]): An optional function to transform
            the extracted DataFrame.
        env (str, optional): The target environment for configuration (e.g., 'dev', 'prod'). If not provided,
                             the environment is determined by load_config or the APP_ENV variable.

    Raises:
        Exception: Propagates any exceptions encountered during the ETL process.
    """
    spark = None  # Initialize spark variable to hold the Spark session.
    try:
        logger.info("Starting ETL process...")
        # Load configuration using the provided environment or default settings.
        config = load_config(env)
        spark_config = config.get("spark", {})
        # Retrieve the application name from the Spark configuration, or use a default.
        app_name = spark_config.get("app_name", "WindTurbinesProcessing")
        # Determine the final environment; use provided env or fallback to environment variable.
        final_env = env or os.getenv("APP_ENV", "dev")
        # Retrieve or create a Spark session with the specified app name and environment.
        spark = SparkSessionManager.get_spark_session(app_name=app_name, env=final_env)
        logger.info("Spark session retrieved for env='%s' and app name='%s'.", final_env, app_name)

        logger.info("Extracting data...")
        # Log the reader configuration before creating the Reader instance.
        logger.info("Reader configuration before Reader creation: %s", reader_dict)
        # Build a Reader instance using the provided configuration dictionary.
        reader = Reader.get_reader(reader_dict)
        logger.info("Reader created successfully.")
        # Extract data into a DataFrame.
        df = reader.extract()

        # If a date filter configuration is provided, apply the filter.
        if date_filter_config:
            logger.info("Applying date filters with config: %s", date_filter_config)
            df = filter_ingestion_period(
                df,
                filter_column=date_filter_config.get("filter_column", "ingestion_date"),
                start_date=date_filter_config.get("start_date"),
                end_date=date_filter_config.get("end_date"),
            )

        # If a transformation function is provided, apply it to the DataFrame.
        if transform_func:
            logger.info("Applying transform function.")
            df = transform_func(df)

        logger.info("Loading data...")
        # Log the writer configuration before creating the Writer instance.
        logger.info("Final writer_dict before creating Writer: %s", writer_dict)
        # Build a Writer instance using the provided configuration dictionary.
        writer = Writer.get_writer(writer_dict)
        # Write the DataFrame to the destination.
        writer.load(df)
        logger.info("ETL process completed successfully.")

    # Catch and log any unexpected exceptions during the ETL process.
    except Exception as e:
        logger.error("An unexpected error occurred: %s", e)
        raise
    finally:
        # Ensure the Spark session is stopped regardless of success or error.
        if spark:
            logger.info("Stopping Spark session.")
            SparkSessionManager.stop_spark_session()