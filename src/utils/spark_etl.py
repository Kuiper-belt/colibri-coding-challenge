"""
Improved ETL utility module with Apache Spark, integrating SparkSessionManager
from spark_session_manager.py.

Features & Changes:
1. Configuration validation (# [UPDATED])
2. Schema validation (# [ADDED])
3. Improved error handling (# [UPDATED])
4. Parameterized logging (# [UPDATED])
5. Optional advanced checks (commented out) (# [ADDED])

Usage:
- Use the 'etl' function as the main entry point for your ETL pipeline.
- The 'Reader' and 'Writer' classes can be configured via dictionaries.
"""

import os
import logging
from typing import Callable, Optional, Any
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, DateType
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException  # For Spark-specific exceptions

# [ADDED] to load our JSON config (dev_config.json or another environment's config)
from src.config.config_loader import load_config

# Importing SparkSessionManager from spark_session_manager module
from src.utils.spark_session_manager import SparkSessionManager

# Configure the root logger for this module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataSet:
    """
    A base class to represent a dataset, specifying a file format and associated options.
    """

    def __init__(self, format: str, options: Optional[dict[str, Any]] = None) -> None:
        """
        Initializes DataSet instance.

        Args:
            format (str): Format for dataset (e.g., 'csv', 'parquet').
            options (Optional[dict[str, Any]]): Key-value pairs for reader/writer config.
        """
        # 'format' indicates how Spark should interpret the data (e.g., 'csv', 'parquet', etc.).
        self.format = format
        # 'options' holds additional Spark options (like header=True, inferSchema=True, etc.).
        self.options = options or {}


class Reader(DataSet):
    """
    Subclass of DataSet that reads data from a specified path or source.
    """

    def __init__(
        self,
        format: str,
        options: Optional[dict[str, Any]] = None,
        schema: Optional[StructType] = None,
    ) -> None:
        """
        Initializes Reader instance.

        Args:
            format (str): Format for the dataset (e.g., 'csv', 'parquet').
            options (Optional[dict[str, Any]]): Spark read options.
            schema (Optional[StructType]): Optional user-defined schema for reading.
        """
        super().__init__(format, options)
        self.schema = schema  # If provided, Spark will apply this schema upon reading.

    @classmethod
    def get_reader(cls, reader_dict: dict[str, Any]) -> "Reader":
        """
            Alternative constructor that returns a Reader instance from a dictionary config.

            Args:
                reader_dict (dict[str, Any]): Configuration dictionary (format, path, schema, etc.).

            Returns:
                Reader: An instance of the Reader class.
            """
        # Retrieve the 'options' from the dictionary. Could be labeled as 'read_options' or 'options'.
        options = reader_dict.get("read_options") or reader_dict.get("options", reader_dict.copy())

        # Retrieve the 'format' from the dictionary
        format_ = reader_dict.get("format")

        if not format_:
            # If 'format' is not explicitly given, try to infer from the file extension in 'path'.
            path = options.get("path")
            if not path:
                raise ValueError(
                    "No 'format' specified and no 'path' found in options. "
                    "Unable to infer format."
                )
            format_ = path.split(".")[-1]  # e.g., "csv" if path ends with ".csv"

        # Special case for JDBC format: Ensure 'url' is present
        if format_ == "jdbc" and "url" not in options:
            raise ValueError("Missing 'url' in options for JDBC format.")

        # Optional: Extract the schema if provided
        schema = reader_dict.get("schema")

        # Log the extracted options for debugging
        logger.info("Options extracted for Reader: %s", options)

        # Construct and return a Reader instance
        return cls(format_, options, schema)

    def extract(self) -> DataFrame:
        """
        Reads data from the specified dataset into a Spark DataFrame.

        Returns:
            DataFrame: Spark DataFrame containing the extracted data.
        """

        # Use SparkSessionManager to get or create the SparkSession
        spark = SparkSessionManager.get_spark_session(app_name="ReaderExtract")
        if not spark:
            raise RuntimeError(
                "No active SparkSession. Ensure SparkSession is started."
            )

        try:
            # Log the current options passed to the reader
            logger.info("Reader options passed to Spark: %s", self.options)

            # Validate critical options (e.g., 'url' for JDBC)
            if self.format == "jdbc" and "url" not in self.options:
                raise ValueError("Missing 'url' in reader options for JDBC format.")

            # Use the Spark reader with the specified format/options
            reader = spark.read.format(self.format).options(**self.options)

            # Log the options to debug issues
            logger.info("Reader options before loading data: %s", self.options)

            # If a schema was provided, apply it to the reader
            if self.schema:
                reader = reader.schema(self.schema)

            # Actually load the data into a DataFrame
            df = reader.load()

            # Optional: Compare the actual schema to the expected schema, if provided
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

        # Catch Spark-specific errors (AnalysisException) separately
        except AnalysisException as ae:
            logger.error("Spark analysis error during extract: %s", ae)
            raise
        except Exception as e:
            logger.error("Generic error during data extraction: %s", e)
            if self.format == "jdbc" and not self.options.get("url"):
                raise ValueError("Missing 'url' in reader options for JDBC format.")
            raise


class Writer(DataSet):
    """
    Subclass of DataSet that writes out data to a specified path or destination.
    """

    def __init__(
        self, format: str, options: Optional[dict[str, Any]] = None, mode: str = "error"
    ) -> None:
        """
        Initializes Writer instance.

        Args:
            format (str): Format to write the dataset in (e.g., 'csv', 'parquet').
            options (Optional[dict[str, Any]]): Spark write options.
            mode (str): Write mode (append, overwrite, error, ignore).
        """
        super().__init__(format, options)
        self.mode = mode  # Defines how Spark handles existing data at the destination.

    @classmethod
    def get_writer(cls, writer_dict: dict[str, Any]) -> "Writer":
        """
        Alternative constructor that returns a Writer instance from a dictionary config.

        Args:
            writer_dict (dict[str, Any]): Configuration dictionary (format, path, mode, etc.).

        Returns:
            Writer: An instance of the Writer class.
        """
        # Check for "write_options" or directly use the writer_dict
        options = writer_dict.get("write_options") or writer_dict

        # Validate that a format is provided
        format_ = writer_dict.get("format")
        if not format_:
            raise ValueError("No 'format' specified in writer_dict. Cannot initialize Writer.")

        # Default write mode is 'error', but can be overridden
        mode = writer_dict.get("mode", "error")

        # Construct and return a Writer instance
        return cls(format_, options, mode)

    def load(self, df: DataFrame) -> None:
        """
        Writes the provided DataFrame to the specified destination.

        Args:
            df (DataFrame): The DataFrame to be written.
        """
        try:
            logger.info("Writer options before save: %s", self.options)
            (
                df.write.format(self.format)
                .options(**self.options)
                .mode(self.mode)
                .save()
            )
            logger.info("Data successfully written to destination.")

        # Catch Spark-specific errors separately
        except AnalysisException as ae:
            logger.error("Spark analysis error during write: %s", ae)
            raise
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
    Filters the DataFrame based on a specified ingestion period.

    Args:
        df (DataFrame): The DataFrame to be filtered.
        filter_column (str): Column name in df that holds date information.
        start_date (Optional[str]): Start date in 'YYYY-MM-DD' format. Defaults to today's date if None.
        end_date (Optional[str]): End date in 'YYYY-MM-DD' format. Defaults to today's date if None.

    Returns:
        DataFrame: Filtered DataFrame containing rows whose date is in [start_date, end_date].
    """
    try:
        # If end_date not provided, use the current date
        end_date_dt = (
            datetime.strptime(end_date, "%Y-%m-%d").date()
            if end_date
            else datetime.now().date()
        )
        # If start_date not provided, default to end_date (single-day filter if both are None)
        start_date_dt = (
            datetime.strptime(start_date, "%Y-%m-%d").date()
            if start_date
            else end_date_dt
        )

        # Perform the Spark filter with cast to DateType
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
    env: str = None,  # <--- We'll let load_config detect environment if None
) -> None:
    """
    Orchestrates an ETL job using Reader and Writer configurations, optional date filtering,
    and an optional transformation step.

    Args:
        reader_dict (dict[str, Any]): Dictionary with options for the Reader (e.g., format, path).
        writer_dict (dict[str, Any]): Dictionary with options for the Writer (e.g., format, path, mode).
        date_filter_config (Optional[dict[str, str]]): Optional dictionary containing date filter params
            like {'filter_column': 'ingestion_date', 'start_date': '2023-01-01', 'end_date': '2023-02-01'}.
        transform_func (Optional[Callable[[DataFrame], DataFrame]]): Optional function that applies
            additional transformations to the DataFrame before writing.
        env (str): Environment name to load config from (e.g., 'dev', 'prod').
                   If None, config_loader defaults to 'dev' or the APP_ENV environment variable.

    Steps:
        1. Loads config via load_config(env).
        2. Retrieves a SparkSession from SparkSessionManager,
           passing the app_name from the loaded config (if present).
        3. Extracts the data using 'Reader'.
        4. Applies date filtering if 'date_filter_config' is provided.
        5. Applies a user-defined transformation function if provided.
        6. Loads the data using 'Writer'.
        7. Stops the SparkSession once done.
    """
    spark = None
    try:
        logger.info("Starting ETL process...")

        # Load the entire config dictionary
        config = load_config(env)  # If env is None, it defaults to 'dev' or uses APP_ENV
        spark_config = config.get("spark", {})
        app_name = spark_config.get("app_name", "WindTurbinesProcessing")

        # Create or retrieve Spark session with the loaded app_name + environment
        # If env is None, "load_config" might have used "dev" anyway, but let's be explicit:
        final_env = env or os.getenv("APP_ENV", "dev")

        # Retrieve or create Spark session from SparkSessionManager
        spark = SparkSessionManager.get_spark_session(
            app_name=app_name,
            env=final_env
        )

        logger.info(
            "Spark session retrieved for env='%s' and app name='%s'.",
            final_env,
            app_name
        )

        logger.info("Extracting data...")

        # Log reader_dict before passing it to the Reader
        logger.info("Reader configuration before Reader creation: %s", reader_dict)

        # Build a Reader instance from the 'reader_dict' config
        reader = Reader.get_reader(reader_dict)

        # Execute the extract method to get a DataFrame
        logger.info("Reader created successfully.")
        df = reader.extract()

        # If date_filter_config is provided, perform date-based filtering
        if date_filter_config:
            logger.info("Applying date filters with config: %s", date_filter_config)
            df = filter_ingestion_period(
                df,
                filter_column=date_filter_config.get("filter_column", "ingestion_date"),
                start_date=date_filter_config.get("start_date"),
                end_date=date_filter_config.get("end_date"),
            )

        # Apply any custom transformation function
        if transform_func:
            logger.info("Applying transform function.")
            df = transform_func(df)

        logger.info("Loading data...")

        # Log the final writer_dict to debug issues with writer options
        logger.info("Final writer_dict before creating Writer: %s", writer_dict)

        # Build a Writer instance from the 'writer_dict' config
        writer = Writer.get_writer(writer_dict)

        # Execute the load method to write out the DataFrame
        writer.load(df)

        logger.info("ETL process completed successfully.")

    except Exception as e:
        # Catch any exception from the entire pipeline and log it
        logger.error("An unexpected error occurred: %s", e)
        raise
    finally:
        if spark:  # If spark is not None, call the manager's stop method
            logger.info("Stopping Spark session.")
            SparkSessionManager.stop_spark_session()
