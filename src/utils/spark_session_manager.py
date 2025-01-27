"""
Module: spark_session_manager

This module provides a centralized class, `SparkSessionManager`, for managing the lifecycle of SparkSession instances.

The SparkSession is the entry point to using PySpark, and this module ensures that:
- SparkSessions are initialized with configurations specific to the given environment (e.g., dev, prod).
- Custom configurations such as application name, Spark settings, and external JAR files are seamlessly applied.
- A singleton pattern is implemented to avoid redundant SparkSession creation.
- The active SparkSession can be gracefully stopped when it is no longer needed.

The `SparkSessionManager` is particularly useful in ETL pipelines or other Spark-based applications where consistent configuration
and resource management are critical. The module leverages external configuration files, enabling flexibility across multiple environments.

Dependencies:
- `pyspark.sql.SparkSession`: For creating and managing SparkSession instances.
- `src.config.config_loader`: To dynamically load environment-specific configurations.
- `logging`: For runtime information and error tracking.
"""

from pyspark.sql import SparkSession
from src.config.config_loader import load_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkSessionManager:
    """
    A class to manage the lifecycle of SparkSession instances.

    This class ensures that a single instance of SparkSession is created and shared across the application
    (singleton pattern). It dynamically configures the SparkSession based on the
    environment (e.g., dev, prod) and additional custom settings, such as JAR files and Spark-specific options.
    """
    _spark_instance = None  # Singleton instance of SparkSession

    @staticmethod
    def get_spark_session(app_name: str = None, env: str = "dev") -> SparkSession:
        """
        Retrieves or initializes a SparkSession instance with environment-specific configurations.

        This method creates a SparkSession if none exists, applying settings specified in the environment
        configuration file. It supports dynamically setting the application name, custom Spark configurations,
        and external JAR dependencies.

        Args:
            app_name (str, optional): The name of the Spark application. Defaults to the value specified
            in the configuration file. env (str): The environment for which to configure
            the SparkSession (e.g., 'dev', 'test', 'prod'). Defaults to 'dev'.

        Returns:
            SparkSession: The active SparkSession instance, configured based on the specified environment and settings.

        Raises:
            RuntimeError: If an error occurs during SparkSession initialization.
        """
        if SparkSessionManager._spark_instance is None:
            try:
                # Load configuration for the specified environment
                logger.info(f"Loading configuration for environment: {env}")
                config = load_config(env)
                spark_config = config.get("spark", {})

                # Determine application name
                app_name = app_name or spark_config.get("app_name", "DefaultApp")

                # Initialize SparkSession builder
                logger.info(f"Initializing SparkSession for app: {app_name}")
                builder = SparkSession.builder.appName(app_name)

                # Apply Spark configurations
                for key, value in spark_config.get("config", {}).items():
                    logger.info(f"Applying Spark configuration: {key}={value}")
                    builder = builder.config(key, value)

                # Include JAR files if specified
                jar_path = spark_config.get("jars")
                if jar_path:
                    logger.info(f"Adding JARs to SparkSession: {jar_path}")
                    builder = builder.config("spark.jars", jar_path)

                # Create and store the SparkSession
                SparkSessionManager._spark_instance = builder.getOrCreate()
                logger.info(f"SparkSession successfully created for app: {app_name}")

            except Exception as e:
                logger.error(f"Error creating SparkSession: {e}")
                raise RuntimeError(f"Failed to create SparkSession: {e}")

        return SparkSessionManager._spark_instance

    @staticmethod
    def stop_spark_session() -> None:
        """
        Stops the active SparkSession instance.

        This method ensures that any active SparkSession is gracefully terminated, releasing associated resources.
        If no active SparkSession exists, a warning is logged.

        Raises:
            RuntimeError: If an error occurs while stopping the SparkSession.
        """
        try:
            if SparkSessionManager._spark_instance is not None:
                SparkSessionManager._spark_instance.stop()
                logger.info("SparkSession stopped successfully.")
                SparkSessionManager._spark_instance = None
            else:
                logger.warning("No active SparkSession to stop.")
        except Exception as e:
            logger.error(f"Failed to stop SparkSession: {e}")
            raise RuntimeError(f"Failed to stop SparkSession: {e}")
