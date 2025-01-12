"""
This module contains a class for managing SparkSession instances,
used for interacting with Apache Spark and external data sources.
"""
from pyspark.sql import SparkSession
from config.config_loader import load_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkSessionManager:
    """
    A class to manage SparkSession instances.
    """
    _spark_instance = None  # Singleton instance of SparkSession

    @staticmethod
    def get_spark_session(app_name: str = None, env: str = "dev") -> SparkSession:
        """
        Initializes or retrieves a SparkSession instance based on dynamic configuration.

        Args:
            app_name (str): Optional name of the Spark app (overrides the configuration file value).
            env (str): Environment name (e.g., 'dev', 'prod'). Defaults to 'dev'.

        Returns:
            SparkSession: SparkSession instance.
        """
        if SparkSessionManager._spark_instance is None:
            try:
                # Load configuration for the specified environment
                config = load_config(env)
                spark_config = config.get("spark", {})

                # Use the app name from the arguments or the configuration
                app_name = app_name or spark_config.get("app_name", "DefaultApp")

                # Initialize the SparkSession builder
                builder = SparkSession.builder.appName(app_name)

                # Apply Spark-specific configurations
                for key, value in spark_config.get("config", {}).items():
                    builder = builder.config(key, value)

                # Add JAR files if specified in the configuration
                jar_path = spark_config.get("jars")
                if jar_path:
                    builder = builder.config("spark.jars", jar_path)

                # Create the SparkSession
                SparkSessionManager._spark_instance = builder.getOrCreate()
                logger.info(f"SparkSession created successfully for app: {app_name}")

            except Exception as e:
                logger.error(f"Failed to create SparkSession: {e}")
                raise RuntimeError(f"Failed to create SparkSession: {e}")

        return SparkSessionManager._spark_instance

    @staticmethod
    def stop_spark_session() -> None:
        """
        Stops the active SparkSession instance.

        Raises:
            RuntimeError: If the SparkSession cannot be stopped.
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
