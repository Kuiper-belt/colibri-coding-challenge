import logging
from src.utils import SparkSessionManager
from src.config import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_postgresql_connection():
    """
    Tests the connection to a PostgreSQL database using SparkSession.
    Reads data from a table and displays it if the connection is successful.
    """
    # Load configuration for the current environment
    config = load_config()  # Automatically detects environment or defaults to 'dev'
    db_config = config["database"]
    spark_config = config["spark"]

    # Create or retrieve a SparkSession
    spark = SparkSessionManager.get_spark_session(
        app_name=spark_config["app_name"],
        env="dev"  # You can dynamically detect or override this
    )

    try:
        logger.info(f"Attempting to connect to PostgreSQL table {db_config['table']}...")

        # Read data from PostgreSQL using JDBC
        df = spark.read.jdbc(
            url=db_config["jdbc_url"],
            table=db_config["table"],
            properties={
                "user": db_config["user"],
                "password": db_config["password"],
                "driver": db_config["driver"]
            }
        )

        logger.info(f"Connection successful! Data from {db_config['table']}:")
        df.show()
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
    finally:
        # Ensure the SparkSession is stopped after execution
        SparkSessionManager.stop_spark_session()


def main():
    """
    Main entry point for the application.
    Executes the PostgreSQL connection test.
    """
    logger.info("Starting the application...")
    test_postgresql_connection()
    logger.info("Application finished successfully.")


if __name__ == "__main__":
    main()
