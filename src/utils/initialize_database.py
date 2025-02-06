"""
This script initializes a PostgreSQL database and its tables based on configurations
defined in the project. It uses the PostgreSQLManager utility class to manage the database
creation and table setup.
"""

import logging
from src.utils.postgresql_db import PostgreSQLManager
from src.config.config_loader import load_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_database(env="dev"):
    """
    Initializes the database and tables based on the configuration.

    Args:
        env (str): The environment name (e.g., 'dev', 'prod').
    """
    try:
        # Load configuration
        logger.info(f"Loading configuration for environment: {env}")
        config = load_config(env)

        # Database and table schema details
        database_name = config["pgsql_database"]  # Use the correct key for the database name
        schemas = config["schemas"]

        # Initialize PostgreSQLManager
        logger.info("Initializing PostgreSQLManager.")
        pg_manager = PostgreSQLManager(env)

        # Create database if it doesn't exist
        logger.info(f"Ensuring database '{database_name}' exists.")
        pg_manager.create_database(database_name)

        # Create tables
        logger.info(f"Creating tables in database '{database_name}'.")
        for table_name, schema in schemas.items():
            pg_manager.create_tables(database_name, {table_name: schema})

    except Exception as e:
        logger.error(f"An error occurred during database initialization: {e}")
        raise
    finally:
        # Close connection
        if 'pg_manager' in locals():
            pg_manager.close_connection()
            logger.info("Database initialization completed.")

if __name__ == "__main__":
    # Default to 'dev' environment if no argument is provided
    initialize_database(env="dev")
