"""
This module contains utility functions and classes for interacting with PostgreSQL
databases using Python and PySpark, integrated with a dynamic config setup.
"""
import psycopg2
from psycopg2 import sql
from src.config.config_loader import load_config
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from urllib.parse import urlparse


class PostgreSQLManager:
    def __init__(self, env="dev") -> None:
        """
        Initializes a PostgreSQLManager instance.

        Args:
            env (str): Environment name (e.g., 'dev', 'prod').
        """
        # Load configuration for the specified environment
        config = load_config(env)
        db_config = config.get("database", {})

        # Parse JDBC URL using urlparse
        jdbc_url = db_config.get("jdbc_url", "jdbc:postgresql://localhost:5432/postgres")
        parsed_url = urlparse(jdbc_url.replace("jdbc:", ""))

        self.host = parsed_url.hostname or "localhost"
        self.port = parsed_url.port or 5432  # Default PostgreSQL port
        self.default_database = parsed_url.path.lstrip("/") or "postgres"
        self.user = db_config.get("user", "postgres")
        self.password = db_config.get("password", None)
        self.connection = None

    def connect(self, database=None) -> None:
        """
        Establishes a connection to the PostgreSQL database.

        Args:
            database (str): Database name to connect to. Defaults to `default_database`.

        Raises:
            RuntimeError: If connection to PostgreSQL fails.
        """
        database = database or self.default_database
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=database
            )
            logger.info(f"Connected to PostgreSQL database '{database}' successfully.")
        except psycopg2.OperationalError as e:
            if "does not exist" in str(e):
                logger.warning(f"Database '{database}' does not exist. Connecting to 'postgres' for database creation.")
                # Connect to 'postgres' to create the missing database
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    dbname="postgres"
                )
                logger.info("Connected to the 'postgres' database successfully.")
            else:
                logger.error(f"OperationalError: Failed to connect to PostgreSQL: {e}")
                raise

    def create_database(self, database: str) -> None:
        """
        Creates a PostgreSQL database if it doesn't exist.

        Args:
            database (str): Name of the database to be created.
        """
        try:
            if not self.connection:
                self.connect()
            self.connection.autocommit = True
            cursor = self.connection.cursor()
            cursor.execute(
                sql.SQL("CREATE DATABASE {};").format(sql.Identifier(database))
            )
            logger.info(f"Database '{database}' created successfully.")
        except psycopg2.Error as e:
            if "already exists" not in str(e):
                logger.error(f"Failed to create PostgreSQL database: {e}")
                raise
            else:
                logger.warning(f"Database '{database}' already exists.")
        finally:
            if cursor:
                cursor.close()

    def create_tables(self, database: str, schemas: dict[str, dict[str, str]],
                      constraints: dict[str, list[str]] = None) -> None:
        """
        Creates PostgreSQL tables in the specified database.

        Args:
            database (str): Name of the database where tables will be created.
            schemas (dict[str, dict[str, str]]): Table schemas with column definitions.
            constraints (dict[str, list[str]]): Table-specific constraints like primary keys, foreign keys.
        """
        try:
            if not self.connection:
                self.connect(database)
            cursor = self.connection.cursor()

            for table, columns in schemas.items():
                # Define columns
                column_string = ", ".join(f"{col} {dtype}" for col, dtype in columns.items())

                # Add constraints
                constraint_string = ", ".join(constraints.get(table, [])) if constraints else ""
                full_table_definition = f"{column_string}, {constraint_string}" if constraint_string else column_string

                # Execute the SQL
                cursor.execute(
                    sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
                        sql.Identifier(table), sql.SQL(full_table_definition)
                    )
                )
                logger.info(f"Table '{table}' created successfully in database '{database}'.")
        except psycopg2.Error as e:
            logger.error(f"Failed to create tables: {e}")
            raise RuntimeError(f"Failed to create PostgreSQL tables: {e}")
        finally:
            if cursor:
                cursor.close()

    def close_connection(self) -> None:
        """
        Closes the PostgreSQL connection.

        Raises:
            RuntimeError: If closing PostgreSQL connection fails.
        """
        try:
            if self.connection:
                self.connection.close()
                logger.info("PostgreSQL connection closed successfully.")
        except psycopg2.Error as e:
            logger.error(f"Failed to close PostgreSQL connection: {e}")
            raise RuntimeError(f"Failed to close PostgreSQL connection: {e}")


def get_postgresql_options(database: str, table: str, env="dev") -> dict[str, str]:
    """
    Constructs options for reading or writing data from/to PostgreSQL using PySpark.

    Args:
        database (str): Name of the database.
        table (str): Name of the table.
        env (str): Environment name (e.g., 'dev', 'prod').

    Returns:
        dict[str, str]: Options for PostgreSQL connection.
    """
    config = load_config(env)
    db_config = config.get("database", {})
    jdbc_url = db_config.get("jdbc_url") + database  # Append the database name
    return {
        "url": jdbc_url,
        "driver": db_config.get("driver", "org.postgresql.Driver"),
        "user": db_config.get("user"),
        "password": db_config.get("password"),
        "dbtable": table
    }