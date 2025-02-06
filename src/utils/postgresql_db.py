"""
This module provides utility functions and classes for interacting with PostgreSQL
databases using Python and PySpark. It includes a dynamic configuration setup that
supports multiple environments (e.g. dev, prod, test) and encapsulates common operations
such as connecting to the database, creating databases and tables, and closing connections.

Key features:
  - Dynamically loads configuration for different environments.
  - Parses a JDBC URL to extract connection parameters.
  - Provides methods for establishing and closing connections.
  - Supports database and table creation with error handling.
  - Constructs connection options for PySpark integration.

Dependencies:
  - psycopg2 for PostgreSQL connectivity.
  - PySpark for processing large datasets in distributed environments.
  - A custom configuration loader module (src.config.config_loader) for managing settings.
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
        Initialise a PostgreSQLManager instance.

        This method loads the configuration for the specified environment and extracts
        the necessary connection parameters from a JDBC URL.

        Args:
            env (str): The target environment name (e.g. 'dev', 'prod', 'test').

        Attributes:
            host (str): The PostgreSQL server host.
            port (int): The PostgreSQL server port.
            default_database (str): The default database to connect to.
            user (str): The database username.
            password (str or None): The database password.
            connection (psycopg2.extensions.connection or None): The active connection object.
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
        Establish a connection to a PostgreSQL database.

        If a connection already exists and is open, the method logs the status and returns
        immediately. In case of a connection failure due to a missing target database, the
        method attempts to connect to the default 'postgres' database to allow for database creation.

        Args:
            database (str, optional): The name of the database to connect to.
                                      Defaults to the configured default_database.

        Raises:
            psycopg2.OperationalError: If the connection cannot be established for reasons
                                         other than a missing database.
        """
        if self.connection and not self.connection.closed:
            logger.info(f"Already connected to PostgreSQL: {self.host}/{database or self.default_database}")
            return

        database = database or self.default_database
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=database
            )
            self.connection.cursor()
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
        Create a PostgreSQL database if it does not already exist.

        This method checks whether the specified database exists by querying the system
        catalog. If the database is absent, it executes a CREATE DATABASE statement. If a
        permission error is encountered, it logs the error and raises a RuntimeError.

        Args:
            database (str): The name of the database to be created.

        Raises:
            RuntimeError: If the user does not have sufficient privileges to create the database,
                          or if any other error occurs during the creation process.
        """
        cursor = None
        try:
            if not self.connection:
                self.connect()
            self.connection.autocommit = True
            cursor = self.connection.cursor()

            cursor.execute(
                sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [database]
            )
            exists = cursor.fetchone()

            if exists:
                logger.warning(f"Database '{database}' already exists.")
                return

            cursor.execute(
                sql.SQL("CREATE DATABASE {};").format(sql.Identifier(database))
            )
            logger.info(f"Database '{database}' created successfully.")
        except psycopg2.Error as e:
            # Convert error message to lowercase for case-insensitive comparison.
            if "permission denied" in str(e).lower():
                logger.error(f"Insufficient privileges to create database '{database}': {e}")
                raise RuntimeError(f"Permission denied: {e}")
            else:
                logger.error(f"Failed to create PostgreSQL database: {e}")
                raise
        finally:
            if cursor:
                cursor.close()

    def create_tables(self, database: str, schemas: dict[str, dict[str, str]],
                      constraints: dict[str, list[str]] = None) -> None:
        """
        Create tables in the specified PostgreSQL database.

        This method iterates over a dictionary of table schemas and constructs a CREATE TABLE
        statement for each table. Optional constraints such as primary keys or foreign keys may be
        appended. All SQL executions are performed within a transaction that is committed upon
        success or rolled back upon failure.

        Args:
            database (str): The name of the database where the tables will be created.
            schemas (dict[str, dict[str, str]]): A dictionary mapping table names to their column
                                                  definitions. Each column definition is provided
                                                  as a mapping of column name to SQL datatype.
            constraints (dict[str, list[str]], optional): A dictionary mapping table names to a list
                                                          of SQL constraint strings. Defaults to None.

        Raises:
            RuntimeError: If table creation fails, along with a descriptive error message.
        """
        cursor = None
        try:
            if not self.connection:
                self.connect(database)
            cursor = self.connection.cursor()
            self.connection.autocommit = False  # Start transaction

            for table, columns in schemas.items():
                # Define columns
                column_string = ", ".join(f"{col} {dtype}" for col, dtype in columns.items())
                # Add constraints if provided
                constraint_string = ", ".join(constraints.get(table, [])) if constraints else ""
                full_table_definition = f"{column_string}, {constraint_string}" if constraint_string else column_string

                # Execute the SQL to create the table
                cursor.execute(
                    sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
                        sql.Identifier(table), sql.SQL(full_table_definition)
                    )
                )
                logger.info(f"Table '{table}' created successfully in database '{database}'.")

            self.connection.commit()  # Commit changes if no errors
        except psycopg2.Error as e:
            if self.connection:
                self.connection.rollback()  # Rollback in case of failure
            logger.error(f"Failed to create tables: {e}")
            raise RuntimeError(f"Failed to create PostgreSQL tables: {e}")
        finally:
            if cursor:
                cursor.close()

    def close_connection(self) -> None:
        """
        Close the active PostgreSQL database connection.

        This method attempts to close the existing connection and logs the result. If no active
        connection exists, or if an error occurs during closure, a RuntimeError is raised.

        Raises:
            RuntimeError: If there is no active connection to close or if closing the connection fails.
        """
        if self.connection:
            try:
                self.connection.close()
                logger.info("PostgreSQL connection closed successfully.")
            except psycopg2.Error as e:
                logger.error(f"Failed to close PostgreSQL connection: {e}")
                raise RuntimeError(f"Failed to close PostgreSQL connection: {e}")
        else:
            raise RuntimeError("No active PostgreSQL connection to close.")

def get_postgresql_options(database: str, table: str, env="dev") -> dict[str, str]:
    """
    Construct connection options for interacting with PostgreSQL via PySpark.

    This function retrieves the configuration for the specified environment and assembles a
    dictionary containing the connection details required by PySpark, including the JDBC URL,
    driver, user, password, and target table.

    Args:
        database (str): The name of the database.
        table (str): The name of the table.
        env (str): The environment name (e.g. 'dev', 'prod', 'test'). Defaults to 'dev'.

    Returns:
        dict[str, str]: A dictionary of PostgreSQL connection options for use with PySpark.

    Raises:
        ValueError: If the JDBC URL is missing from the configuration.
    """
    config = load_config(env)
    db_config = config.get("database", {})

    jdbc_url = db_config.get("jdbc_url")
    if not jdbc_url:
        raise ValueError("JDBC URL is missing from configuration.")

    user = db_config.get("user", "postgres")
    password = db_config.get("password")
    if not password:
        logger.warning("PostgreSQL password is missing in configuration.")

    return {
        "url": f"{jdbc_url}{database}",
        "driver": db_config.get("driver", "org.postgresql.Driver"),
        "user": user,
        "password": password,
        "dbtable": table
    }