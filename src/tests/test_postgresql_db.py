import unittest
from unittest.mock import patch, MagicMock
import psycopg2
from src.utils.postgresql_db import PostgreSQLManager, get_postgresql_options

class TestPostgreSQLManager(unittest.TestCase):
    def setUp(self):
        # Patch load_config so that PostgreSQLManager uses a test configuration
        self.load_config_patcher = patch("src.utils.postgresql_db.load_config")
        self.mock_load_config = self.load_config_patcher.start()
        self.mock_load_config.return_value = {
            "database": {
                "jdbc_url": "jdbc:postgresql://localhost:5432/postgres",
                "user": "test_user",
                "password": "test_pass"
            }
        }
        # Patch psycopg2.connect for the entire test
        self.psycopg2_connect_patcher = patch("src.utils.postgresql_db.psycopg2.connect")
        self.mock_connect = self.psycopg2_connect_patcher.start()
        self.mock_conn = MagicMock()
        self.mock_connect.return_value = self.mock_conn
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

        self.pg_manager = PostgreSQLManager(env="test")

    def tearDown(self):
        self.load_config_patcher.stop()
        self.psycopg2_connect_patcher.stop()

    def test_connect_success(self):
        """Test successful connection to PostgreSQL."""
        self.pg_manager.connect()
        self.mock_connect.assert_called_once()
        self.mock_conn.cursor.assert_called_once()

    def test_connect_failure(self):
        """Test failed connection to PostgreSQL."""
        self.mock_connect.side_effect = psycopg2.OperationalError("Connection failed")
        with self.assertRaises(psycopg2.OperationalError):
            self.pg_manager.connect()

    def test_create_database_success(self):
        """Test creating a database successfully."""
        # Simulate that the database does not exist
        self.mock_cursor.fetchone.return_value = None
        self.pg_manager.create_database("test_db")

        # Check that a CREATE DATABASE SQL statement was executed by examining the call arguments
        calls = self.mock_cursor.execute.call_args_list
        create_call_found = any(
            "CREATE DATABASE" in str(call[0][0]) and "test_db" in str(call[0][0])
            for call in calls
        )
        self.assertTrue(create_call_found, "CREATE DATABASE call not found.")

    def test_create_database_already_exists(self):
        """Test behaviour when trying to create an existing database."""
        # Simulate that the database exists by returning a non-None value
        self.mock_cursor.fetchone.return_value = [1]
        self.pg_manager.create_database("test_db")

        calls = self.mock_cursor.execute.call_args_list
        select_call_found = any(
            "SELECT 1 FROM pg_database WHERE datname = %s" in str(call[0][0]) and call[0][1] == ["test_db"]
            for call in calls
        )
        self.assertTrue(select_call_found, "SELECT query call not found.")

    def test_create_database_permission_denied(self):
        """Test handling of permission error during database creation."""
        # Simulate a permission denied error when executing SQL
        self.mock_cursor.execute.side_effect = psycopg2.Error("Permission denied")
        with self.assertRaises(RuntimeError) as context:
            self.pg_manager.create_database("test_db")
        self.assertIn("Permission denied", str(context.exception))

    def test_create_tables_success(self):
        """Test successful table creation."""
        schema = {
            "test_table": {
                "id": "SERIAL PRIMARY KEY",
                "name": "VARCHAR(255)",
                "created_at": "TIMESTAMP"
            }
        }
        constraints = {"test_table": ["PRIMARY KEY (id)"]}
        self.pg_manager.create_tables("test_db", schema, constraints)

        calls = self.mock_cursor.execute.call_args_list
        table_call_found = any(
            "CREATE TABLE IF NOT EXISTS" in str(call[0][0]) and "test_table" in str(call[0][0])
            for call in calls
        )
        self.assertTrue(table_call_found, "CREATE TABLE call not found.")

    def test_create_tables_failure(self):
        """Test table creation failure handling."""
        schema = {
            "test_table": {
                "id": "SERIAL PRIMARY KEY",
                "name": "VARCHAR(255)"
            }
        }
        # Simulate failure in table creation by making the cursor.execute raise an error
        self.mock_cursor.execute.side_effect = psycopg2.Error("Table creation failed")
        with self.assertRaises(RuntimeError) as context:
            self.pg_manager.create_tables("test_db", schema)
        self.assertIn("Failed to create PostgreSQL tables", str(context.exception))

    def test_close_connection_success(self):
        """Test successful closing of the PostgreSQL connection."""
        self.pg_manager.connect()
        self.pg_manager.close_connection()
        self.mock_conn.close.assert_called_once()

    def test_close_connection_failure(self):
        """Test handling of failure when closing the PostgreSQL connection."""
        self.pg_manager.connect()
        self.mock_conn.close.side_effect = psycopg2.Error("Close failed")
        with self.assertRaises(RuntimeError) as context:
            self.pg_manager.close_connection()
        self.assertIn("Failed to close PostgreSQL connection", str(context.exception))


class TestGetPostgreSQLOptions(unittest.TestCase):
    @patch("src.utils.postgresql_db.load_config")
    def test_get_postgresql_options_success(self, mock_load_config):
        """Test retrieving PostgreSQL options for PySpark successfully."""
        mock_load_config.return_value = {
            "database": {
                "jdbc_url": "jdbc:postgresql://localhost:5432/",
                "driver": "org.postgresql.Driver",
                "user": "test_user",
                "password": "test_pass"
            }
        }
        options = get_postgresql_options("test_db", "test_table", env="test")
        expected_options = {
            "url": "jdbc:postgresql://localhost:5432/test_db",
            "driver": "org.postgresql.Driver",
            "user": "test_user",
            "password": "test_pass",
            "dbtable": "test_table"
        }
        self.assertEqual(options, expected_options)

    @patch("src.utils.postgresql_db.load_config")
    def test_get_postgresql_options_missing_jdbc_url(self, mock_load_config):
        """Test handling missing JDBC URL in PostgreSQL options."""
        mock_load_config.return_value = {"database": {}}
        with self.assertRaises(ValueError) as context:
            get_postgresql_options("test_db", "test_table", env="test")
        self.assertIn("JDBC URL is missing from configuration.", str(context.exception))

    @patch("src.utils.postgresql_db.load_config")
    def test_get_postgresql_options_missing_password(self, mock_load_config):
        """Test handling missing password in PostgreSQL options."""
        mock_load_config.return_value = {
            "database": {
                "jdbc_url": "jdbc:postgresql://localhost:5432/",
                "driver": "org.postgresql.Driver",
                "user": "test_user"
            }
        }
        with self.assertLogs("src.utils.postgresql_db", level="WARNING") as log:
            options = get_postgresql_options("test_db", "test_table", env="test")
        self.assertIn("PostgreSQL password is missing in configuration.", log.output[0])


if __name__ == "__main__":
    unittest.main()
