import unittest
from unittest.mock import patch, MagicMock
from src.utils.initialize_database import initialize_database
from src.utils.postgresql_db import PostgreSQLManager
from src.config.config_loader import load_config


class TestInitializeDatabase(unittest.TestCase):

    @patch("src.utils.initialize_database.PostgreSQLManager")
    @patch("src.utils.initialize_database.load_config")
    def test_initialize_database(self, mock_load_config, mock_postgresql_manager):
        """
        Test the initialize_database function to ensure it creates the database and tables as expected.
        """
        # Mock configuration
        mock_config = {
            "pgsql_database": "test_db",
            "schemas": {
                "table_1": {
                    "id": "INTEGER",
                    "name": "TEXT",
                    "created_at": "TIMESTAMP"
                },
                "table_2": {
                    "id": "INTEGER",
                    "value": "DOUBLE",
                    "updated_at": "TIMESTAMP"
                }
            }
        }
        mock_load_config.return_value = mock_config

        # Mock PostgreSQLManager methods
        mock_pg_manager_instance = MagicMock()
        mock_postgresql_manager.return_value = mock_pg_manager_instance

        # Call the function under test
        initialize_database(env="test")

        # Assertions for configuration loading
        mock_load_config.assert_called_once_with("test")

        # Assertions for database creation
        mock_postgresql_manager.assert_called_once_with("test")
        mock_pg_manager_instance.create_database.assert_called_once_with("test_db")

        # Assertions for table creation
        expected_calls = [
            unittest.mock.call("test_db", {"table_1": mock_config["schemas"]["table_1"]}),
            unittest.mock.call("test_db", {"table_2": mock_config["schemas"]["table_2"]}),
        ]
        mock_pg_manager_instance.create_tables.assert_has_calls(expected_calls, any_order=True)

        # Ensure the connection was closed
        mock_pg_manager_instance.close_connection.assert_called_once()


if __name__ == "__main__":
    unittest.main()
