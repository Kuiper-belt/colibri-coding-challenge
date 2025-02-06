import unittest
import os
import json
import tempfile
from unittest.mock import patch
from src.config.config_loader import load_config, _merge_paths_with_base


class TestConfigLoader(unittest.TestCase):
    """
    Unit tests for the config_loader module.
    """

    def setUp(self):
        """Set up temporary config files for testing."""
        self.test_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.test_dir.name, "test_config.json")

        self.valid_config = {
            "etl_config": {
                "raw_data": {
                    "read_options": {
                        "path": "raw_data.csv"
                    }
                },
                "ingestion_layer_config": {
                    "write_options": {
                        "path": "output_data.csv"
                    }
                }
            }
        }

        with open(self.config_path, "w") as file:
            json.dump(self.valid_config, file)

    def tearDown(self):
        """Clean up temporary test files."""
        self.test_dir.cleanup()

    @patch("src.config.config_loader.os.getenv", return_value="test")
    @patch("src.config.config_loader.open", create=True)
    def test_load_config(self, mock_open, mock_getenv):
        """Test loading configuration for a valid environment."""
        # Mock opening the JSON config file to return our test config
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.valid_config)

        config = load_config("test")

        # Compute expected absolute path
        expected_path = os.path.abspath("raw_data.csv")  # Fix: Expect absolute path
        actual_path = config["etl_config"]["raw_data"]["read_options"]["path"]

        self.assertEqual(actual_path, expected_path, f"Expected {expected_path}, but got {actual_path}")

    @patch("src.config.config_loader.os.path.abspath")
    def test_merge_paths_with_base(self, mock_abspath):
        """Test that _merge_paths_with_base correctly converts relative paths to absolute paths."""
        mock_abspath.side_effect = lambda x: f"/mocked/absolute/{x.lstrip('/')}"  # Avoid infinite recursion

        config = {
            "etl_config": {
                "raw_data": {
                    "read_options": {
                        "path": "raw_data.csv"
                    }
                }
            }
        }
        _merge_paths_with_base(config)

        expected_path = "/mocked/absolute/raw_data.csv"
        self.assertEqual(config["etl_config"]["raw_data"]["read_options"]["path"], expected_path)

    @patch("src.config.config_loader.os.path.abspath")
    def test_merge_paths_with_base_no_data_path(self, mock_abspath):
        """Test _merge_paths_with_base when no base path exists."""
        mock_abspath.side_effect = lambda x: f"/mocked/absolute/{x.lstrip('/')}"  # Avoid recursion

        config = {
            "etl_config": {
                "raw_data": {
                    "read_options": {
                        "path": "raw_data.csv"
                    }
                }
            }
        }
        _merge_paths_with_base(config)

        expected_path = "/mocked/absolute/raw_data.csv"
        self.assertEqual(config["etl_config"]["raw_data"]["read_options"]["path"], expected_path)

    def test_load_config_invalid_json(self):
        """Test loading an invalid JSON config file."""
        invalid_config_path = os.path.join(self.test_dir.name, "invalid_config.json")
        with open(invalid_config_path, "w") as file:
            file.write("{invalid json}")  # Writing invalid JSON

        with patch("src.config.config_loader.os.path.join", return_value=invalid_config_path):
            with self.assertRaises(RuntimeError):
                load_config("test")

    def test_load_config_missing_file(self):
        """Test handling of a missing config file."""
        missing_config_path = os.path.join(self.test_dir.name, "nonexistent_config.json")

        with patch("src.config.config_loader.os.path.join", return_value=missing_config_path):
            with self.assertRaises(RuntimeError):
                load_config("test")


if __name__ == "__main__":
    unittest.main()
