import unittest
import os
import json
import tempfile
from unittest.mock import patch, mock_open
from src.config.config_loader import load_config, _merge_paths_with_base

class TestLoadConfig(unittest.TestCase):
    def setUp(self):
        """Set up temporary test configuration"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.json")
        self.invalid_config_path = os.path.join(self.temp_dir.name, "invalid_config.json")

        # Write an invalid JSON file (malformed)
        with open(self.invalid_config_path, "w") as f:
            f.write('{"data_path": "/base/path", "etl_config": {"raw_data": "invalid_json"')  # Missing closing bracket

        # Sample valid configuration
        self.valid_config = {
            "data_path": "/base/path",
            "etl_config": {
                "raw_data": {
                    "read_options": {"path": "raw_data.csv"}
                },
                "ingestion_layer_config": {
                    "write_options": {"path": "ingestion_data.csv"}
                }
            }
        }

        # Write the valid config to a temp file
        with open(self.config_path, "w") as f:
            json.dump(self.valid_config, f)

    def tearDown(self):
        """Clean up temporary files"""
        self.temp_dir.cleanup()

    @patch("src.config.config_loader.os.getenv", return_value="test")
    @patch("src.config.config_loader.os.path.join")
    def test_load_config_success(self, mock_path_join, mock_getenv):
        """Test successful configuration loading"""
        # Mock the file path to return our test config file
        mock_path_join.return_value = self.config_path

        # Use mock_open to simulate file opening and reading
        with patch("builtins.open", mock_open(read_data=json.dumps(self.valid_config))):
            config = load_config()
            self.assertEqual(config["data_path"], "/base/path")
            self.assertIn("etl_config", config)
            self.assertIn("raw_data", config["etl_config"])

    @patch("src.config.config_loader.os.getenv", return_value="nonexistent")
    @patch("src.config.config_loader.os.path.dirname", return_value=tempfile.gettempdir())
    def test_load_config_file_not_found(self, mock_dirname, mock_getenv):
        """Test handling of a missing configuration file."""
        with self.assertRaises(RuntimeError) as context:
            load_config()
        self.assertIn("Configuration file not found", str(context.exception))

    @patch("src.config.config_loader.os.getenv", return_value="test")
    @patch("src.config.config_loader.os.path.join")
    def test_load_config_invalid_json(self, mock_path_join, mock_getenv):
        """Test error handling for invalid JSON configuration"""
        mock_path_join.return_value = self.invalid_config_path

        with patch("builtins.open",
                   mock_open(read_data='{"data_path": "/base/path", "etl_config": {"raw_data": "invalid_json"')):
            with self.assertRaises(RuntimeError) as context:
                load_config()

        self.assertIn("Invalid JSON format", str(context.exception))

    def test_merge_paths_with_base(self):
        """Test merging of base data path with relative paths in the config."""
        config = {
            "data_path": "/base/path",
            "etl_config": {
                "raw_data": {
                    "read_options": {"path": "raw_data.csv"}
                },
                "ingestion_layer_config": {
                    "write_options": {"path": "ingestion_data.csv"}
                }
            }
        }

        _merge_paths_with_base(config)

        self.assertEqual(config["etl_config"]["raw_data"]["read_options"]["path"], "/base/path/raw_data.csv")
        self.assertEqual(config["etl_config"]["ingestion_layer_config"]["write_options"]["path"], "/base/path/ingestion_data.csv")

    def test_merge_paths_with_base_no_data_path(self):
        """Test merging when 'data_path' is missing in config."""
        config = {
            "etl_config": {
                "raw_data": {"read_options": {"path": "raw_data.csv"}},
                "ingestion_layer_config": {"write_options": {"path": "ingestion_data.csv"}}
            }
        }

        _merge_paths_with_base(config)

        # Paths should remain unchanged
        self.assertEqual(config["etl_config"]["raw_data"]["read_options"]["path"], "raw_data.csv")
        self.assertEqual(config["etl_config"]["ingestion_layer_config"]["write_options"]["path"], "ingestion_data.csv")


if __name__ == "__main__":
    unittest.main()
