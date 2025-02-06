import unittest
import os
from unittest.mock import patch
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from src.jobs.wind_turbines.ingestion_layer import ingestion_layer_transform
from src.utils.ingestion_layer_operations import cast_ingestion_schema, get_all_csv_files


class TestIngestionLayer(unittest.TestCase):
    """
    Unit tests for the Ingestion Layer ETL process.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the Spark session before running tests.
        """
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("TestIngestionLayer") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.heartbeatInterval", "100s") \
            .config("spark.network.timeout", "300s") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        # Define the expected schema for the ingestion Layer
        cls.ingestion_schema = {
            "turbine_id": "INTEGER",
            "timestamp": "TIMESTAMP WITHOUT TIME ZONE",
            "power_output": "DOUBLE PRECISION",
            "metadata_datetime_created": "TIMESTAMP"
        }

    @classmethod
    def tearDownClass(cls):
        """
        Stop the Spark session after tests are complete.
        """
        cls.spark.stop()

    def create_test_dataframe(self):
        """
        Create a test DataFrame representing raw input data.
        """
        test_data = [
            Row(turbine_id=1, timestamp="2024-01-01 00:00:00", power_output=100.0),
            Row(turbine_id=2, timestamp="2024-01-01 01:00:00", power_output=120.5),
            Row(turbine_id=3, timestamp="2024-01-01 02:00:00", power_output=140.3),
            Row(turbine_id=4, timestamp="2024-01-01 03:00:00", power_output=160.8),
        ]
        return self.spark.createDataFrame(test_data)

    def test_ingestion_layer_transform(self):
        """
        Test that ingestion_layer_transform correctly applies transformations.
        """
        try:
            # Create test dataset
            df = self.create_test_dataframe()

            # Apply ingestion Layer transformation
            transformed_df = ingestion_layer_transform(df, self.ingestion_schema)

            # Validate schema after transformation
            for col_name in self.ingestion_schema.keys():
                self.assertIn(col_name, transformed_df.columns, f"Column {col_name} is missing in transformed DataFrame")

            # Ensure metadata column is added
            self.assertIn("metadata_datetime_created", transformed_df.columns, "Metadata column is missing")

            # Print transformed DataFrame
            print("\n🚀 Transformed Ingestion Layer DataFrame:")
            transformed_df.show(truncate=False)

        except Exception as e:
            self.fail(f"Test failed due to PySpark error: {e}")

    def test_cast_ingestion_schema(self):
        """
        Test column casting for Ingestion Layer schema.
        """
        df = self.create_test_dataframe()

        # Apply Ingestion Layer transformation first to generate expected columns
        transformed_df = ingestion_layer_transform(df, self.ingestion_schema)

        # Apply schema casting
        casted_df = cast_ingestion_schema(transformed_df, self.ingestion_schema, skip_columns=["metadata_datetime_created"])

        # Ensure correct data types
        expected_types = {
            "turbine_id": "int",
            "timestamp": "timestamp",
            "power_output": "double",
        }

        for column, expected_type in expected_types.items():
            actual_type = dict(casted_df.dtypes)[column]
            self.assertEqual(actual_type, expected_type, f"Column {column} should be {expected_type}")

    def test_cast_ingestion_schema_missing_columns(self):
        """
        Test that missing columns in schema raise an error.
        """
        df = self.create_test_dataframe().drop("power_output")  # Remove column to simulate missing data

        with self.assertRaises(ValueError):
            cast_ingestion_schema(df, self.ingestion_schema)

    @patch("src.utils.ingestion_layer_operations.glob")
    def test_get_all_csv_files(self, mock_glob):
        """
        Test get_all_csv_files function to ensure it retrieves multiple CSV files correctly.
        """
        # Simulate multiple CSV files in directory
        mock_glob.return_value = [
            "/path/to/data/file1.csv",
            "/path/to/data/file2.csv",
            "/path/to/data/file3.csv"
        ]

        files = get_all_csv_files("/path/to/data")

        # Ensure all files are converted to Spark-compatible format
        expected_files = [
            "file:///path/to/data/file1.csv",
            "file:///path/to/data/file2.csv",
            "file:///path/to/data/file3.csv"
        ]

        self.assertEqual(files, expected_files, "get_all_csv_files did not return expected file paths.")

    @patch("src.utils.ingestion_layer_operations.glob")
    def test_get_all_csv_files_no_files(self, mock_glob):
        """
        Test get_all_csv_files function when no files are found.
        """
        mock_glob.return_value = []  # Simulate no files

        with self.assertRaises(FileNotFoundError):
            get_all_csv_files("/path/to/empty_directory")


if __name__ == "__main__":
    unittest.main()
