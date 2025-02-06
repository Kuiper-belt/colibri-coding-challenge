import unittest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from src.jobs.wind_turbines.curated_layer import curated_layer_transform
from src.utils.curated_layer_operations import cast_curated_schema


class TestCuratedLayer(unittest.TestCase):
    """
    Unit tests for the curated layer transformation process.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the Spark session before running tests.
        """
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("TestCuratedLayer") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.heartbeatInterval", "100s") \
            .config("spark.network.timeout", "300s") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        # Define the expected schema for the curated layer
        cls.curated_schema = {
            "turbine_id": "INTEGER",
            "date": "DATE",
            "mean_power_output": "DOUBLE PRECISION",
            "min_power_output": "DOUBLE PRECISION",
            "max_power_output": "DOUBLE PRECISION",
            "anomalous_power_output": "BOOLEAN"
        }

    @classmethod
    def tearDownClass(cls):
        """
        Stop the Spark session after tests are complete.
        """
        cls.spark.stop()

    def create_test_dataframe(self):
        """
        Create a test DataFrame with normal and extreme values for anomaly detection.
        """
        test_data = [
            Row(turbine_id=1, timestamp="2024-01-01 00:00:00", power_output=100),
            Row(turbine_id=1, timestamp="2024-01-01 01:00:00", power_output=110),
            Row(turbine_id=1, timestamp="2024-01-01 02:00:00", power_output=120),
            Row(turbine_id=1, timestamp="2024-01-01 03:00:00", power_output=130),

            # Extreme values to trigger anomaly detection
            Row(turbine_id=1, timestamp="2024-01-01 04:00:00", power_output=5000),  # Very high
            Row(turbine_id=1, timestamp="2024-01-01 05:00:00", power_output=5),  # Very low
        ]

        return self.spark.createDataFrame(test_data)

    def test_curated_layer_transform(self):
        """
        Test that curated_layer_transform correctly identifies anomalies in the dataset.
        """
        try:
            # Create test dataset with extreme power output values
            df = self.create_test_dataframe()

            # Apply curated layer transformation
            transformed_df = curated_layer_transform(df, self.curated_schema)

            # Validate schema after transformation
            for col_name in self.curated_schema.keys():
                self.assertIn(col_name, transformed_df.columns,
                              f"Column {col_name} is missing in transformed DataFrame")

            # Print DataFrame to debug
            print("\n🚀 Transformed DataFrame:")
            transformed_df.show(truncate=False)

            # Count the number of flagged anomalies
            anomalies = transformed_df.filter(col("anomalous_power_output") == True).count()

            # Ensure at least one anomaly is detected
            self.assertGreater(anomalies, 0, "Anomalous turbines should be flagged")

        except Exception as e:
            self.fail(f"Test failed due to PySpark error: {e}")

    def test_cast_curated_schema(self):
        """
        Test column casting for curated schema.
        """
        df = self.create_test_dataframe()

        # Apply curated layer transformation FIRST to generate expected columns
        transformed_df = curated_layer_transform(df, self.curated_schema)

        # Apply schema casting
        casted_df = cast_curated_schema(transformed_df, self.curated_schema)

        # Ensure correct data types
        expected_types = {
            "turbine_id": "int",
            "date": "date",
            "mean_power_output": "double",
            "min_power_output": "double",
            "max_power_output": "double",
            "anomalous_power_output": "boolean",
        }

        for column, expected_type in expected_types.items():
            actual_type = dict(casted_df.dtypes)[column]
            self.assertEqual(actual_type, expected_type, f"Column {column} should be {expected_type}")

    def test_cast_curated_schema_missing_columns(self):
        """
        Test that missing columns in schema raise an error.
        """
        df = self.create_test_dataframe().drop("power_output")  # Remove column to simulate missing data

        with self.assertRaises(ValueError):
            cast_curated_schema(df, self.curated_schema)


if __name__ == "__main__":
    unittest.main()