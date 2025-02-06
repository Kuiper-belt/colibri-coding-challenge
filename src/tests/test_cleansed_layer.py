import unittest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from src.jobs.wind_turbines.cleansed_layer import cleansed_layer_transform
from src.utils.cleansed_layer_operations import cast_cleansed_schema, get_conditions, log_validation_statistics


class TestCleansedLayer(unittest.TestCase):
    """
    Unit tests for the Cleansed Layer ETL process.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the Spark session before running tests.
        """
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("TestCleansedLayer") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.heartbeatInterval", "100s") \
            .config("spark.network.timeout", "300s") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        # Define the expected schema for the Cleansed Layer
        cls.cleansed_schema = {
            "turbine_id": "INTEGER",
            "timestamp": "TIMESTAMP WITHOUT TIME ZONE",
            "wind_speed": "DOUBLE PRECISION",
            "wind_direction": "DOUBLE PRECISION",
            "power_output": "DOUBLE PRECISION"
        }

        # Define default filtering conditions
        cls.default_conditions = [
            "wind_speed BETWEEN 0 AND 25",
            "wind_direction BETWEEN 0 AND 360",
            "power_output >= 0"
        ]

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
            Row(turbine_id=1, timestamp="2024-01-01 00:00:00", wind_speed=10.5, wind_direction=180.0, power_output=100.0),
            Row(turbine_id=2, timestamp="2024-01-01 01:00:00", wind_speed=15.2, wind_direction=90.0, power_output=120.5),
            Row(turbine_id=3, timestamp="2024-01-01 02:00:00", wind_speed=30.0, wind_direction=45.0, power_output=-10.0),  # Invalid wind_speed and power_output
            Row(turbine_id=4, timestamp="2024-01-01 03:00:00", wind_speed=5.8, wind_direction=400.0, power_output=140.3),  # Invalid wind_direction
            Row(turbine_id=5, timestamp="2024-01-01 04:00:00", wind_speed=None, wind_direction=200.0, power_output=160.8),  # Null wind_speed
        ]
        return self.spark.createDataFrame(test_data)

    def test_cleansed_layer_transform(self):
        """
        Test that cleansed_layer_transform correctly applies transformations and validation.
        """
        try:
            # Create test dataset
            df = self.create_test_dataframe()

            # Apply Cleansed Layer transformation
            transformed_df = cleansed_layer_transform(df, self.cleansed_schema)

            # Validate schema after transformation
            for col_name in self.cleansed_schema.keys():
                self.assertIn(col_name, transformed_df.columns, f"Column {col_name} is missing in transformed DataFrame")

            # Ensure invalid rows are removed
            total_rows = df.count()
            valid_rows = transformed_df.count()
            self.assertLess(valid_rows, total_rows, "Invalid rows should be removed")

            # Ensure duplicates are removed
            transformed_df = transformed_df.dropDuplicates()
            self.assertEqual(transformed_df.count(), valid_rows, "Duplicate rows should be removed")

            # Print transformed DataFrame
            print("\n🚀 Transformed Cleansed Layer DataFrame:")
            transformed_df.show(truncate=False)

        except Exception as e:
            self.fail(f"Test failed due to PySpark error: {e}")

    def test_cast_cleansed_schema(self):
        """
        Test column casting for Cleansed Layer schema.
        """
        df = self.create_test_dataframe()

        # Apply Cleansed Layer transformation first to generate expected columns
        transformed_df = cleansed_layer_transform(df, self.cleansed_schema)

        # Apply schema casting
        casted_df = cast_cleansed_schema(transformed_df, self.cleansed_schema)

        # Ensure correct data types
        expected_types = {
            "turbine_id": "int",
            "timestamp": "timestamp",
            "wind_speed": "double",
            "wind_direction": "double",
            "power_output": "double",
        }

        for column, expected_type in expected_types.items():
            actual_type = dict(casted_df.dtypes)[column]
            self.assertEqual(actual_type, expected_type, f"Column {column} should be {expected_type}")

    def test_cast_cleansed_schema_missing_columns(self):
        """
        Test that missing columns in schema raise an error.
        """
        df = self.create_test_dataframe().drop("power_output")  # Remove column to simulate missing data

        with self.assertRaises(ValueError):
            cast_cleansed_schema(df, self.cleansed_schema)

    def test_get_conditions(self):
        """
        Test that get_conditions correctly generates filtering expressions.
        """
        df = self.create_test_dataframe()

        # Generate filtering conditions
        combined_condition = get_conditions(
            conditions=self.default_conditions,
            df=df,
            dynamic_conditions=["turbine_id > 0"]
        )

        # Apply filtering
        filtered_df = df.filter(combined_condition)

        # Validate that filtering reduces row count
        self.assertLess(filtered_df.count(), df.count(), "Invalid rows should be filtered out")

    def test_log_validation_statistics(self):
        """
        Test that log_validation_statistics correctly logs removed rows.
        """
        df = self.create_test_dataframe()

        # Capture original row count
        original_count = df.count()

        # Apply filtering
        transformed_df = cleansed_layer_transform(df, self.cleansed_schema)

        # Log statistics
        log_validation_statistics(transformed_df, original_count, "Filter invalid rows based on conditions")

        # Validate that rows were removed
        self.assertLess(transformed_df.count(), original_count, "Invalid rows should be removed")


if __name__ == "__main__":
    unittest.main()
