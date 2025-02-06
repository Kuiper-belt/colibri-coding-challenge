import unittest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from src.jobs.wind_turbines.quarantine_layer import quarantine_layer_transform
from src.utils.cleansed_layer_operations import cast_cleansed_schema, get_conditions


class TestQuarantineLayer(unittest.TestCase):
    """
    Unit tests for the Quarantine Layer ETL process.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the Spark session before running tests.
        """
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("TestQuarantineLayer") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.heartbeatInterval", "100s") \
            .config("spark.network.timeout", "300s") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        # Define the expected schema for the Quarantine Layer
        cls.quarantine_schema = {
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

    def test_quarantine_layer_transform(self):
        """
        Test that quarantine_layer_transform correctly isolates invalid records.
        """
        try:
            # Create test dataset
            df = self.create_test_dataframe()

            # Apply Quarantine Layer transformation
            quarantined_df = quarantine_layer_transform(df, self.quarantine_schema)

            # Validate schema after transformation
            for col_name in self.quarantine_schema.keys():
                self.assertIn(col_name, quarantined_df.columns, f"Column {col_name} is missing in quarantined DataFrame")

            # Ensure only invalid rows are included
            invalid_rows = quarantined_df.count()
            self.assertGreater(invalid_rows, 0, "Invalid records should be quarantined")

            # Ensure duplicates are removed
            quarantined_df = quarantined_df.dropDuplicates()
            self.assertEqual(quarantined_df.count(), invalid_rows, "Duplicate rows should be removed")

            # Print quarantined DataFrame
            print("\n🚀 Quarantined DataFrame:")
            quarantined_df.show(truncate=False)

        except Exception as e:
            self.fail(f"Test failed due to PySpark error: {e}")

    def test_cast_quarantine_schema(self):
        """
        Test column casting for Quarantine Layer schema.
        """
        df = self.create_test_dataframe()

        # Apply Quarantine Layer transformation first to generate expected columns
        quarantined_df = quarantine_layer_transform(df, self.quarantine_schema)

        # Apply schema casting
        casted_df = cast_cleansed_schema(quarantined_df, self.quarantine_schema)

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

    def test_get_conditions_for_quarantine(self):
        """
        Test that get_conditions correctly generates filtering expressions for invalid records.
        """
        df = self.create_test_dataframe()

        # Generate filtering conditions
        combined_condition = get_conditions(
            conditions=self.default_conditions,
            df=df,
            dynamic_conditions=["turbine_id > 0"]
        )

        # Apply filtering for quarantine (negated condition)
        quarantined_df = df.filter(~combined_condition)

        # Validate that only invalid rows are present
        self.assertGreater(quarantined_df.count(), 0, "Invalid records should be quarantined")

    def test_quarantine_layer_no_invalid_records(self):
        """
        Test the scenario where all records meet the 'DEFAULT_CLEANSED_CONDITIONS' conditions.
        """
        valid_data = [
            Row(turbine_id=1, timestamp="2024-01-01 00:00:00", wind_speed=10.0, wind_direction=180.0, power_output=100.0),
            Row(turbine_id=2, timestamp="2024-01-01 01:00:00", wind_speed=15.0, wind_direction=90.0, power_output=120.5),
        ]
        valid_df = self.spark.createDataFrame(valid_data)

        quarantined_df = quarantine_layer_transform(valid_df, self.quarantine_schema)

        # Ensure no records are quarantined
        self.assertEqual(quarantined_df.count(), 0, "No records should be quarantined if all data is valid")


if __name__ == "__main__":
    unittest.main()
