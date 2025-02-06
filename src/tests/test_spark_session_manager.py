import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from src.utils.spark_session_manager import SparkSessionManager


class TestSparkSessionManager(unittest.TestCase):
    @patch("src.utils.spark_session_manager.load_config")
    def test_get_spark_session(self, mock_load_config):
        """
        Test that get_spark_session initializes a SparkSession with the correct configuration.
        """
        # Mock configuration
        mock_load_config.return_value = {
            "spark": {
                "app_name": "TestApp",
                "config": {
                    "spark.executor.memory": "2g",
                    "spark.driver.memory": "1g",
                },
            }
        }

        # Call the method
        spark_session = SparkSessionManager.get_spark_session(env="test")

        # Assertions
        self.assertIsInstance(spark_session, SparkSession)
        self.assertEqual(spark_session.sparkContext.appName, "TestApp")
        self.assertEqual(spark_session.conf.get("spark.executor.memory"), "2g")
        self.assertEqual(spark_session.conf.get("spark.driver.memory"), "1g")

        # Cleanup
        SparkSessionManager.stop_spark_session()

    @patch("src.utils.spark_session_manager.SparkSession.stop")
    def test_stop_spark_session(self, mock_spark_stop):
        """
        Test that stop_spark_session stops an active SparkSession and clears the instance.
        """
        # Step 1: Simulate an active SparkSession
        SparkSessionManager._spark_instance = SparkSession.builder.appName("TestApp").getOrCreate()

        # Step 2: Call stop_spark_session
        SparkSessionManager.stop_spark_session()

        # Assert that SparkSession.stop() was called exactly once
        mock_spark_stop.assert_called_once()

        # Assert that the _spark_instance is set to None
        self.assertIsNone(SparkSessionManager._spark_instance)

    def test_stop_spark_session_no_active_session(self):
        """
        Test that stop_spark_session does nothing when no active SparkSession exists.
        """
        # Step 1: Ensure no active SparkSession
        SparkSessionManager._spark_instance = None

        # Patch SparkSession.stop to confirm it isn't called
        with patch("src.utils.spark_session_manager.SparkSession.stop") as mock_spark_stop:
            SparkSessionManager.stop_spark_session()

            # Assert that SparkSession.stop was not called
            mock_spark_stop.assert_not_called()

        # Assert that the _spark_instance is still None
        self.assertIsNone(SparkSessionManager._spark_instance)

    @patch("src.utils.spark_session_manager.load_config")
    def test_get_spark_session_error_handling(self, mock_load_config):
        """
        Test that get_spark_session raises a RuntimeError if initialization fails.
        """
        # Mock configuration to simulate an error
        mock_load_config.side_effect = Exception("Mocked load_config failure")

        with self.assertRaises(RuntimeError) as context:
            SparkSessionManager.get_spark_session(env="test")

        self.assertIn("Failed to create SparkSession", str(context.exception))


if __name__ == "__main__":
    unittest.main()