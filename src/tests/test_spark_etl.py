import sys
import os
# Add the project root (parent directory of src) to the Python path.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

import pyspark
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.utils import AnalysisException

# Import module components using the correct path.
from src.utils.spark_etl import DataSet, Reader, Writer, filter_ingestion_period, etl

# -----------------------------
# Tests for the DataSet class.
# -----------------------------
class TestDataSet(unittest.TestCase):
    def test_init(self):
        ds = DataSet("csv", {"option1": "value1"})
        self.assertEqual(ds.format, "csv")
        self.assertEqual(ds.options, {"option1": "value1"})

# -----------------------------
# Tests for the Reader class.
# -----------------------------
class TestReader(unittest.TestCase):
    def test_get_reader_with_explicit_format(self):
        config = {
            "format": "parquet",
            "options": {"path": "/data/file.parquet"},
            "schema": "dummy_schema"
        }
        reader = Reader.get_reader(config)
        self.assertIsInstance(reader, Reader)
        self.assertEqual(reader.format, "parquet")
        self.assertEqual(reader.options, {"path": "/data/file.parquet"})
        self.assertEqual(reader.schema, "dummy_schema")

    def test_get_reader_infers_format(self):
        config = {
            "options": {"path": "/data/file.csv"}
        }
        reader = Reader.get_reader(config)
        self.assertEqual(reader.format, "csv")
        self.assertEqual(reader.options, {"path": "/data/file.csv"})

    def test_get_reader_jdbc_missing_url(self):
        config = {
            "format": "jdbc",
            "options": {"path": "/data/dummy"}
        }
        with self.assertRaises(ValueError) as context:
            Reader.get_reader(config)
        self.assertIn("Missing 'url'", str(context.exception))

    @patch("src.utils.spark_etl.SparkSessionManager.get_spark_session")
    def test_extract_success(self, mock_get_spark_session):
        dummy_spark = MagicMock()
        mock_get_spark_session.return_value = dummy_spark

        dummy_reader = MagicMock()
        # Simulate the Spark read chain.
        dummy_spark.read.format.return_value = dummy_reader
        dummy_reader.options.return_value = dummy_reader
        dummy_reader.load.return_value = "dummy_dataframe"

        config = {
            "format": "csv",
            "options": {"path": "/data/file.csv"}
        }
        reader = Reader.get_reader(config)
        result = reader.extract()
        self.assertEqual(result, "dummy_dataframe")
        dummy_spark.read.format.assert_called_with("csv")
        dummy_reader.options.assert_called_with(**{"path": "/data/file.csv"})

    @patch("src.utils.spark_etl.SparkSessionManager.get_spark_session", return_value=None)
    def test_extract_no_spark(self, mock_get_spark_session):
        config = {
            "format": "csv",
            "options": {"path": "/data/file.csv"}
        }
        reader = Reader.get_reader(config)
        with self.assertRaises(RuntimeError) as context:
            reader.extract()
        self.assertIn("No active SparkSession", str(context.exception))

# -----------------------------
# Tests for the Writer class.
# -----------------------------
class TestWriter(unittest.TestCase):
    def test_get_writer_success(self):
        config = {
            "format": "parquet",
            "write_options": {"path": "/output/file.parquet"},
            "mode": "overwrite"
        }
        writer = Writer.get_writer(config)
        self.assertIsInstance(writer, Writer)
        self.assertEqual(writer.format, "parquet")
        self.assertEqual(writer.options, {"path": "/output/file.parquet"})
        self.assertEqual(writer.mode, "overwrite")

    def test_get_writer_missing_format(self):
        config = {
            "write_options": {"path": "/output/file.parquet"}
        }
        with self.assertRaises(ValueError) as context:
            Writer.get_writer(config)
        self.assertIn("No 'format' specified", str(context.exception))

    def test_load_success(self):
        writer = Writer("csv", {"path": "/output/file.csv"}, mode="append")
        dummy_df = MagicMock()
        dummy_writer = MagicMock()
        # Setup the DataFrame write chain.
        dummy_df.write.format.return_value = dummy_writer
        dummy_writer.options.return_value = dummy_writer
        dummy_writer.mode.return_value = dummy_writer
        dummy_writer.save.return_value = None

        writer.load(dummy_df)
        dummy_df.write.format.assert_called_with("csv")
        dummy_writer.options.assert_called_with(**{"path": "/output/file.csv"})
        dummy_writer.mode.assert_called_with("append")
        dummy_writer.save.assert_called_once()

    def test_load_failure(self):
        writer = Writer("csv", {"path": "/output/file.csv"}, mode="append")
        dummy_df = MagicMock()
        dummy_writer = MagicMock()
        dummy_df.write.format.return_value = dummy_writer
        dummy_writer.options.return_value = dummy_writer
        dummy_writer.mode.return_value = dummy_writer
        dummy_writer.save.side_effect = Exception("Write failed")
        with self.assertRaises(Exception) as context:
            writer.load(dummy_df)
        self.assertIn("Write failed", str(context.exception))

# ----------------------------------
# Tests for the filter_ingestion_period function.
# ----------------------------------
class TestFilterIngestionPeriod(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestFilter").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_filter_ingestion_period_valid(self):
        data = [
            {"id": 1, "ingestion_date": "2023-01-01"},
            {"id": 2, "ingestion_date": "2023-01-02"},
            {"id": 3, "ingestion_date": "2023-01-03"}
        ]
        df = self.spark.createDataFrame(data)
        filtered_df = filter_ingestion_period(
            df, "ingestion_date", start_date="2023-01-02", end_date="2023-01-03"
        )
        result = filtered_df.collect()
        self.assertEqual(len(result), 2)
        ids = [row.id for row in result]
        self.assertListEqual(ids, [2, 3])

    def test_filter_ingestion_period_invalid_date(self):
        data = [{"id": 1, "ingestion_date": "2023-01-01"}]
        df = self.spark.createDataFrame(data)
        with self.assertRaises(ValueError):
            filter_ingestion_period(df, "ingestion_date", start_date="invalid-date")

# ----------------------------------
# Tests for the etl function.
# ----------------------------------
class TestETL(unittest.TestCase):
    @patch("src.utils.spark_etl.load_config")
    @patch("src.utils.spark_etl.SparkSessionManager.get_spark_session")
    @patch("src.utils.spark_etl.Reader.get_reader")
    @patch("src.utils.spark_etl.Writer.get_writer")
    @patch("src.utils.spark_etl.SparkSessionManager.stop_spark_session")
    def test_etl_success(self, mock_stop, mock_get_writer, mock_get_reader, mock_get_spark_session, mock_load_config):
        dummy_config = {"spark": {"app_name": "TestETL"}}
        mock_load_config.return_value = dummy_config

        dummy_spark = MagicMock()
        mock_get_spark_session.return_value = dummy_spark

        dummy_reader = MagicMock()
        dummy_df = MagicMock(spec=DataFrame)
        dummy_reader.extract.return_value = dummy_df
        mock_get_reader.return_value = dummy_reader

        dummy_writer = MagicMock()
        mock_get_writer.return_value = dummy_writer

        reader_dict = {"format": "csv", "options": {"path": "/data/input.csv"}}
        writer_dict = {"format": "parquet", "write_options": {"path": "/data/output.parquet"}}
        transform_func = lambda df: df

        etl(reader_dict, writer_dict, transform_func=transform_func, env="test")

        dummy_reader.extract.assert_called_once()
        dummy_writer.load.assert_called_once_with(dummy_df)
        mock_stop.assert_called_once()

    @patch("src.utils.spark_etl.load_config")
    @patch("src.utils.spark_etl.SparkSessionManager.get_spark_session")
    @patch("src.utils.spark_etl.Reader.get_reader")
    @patch("src.utils.spark_etl.SparkSessionManager.stop_spark_session")
    def test_etl_exception(self, mock_stop, mock_get_reader, mock_get_spark_session, mock_load_config):
        dummy_config = {"spark": {"app_name": "TestETL"}}
        mock_load_config.return_value = dummy_config

        dummy_spark = MagicMock()
        mock_get_spark_session.return_value = dummy_spark

        dummy_reader = MagicMock()
        dummy_reader.extract.side_effect = Exception("Extraction failed")
        mock_get_reader.return_value = dummy_reader

        reader_dict = {"format": "csv", "options": {"path": "/data/input.csv"}}
        writer_dict = {"format": "parquet", "write_options": {"path": "/data/output.parquet"}}

        with self.assertRaises(Exception) as context:
            etl(reader_dict, writer_dict, env="test")
        self.assertIn("Extraction failed", str(context.exception))
        mock_stop.assert_called_once()

if __name__ == "__main__":
    unittest.main()
