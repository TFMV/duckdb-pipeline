#!/usr/bin/env python3
"""
Unit tests for the DataLakeIngester class and its components.
"""

import unittest
from unittest.mock import MagicMock, patch, mock_open
import io
import os
import configparser
from datetime import datetime
from data_lake_ingester import (
    DataLakeIngester,
    ConfigProvider,
    DataCollector,
    DataStorage,
    S3Credentials,
    FileConfigProvider,
    HttpDataCollector,
    S3DataStorage,
)


class TestFileConfigProvider(unittest.TestCase):
    """Test the FileConfigProvider class."""

    @patch("os.path.exists")
    @patch("configparser.ConfigParser.read")
    def test_get_config_file_exists(self, mock_read, mock_exists):
        """Test get_config when file exists."""
        mock_exists.return_value = True
        provider = FileConfigProvider("test_config.ini")
        config = provider.get_config()
        mock_read.assert_called_once_with("test_config.ini")
        self.assertIsInstance(config, configparser.ConfigParser)

    @patch("os.path.exists")
    def test_get_config_file_not_exists(self, mock_exists):
        """Test get_config when file doesn't exist."""
        mock_exists.return_value = False
        provider = FileConfigProvider("test_config.ini")
        with self.assertRaises(FileNotFoundError):
            provider.get_config()


class TestHttpDataCollector(unittest.TestCase):
    """Test the HttpDataCollector class."""

    @patch("requests.get")
    def test_collect_success(self, mock_get):
        """Test collect with successful response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"test data"
        mock_get.return_value = mock_response

        collector = HttpDataCollector()
        result = collector.collect("http://test.com/data")

        mock_get.assert_called_once_with("http://test.com/data")
        self.assertEqual(result.getvalue(), b"test data")

    @patch("requests.get")
    def test_collect_failure(self, mock_get):
        """Test collect with failed response."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = Exception("Not found")
        mock_get.return_value = mock_response

        collector = HttpDataCollector()
        with self.assertRaises(Exception):
            collector.collect("http://test.com/data")


class TestS3DataStorage(unittest.TestCase):
    """Test the S3DataStorage class."""

    def setUp(self):
        """Set up test fixtures."""
        self.credentials = S3Credentials(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region_name="test_region",
            endpoint_url="test_endpoint",
        )
        self.mock_s3_client = MagicMock()
        self.mock_progress_callback = MagicMock()

    @patch("boto3.client")
    def test_create_s3_client(self, mock_boto3_client):
        """Test _create_s3_client method."""
        mock_boto3_client.return_value = self.mock_s3_client
        storage = S3DataStorage(
            credentials=self.credentials,
            bucket="test_bucket",
            progress_callback=self.mock_progress_callback,
        )

        mock_boto3_client.assert_called_once_with(
            "s3",
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region_name="test_region",
            endpoint_url="test_endpoint",
        )
        self.assertEqual(storage.s3_client, self.mock_s3_client)

    @patch("boto3.client")
    def test_store_success(self, mock_boto3_client):
        """Test store method with successful upload."""
        mock_boto3_client.return_value = self.mock_s3_client
        storage = S3DataStorage(
            credentials=self.credentials,
            bucket="test_bucket",
            progress_callback=self.mock_progress_callback,
        )

        test_data = io.BytesIO(b"test data")
        storage.store(test_data, "test_key")

        self.mock_s3_client.upload_fileobj.assert_called_once_with(
            test_data, "test_bucket", "test_key", Callback=self.mock_progress_callback
        )

    @patch("boto3.client")
    def test_store_failure(self, mock_boto3_client):
        """Test store method with failed upload."""
        from boto3.exceptions import S3UploadFailedError

        mock_boto3_client.return_value = self.mock_s3_client
        self.mock_s3_client.upload_fileobj.side_effect = S3UploadFailedError(
            "Upload failed"
        )

        storage = S3DataStorage(
            credentials=self.credentials,
            bucket="test_bucket",
            progress_callback=self.mock_progress_callback,
        )

        test_data = io.BytesIO(b"test data")
        with self.assertRaises(S3UploadFailedError):
            storage.store(test_data, "test_key")


class TestDataLakeIngester(unittest.TestCase):
    """Test the DataLakeIngester class."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock config
        self.mock_config = configparser.ConfigParser()
        self.mock_config["aws"] = {
            "s3_access_key_id": "test_key",
            "s3_secret_access_key": "test_secret",
            "s3_region_name": "test_region",
            "s3_endpoint_url": "test_endpoint",
        }
        self.mock_config["datalake"] = {"bronze_bucket": "test_bucket"}

        # Mock config provider
        self.mock_config_provider = MagicMock(spec=ConfigProvider)
        self.mock_config_provider.get_config.return_value = self.mock_config

        # Mock data collector
        self.mock_data_collector = MagicMock(spec=DataCollector)
        self.mock_data_collector.collect.return_value = io.BytesIO(b"test data")

        # Mock data storage
        self.mock_data_storage = MagicMock(spec=DataStorage)

        # Create DataLakeIngester with mock dependencies
        self.ingester = DataLakeIngester(
            dataset_base_path="test-dataset",
            config_provider=self.mock_config_provider,
            data_collector=self.mock_data_collector,
            data_storage=self.mock_data_storage,
        )

    def test_ingest_hourly_gharchive(self):
        """Test ingest_hourly_gharchive method."""
        process_date = datetime(2023, 1, 1, 12)
        self.ingester.ingest_hourly_gharchive(process_date)

        # Check that data collector was called with correct URL
        expected_url = "http://data.gharchive.org/2023-01-01-12.json.gz"
        self.mock_data_collector.collect.assert_called_once_with(expected_url)

        # Check that data storage was called with correct key
        expected_key = "test-dataset/2023-01-01/12/2023-01-01-12.json.gz"
        self.mock_data_storage.store.assert_called_once()
        args, _ = self.mock_data_storage.store.call_args
        self.assertEqual(args[1], expected_key)

    def test_get_s3_credentials(self):
        """Test _get_s3_credentials method."""
        credentials = self.ingester._get_s3_credentials()
        self.assertIsInstance(credentials, S3Credentials)
        self.assertEqual(credentials.aws_access_key_id, "test_key")
        self.assertEqual(credentials.aws_secret_access_key, "test_secret")
        self.assertEqual(credentials.region_name, "test_region")
        self.assertEqual(credentials.endpoint_url, "test_endpoint")

    def test_bronze_bucket_name(self):
        """Test _bronze_bucket_name method."""
        bucket_name = self.ingester._bronze_bucket_name()
        self.assertEqual(bucket_name, "test_bucket")

    def test_generate_sink_key(self):
        """Test _generate_sink_key method."""
        process_date = datetime(2023, 1, 1, 12)
        filename = "test.json.gz"
        sink_base_path = "test-path"

        key = self.ingester._generate_sink_key(process_date, filename, sink_base_path)
        expected_key = "test-path/2023-01-01/12/test.json.gz"
        self.assertEqual(key, expected_key)


if __name__ == "__main__":
    unittest.main()
