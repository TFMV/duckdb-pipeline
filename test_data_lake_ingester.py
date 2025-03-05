#!/usr/bin/env python3
"""
Test script for the DataLakeIngester class.
This demonstrates how to use the refactored code.
"""

import os
import io
from datetime import datetime
from unittest.mock import MagicMock
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
import configparser
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_with_real_dependencies():
    """Test with real dependencies."""
    try:
        # Create a DataLakeIngester with default dependencies
        ingester = DataLakeIngester(dataset_base_path="github-archive")

        # Process data for a specific date and hour
        process_date = datetime(2023, 1, 1, 12)  # January 1, 2023, 12:00
        ingester.ingest_hourly_gharchive(process_date)

        logger.info("Successfully ingested data with real dependencies")
    except Exception as e:
        logger.error(f"Error ingesting data with real dependencies: {e}")


def test_with_mock_dependencies():
    """Test with mock dependencies."""
    # Create mock dependencies
    mock_config = configparser.ConfigParser()
    mock_config["aws"] = {
        "s3_access_key_id": "mock_access_key",
        "s3_secret_access_key": "mock_secret_key",
        "s3_region_name": "us-east-1",
    }
    mock_config["datalake"] = {"bronze_bucket": "mock-bronze-bucket"}

    # Mock config provider
    mock_config_provider = MagicMock(spec=ConfigProvider)
    mock_config_provider.get_config.return_value = mock_config

    # Mock data collector
    mock_data_collector = MagicMock(spec=DataCollector)
    mock_data_collector.collect.return_value = io.BytesIO(b"mock data")

    # Mock data storage
    mock_data_storage = MagicMock(spec=DataStorage)

    # Create DataLakeIngester with mock dependencies
    ingester = DataLakeIngester(
        dataset_base_path="github-archive",
        config_provider=mock_config_provider,
        data_collector=mock_data_collector,
        data_storage=mock_data_storage,
    )

    # Process data for a specific date and hour
    process_date = datetime(2023, 1, 1, 12)  # January 1, 2023, 12:00
    ingester.ingest_hourly_gharchive(process_date)

    # Verify that the mock dependencies were called correctly
    mock_data_collector.collect.assert_called_once()
    mock_data_storage.store.assert_called_once()

    logger.info("Successfully tested with mock dependencies")


def test_custom_implementation():
    """Test with custom implementation of dependencies."""

    # Custom config provider
    class CustomConfigProvider(ConfigProvider):
        def get_config(self):
            config = configparser.ConfigParser()
            config["aws"] = {
                "s3_access_key_id": "custom_access_key",
                "s3_secret_access_key": "custom_secret_key",
                "s3_region_name": "eu-west-1",
            }
            config["datalake"] = {"bronze_bucket": "custom-bronze-bucket"}
            return config

    # Custom data collector
    class CustomDataCollector(DataCollector):
        def collect(self, source_url):
            logger.info(f"Custom collector fetching data from {source_url}")
            return io.BytesIO(b"custom data")

    # Custom data storage
    class CustomDataStorage(DataStorage):
        def store(self, data, destination):
            content = data.read()
            logger.info(f"Custom storage storing {len(content)} bytes to {destination}")

    # Create DataLakeIngester with custom dependencies
    ingester = DataLakeIngester(
        dataset_base_path="github-archive",
        config_provider=CustomConfigProvider(),
        data_collector=CustomDataCollector(),
        data_storage=CustomDataStorage(),
    )

    # Process data for a specific date and hour
    process_date = datetime(2023, 1, 1, 12)  # January 1, 2023, 12:00
    ingester.ingest_hourly_gharchive(process_date)

    logger.info("Successfully tested with custom implementation")


if __name__ == "__main__":
    # Uncomment the test you want to run
    # test_with_real_dependencies()  # Requires real AWS credentials
    test_with_mock_dependencies()
    test_custom_implementation()
