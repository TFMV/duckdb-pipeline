import os
import io
import requests
import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, BinaryIO, Callable, Any, Union
import configparser
import logging
from abc import ABC, abstractmethod

# Configure logging at module level
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class S3Credentials:
    """Data class to store S3 credentials."""

    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: Optional[str] = None
    endpoint_url: Optional[str] = None


class ConfigProvider(ABC):
    """Abstract base class for configuration providers."""

    @abstractmethod
    def get_config(self) -> configparser.ConfigParser:
        """Get the configuration."""
        pass


class FileConfigProvider(ConfigProvider):
    """Configuration provider that loads from a file."""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the file config provider.

        Args:
            config_path: Path to the config file. If None, uses default path.
        """
        self.config_path = config_path or os.path.join(
            os.path.dirname(__file__), "config.ini"
        )

    def get_config(self) -> configparser.ConfigParser:
        """
        Load configuration from the file.

        Returns:
            ConfigParser object with loaded configuration.

        Raises:
            FileNotFoundError: If the config file doesn't exist.
        """
        config = configparser.ConfigParser()
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found at {self.config_path}")
        config.read(self.config_path)
        return config


class DataCollector(ABC):
    """Abstract base class for data collectors."""

    @abstractmethod
    def collect(self, source_url: str) -> BinaryIO:
        """
        Collect data from the source URL.

        Args:
            source_url: URL to collect data from.

        Returns:
            Binary IO object containing the collected data.

        Raises:
            Exception: If data collection fails.
        """
        pass


class HttpDataCollector(DataCollector):
    """Data collector that uses HTTP requests."""

    def collect(self, source_url: str) -> BinaryIO:
        """
        Download data from the given URL.

        Args:
            source_url: URL to download data from.

        Returns:
            Binary IO object containing the downloaded data.

        Raises:
            requests.HTTPError: If the HTTP request fails.
        """
        logger.info(f"Downloading data from: {source_url}")
        response = requests.get(source_url)
        if response.status_code == 200:
            return io.BytesIO(response.content)
        else:
            logger.error(
                f"Failed to download file from {source_url}. Status code: {response.status_code}"
            )
            response.raise_for_status()


class DataStorage(ABC):
    """Abstract base class for data storage."""

    @abstractmethod
    def store(self, data: BinaryIO, destination: str) -> None:
        """
        Store data at the specified destination.

        Args:
            data: Binary IO object containing the data to store.
            destination: Destination to store the data at.

        Raises:
            Exception: If data storage fails.
        """
        pass


class S3DataStorage(DataStorage):
    """Data storage that uses S3."""

    def __init__(
        self,
        credentials: S3Credentials,
        bucket: str,
        progress_callback: Optional[Callable[[int], None]] = None,
    ):
        """
        Initialize the S3 data storage.

        Args:
            credentials: S3 credentials.
            bucket: S3 bucket name.
            progress_callback: Optional callback function to track upload progress.
        """
        self.credentials = credentials
        self.bucket = bucket
        self.progress_callback = progress_callback
        self.s3_client = self._create_s3_client()

    def _create_s3_client(self):
        """
        Create an S3 client using the provided credentials.

        Returns:
            boto3 S3 client.
        """
        credentials_dict = {
            "aws_access_key_id": self.credentials.aws_access_key_id,
            "aws_secret_access_key": self.credentials.aws_secret_access_key,
        }

        if self.credentials.region_name:
            credentials_dict["region_name"] = self.credentials.region_name

        if self.credentials.endpoint_url:
            credentials_dict["endpoint_url"] = self.credentials.endpoint_url

        return boto3.client("s3", **credentials_dict)

    def store(self, data: BinaryIO, key: str) -> None:
        """
        Upload data to S3.

        Args:
            data: Binary IO object containing the data to upload.
            key: S3 object key.

        Raises:
            S3UploadFailedError: If the S3 upload fails.
        """
        try:
            extra_args = {}
            if self.progress_callback:
                extra_args["Callback"] = self.progress_callback

            self.s3_client.upload_fileobj(data, self.bucket, key, **extra_args)
            logger.info(f"Successfully uploaded {key} to {self.bucket}")
        except S3UploadFailedError as e:
            logger.error(f"Failed to upload {key} to {self.bucket}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred uploading to S3: {e}")
            raise


class DataLakeIngester:
    """
    Class for ingesting data into a data lake.

    This class follows the SOLID principles:
    - Single Responsibility: Each component has a single responsibility
    - Open/Closed: The class is open for extension but closed for modification
    - Liskov Substitution: Subclasses can be used in place of their parent classes
    - Interface Segregation: Clients only depend on the interfaces they use
    - Dependency Inversion: High-level modules depend on abstractions
    """

    def __init__(
        self,
        dataset_base_path: str,
        config_provider: ConfigProvider = None,
        data_collector: DataCollector = None,
        data_storage: DataStorage = None,
    ):
        """
        Initialize the DataLakeIngester.

        Args:
            dataset_base_path: The key prefix to use for this dataset.
            config_provider: Provider for configuration. If None, uses FileConfigProvider.
            data_collector: Collector for data. If None, uses HttpDataCollector.
            data_storage: Storage for data. If None, creates S3DataStorage from config.
        """
        self.dataset_base_path = dataset_base_path
        self.config_provider = config_provider or FileConfigProvider()
        self.config = self.config_provider.get_config()

        self.data_collector = data_collector or HttpDataCollector()

        if data_storage is None:
            s3_credentials = self._get_s3_credentials()
            bronze_bucket = self._bronze_bucket_name()
            data_storage = S3DataStorage(
                credentials=s3_credentials,
                bucket=bronze_bucket,
                progress_callback=self._s3_progress_callback,
            )

        self.data_storage = data_storage

    def ingest_hourly_gharchive(self, process_date: datetime) -> None:
        """
        Ingest hourly data from GHArchive and upload to S3.

        Args:
            process_date: Date and time to process.

        Raises:
            Exception: If ingestion fails.
        """
        try:
            # The format of the Hourly json dump files is YYYY-MM-DD-H.json.gz
            # with Hour part without leading zero when single digit
            date_hour = datetime.strftime(process_date, "%Y-%m-%d-%-H")
            data_filename = f"{date_hour}.json.gz"
            data_url = f"http://data.gharchive.org/{data_filename}"

            s3_key = self._generate_sink_key(
                process_date=process_date,
                filename=data_filename,
                sink_base_path=self.dataset_base_path,
            )

            # Collect and store data
            data = self.data_collector.collect(data_url)
            self.data_storage.store(data, s3_key)

            logger.info(f"Successfully ingested data for {date_hour}")
        except Exception as e:
            logger.error(f"Failed to ingest data for {process_date}: {e}")
            raise

    def _get_s3_credentials(self) -> S3Credentials:
        """
        Retrieve S3 credentials from the configuration.

        Returns:
            S3Credentials object.

        Raises:
            KeyError: If required credentials are missing.
        """
        try:
            credentials = S3Credentials(
                aws_access_key_id=self.config.get("aws", "s3_access_key_id"),
                aws_secret_access_key=self.config.get("aws", "s3_secret_access_key"),
            )

            # Check if optional parameters are present
            if self.config.has_option("aws", "s3_region_name"):
                credentials.region_name = self.config.get("aws", "s3_region_name")

            if self.config.has_option("aws", "s3_endpoint_url"):
                credentials.endpoint_url = self.config.get("aws", "s3_endpoint_url")

            return credentials
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            logger.error(f"Missing S3 credentials in config: {e}")
            raise KeyError(f"Missing S3 credentials in config: {e}")

    def _bronze_bucket_name(self) -> str:
        """
        Get S3 bronze bucket name from the config file.

        Returns:
            S3 bronze bucket name.

        Raises:
            KeyError: If the bucket name is missing from config.
        """
        try:
            return self.config.get("datalake", "bronze_bucket")
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            logger.error(f"Missing bronze bucket name in config: {e}")
            raise KeyError(f"Missing bronze bucket name in config: {e}")

    def _generate_sink_key(
        self, process_date: datetime, filename: str, sink_base_path: str
    ) -> str:
        """
        Generate the S3 sink key for the sink file.

        Args:
            process_date: The process date for the current batch.
            filename: Source filename.
            sink_base_path: Key path within the S3 bucket.

        Returns:
            The key portion for the S3 sink path.
        """
        date_partition = datetime.strftime(process_date, "%Y-%m-%d")
        hour_partition = datetime.strftime(process_date, "%H")
        return f"{sink_base_path}/{date_partition}/{hour_partition}/{filename}"

    def _s3_progress_callback(self, bytes_transferred: int) -> None:
        """
        Callback function to print progress of S3 upload.

        Args:
            bytes_transferred: Number of bytes transferred.
        """
        logger.info(f"Transferred: {bytes_transferred} bytes to S3 bucket")
