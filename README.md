# DuckDB Pipeline

A data pipeline for ingesting data from GHArchive into a data lake using DuckDB.

## Overview

This project provides a framework for ingesting data from external sources (like GHArchive) into a data lake. The code is designed following SOLID principles to be modular, extensible, and testable.

## SOLID Principles Applied

### Single Responsibility Principle (SRP)

Each class has a single responsibility:

- `ConfigProvider`: Responsible for providing configuration
- `DataCollector`: Responsible for collecting data from a source
- `DataStorage`: Responsible for storing data to a destination
- `DataLakeIngester`: Orchestrates the data ingestion process

### Open/Closed Principle (OCP)

The code is open for extension but closed for modification:

- New data collectors can be added without modifying existing code
- New storage mechanisms can be added without modifying existing code
- New configuration providers can be added without modifying existing code

### Liskov Substitution Principle (LSP)

Subclasses can be used in place of their parent classes:

- `HttpDataCollector` can be used anywhere a `DataCollector` is expected
- `S3DataStorage` can be used anywhere a `DataStorage` is expected
- `FileConfigProvider` can be used anywhere a `ConfigProvider` is expected

### Interface Segregation Principle (ISP)

Interfaces are focused and minimal:

- `DataCollector` only has a `collect` method
- `DataStorage` only has a `store` method
- `ConfigProvider` only has a `get_config` method

### Dependency Inversion Principle (DIP)

High-level modules depend on abstractions, not concrete implementations:

- `DataLakeIngester` depends on the abstract `DataCollector`, `DataStorage`, and `ConfigProvider` interfaces
- Concrete implementations are injected at runtime

## Usage

### Basic Usage

```python
from data_lake_ingester import DataLakeIngester
from datetime import datetime

# Create a DataLakeIngester with default dependencies
ingester = DataLakeIngester(dataset_base_path="github-archive")

# Process data for a specific date and hour
process_date = datetime(2023, 1, 1, 12)  # January 1, 2023, 12:00
ingester.ingest_hourly_gharchive(process_date)
```

### Custom Dependencies

```python
from data_lake_ingester import (
    DataLakeIngester,
    ConfigProvider,
    DataCollector,
    DataStorage
)
import configparser
import io

# Custom config provider
class CustomConfigProvider(ConfigProvider):
    def get_config(self):
        config = configparser.ConfigParser()
        config['aws'] = {
            's3_access_key_id': 'custom_access_key',
            's3_secret_access_key': 'custom_secret_key',
            's3_region_name': 'eu-west-1'
        }
        config['datalake'] = {
            'bronze_bucket': 'custom-bronze-bucket'
        }
        return config

# Custom data collector
class CustomDataCollector(DataCollector):
    def collect(self, source_url):
        print(f"Custom collector fetching data from {source_url}")
        return io.BytesIO(b'custom data')

# Custom data storage
class CustomDataStorage(DataStorage):
    def store(self, data, destination):
        content = data.read()
        print(f"Custom storage storing {len(content)} bytes to {destination}")

# Create DataLakeIngester with custom dependencies
ingester = DataLakeIngester(
    dataset_base_path="github-archive",
    config_provider=CustomConfigProvider(),
    data_collector=CustomDataCollector(),
    data_storage=CustomDataStorage()
)

# Process data for a specific date and hour
process_date = datetime(2023, 1, 1, 12)  # January 1, 2023, 12:00
ingester.ingest_hourly_gharchive(process_date)
```

## Configuration

The default configuration is loaded from a `config.ini` file in the same directory as the script. The file should have the following format:

```ini
[aws]
s3_access_key_id = your_access_key
s3_secret_access_key = your_secret_key
s3_region_name = your_region
s3_endpoint_url = your_endpoint_url

[datalake]
bronze_bucket = your_bronze_bucket
```

## Testing

Run the tests with:

```bash
python test_data_lake_ingester.py
```

## License

MIT

### Clone The Repository

```bash
git clone https://github.com/pracdata/duckdb-pipeline.git
```

### Setup Python Virtual Environment

```bash
$ cd duckdb-pipeline
$ python3 -m venv .venv
$ source .venv/bin/activate
# Install required packages
$ pip install -r requirements.txt
```

### Configuration

1. Rename `config.ini.template` to `config.ini`
2. Edit `config.ini` and fill in your actual AWS S3 credential values in the `[aws]` section.
3. If you are using a S3 compatible storage, setup the `s3_endpoint_url` parameter as well. Otherwise remove the line
4. Edit `config.ini` and fill in the bucket names in `[datalake]` section for each zone in your data lake.

### Scheduling

There are Python scripts available in `scripts` directory for each phase (Ingestion, Serilisation, Aggregation) for calling with a scheduler like **Crontab**.
Following shows sample cron statements to run each pipeline script at an appropriate time.
Update the paths to match your setting and also ensure you allow enough time between each pipeline to complete.

```
# schedule the ingestion pipeline script to run 15 minutes past each hour
15 * * * * /path/to/your/venv/bin/python3 /path/to/your/duckdb-pipeline/scripts/run_ingest_source_data.py >> /tmp/ingest_source_data.out 2>&1
# schedule the serialisation pipeline script to run 30 minutes past each hour
30 * * * * /path/to/your/venv/bin/python3 /path/to/your/duckdb-pipeline/scripts/run_serialise_raw_data.py >> /tmp/serialise_raw_data.out 2>&1
# schedule the aggregation pipeline script to run 2 hours past midnight
0 2 * * * /path/to/your/venv/bin/python3 /path/to/your/duckdb-pipeline/scripts/run_agg_silver_data.py >> /tmp/aggregate_silver_data.out 2>&1
```
