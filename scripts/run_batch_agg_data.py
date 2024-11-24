#!/usr/bin/env python3
import sys
import os
import logging
import argparse
from datetime import datetime, timedelta
# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_lake_transformer import DataLakeTransformer

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_date(date_str):
  """Convert date string to datetime object"""
  try:
    return datetime.strptime(date_str, '%Y-%m-%d')
  except ValueError:
    raise argparse.ArgumentTypeError(f"Invalid date format. Please use YYYY-MM-DD")

def main():
  # Set up argument parser
  parser = argparse.ArgumentParser(description='Aggregate Silver data for a date range')
  parser.add_argument('--start-date', 
                    type=parse_date,
                    required=True,
                    help='Start date in YYYY-MM-DD format')
  parser.add_argument('--end-date', 
                    type=parse_date,
                    required=True,
                    help='End date in YYYY-MM-DD format')
  # Parse arguments
  args = parser.parse_args()
  # Validate date range
  if args.end_date < args.start_date:
    parser.error("End date must be greater than or equal to start date")

  try:
    transformer = DataLakeTransformer(dataset_base_path='gharchive/events')
    current_date = args.start_date
    while current_date <= args.end_date:
      logging.info(f"processing for day {current_date.date()}")
      transformer.aggregate_silver_data(current_date)
      logging.info(f"Successfully aggregated bronze data for {current_date}")
      current_date += timedelta(days=1)
  except Exception as e:
    logging.error(f"Error in aggregate_silver_data: {str(e)}")

if __name__ == "__main__":
  main()
