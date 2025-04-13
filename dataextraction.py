# dataextraction.py
import pandas as pd
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
import csv

from utils.api_utils import fetch_api_data


class DatasetExtractor:
    def __init__(self):
        self.output_dir = Path('extracted_datasets')
        self.output_dir.mkdir(exist_ok=True)
        self.error_log = self.output_dir / 'extraction_errors.csv'
        self.setup_logging()

        # Initialize error log if it doesn't exist
        if not self.error_log.exists():
            with open(self.error_log, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'timestamp', 'agency', 'url', 'dataset_name', 'error_type', 'error_message', 'status_code'
                ])
                writer.writeheader()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.output_dir / 'extraction.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

    def sanitize_filename(self, name):
        """Create safe filenames from dataset names."""
        if not name or not isinstance(name, str):
            return "unnamed_dataset"

        # Remove special characters and limit length
        name = re.sub(r'[^\w\-_\. ]', '', name.strip())
        name = re.sub(r'\s+', '_', name)  # Replace spaces with underscores
        return name[:100]  # Limit filename length

    def sanitize_agency_name(self, name):
        """Create safe directory names for agencies."""
        return self.sanitize_filename(name).lower()

    def log_error(self, agency, url, dataset_name, error_type, error_msg, status_code=None):
        """Log errors to CSV file."""
        with open(self.error_log, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'timestamp', 'agency', 'url', 'dataset_name', 'error_type', 'error_message', 'status_code'
            ])
            writer.writerow({
                'timestamp': datetime.now().isoformat(),
                'agency': agency,
                'url': url,
                'dataset_name': dataset_name,
                'error_type': error_type,
                'error_message': error_msg[:500],  # Limit message length
                'status_code': status_code
            })

    def save_dataset(self, data, dataset_name, agency_name):
        """Save dataset using the name from source CSV."""
        try:
            # Create filename from source dataset name
            safe_name = self.sanitize_filename(dataset_name)
            filename = f"{safe_name}.csv"
            agency_dir = self.output_dir / self.sanitize_agency_name(agency_name)
            agency_dir.mkdir(exist_ok=True)
            filepath = agency_dir / filename

            # Convert data to DataFrame
            if isinstance(data, dict):
                df = pd.DataFrame([data])
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                logging.error(f"Unsupported data type: {type(data)}")
                return False

            # Check if DataFrame contains actual data
            if len(df) == 0 or all(col.startswith(('version_', 'api_', 'extraction_')) for col in df.columns):
                logging.warning(f"Skipping empty dataset: {dataset_name}")
                return False

            # Save to CSV
            df.to_csv(
                filepath,
                index=False,
                encoding='utf-8-sig',
                quoting=csv.QUOTE_NONNUMERIC
            )
            logging.info(f"Saved dataset to {filepath}")
            return True

        except Exception as e:
            logging.error(f"Failed to save dataset {dataset_name}: {str(e)}")
            return False

    def process_agency_data(self, input_csv, agency_name):
        """Process all datasets using names from source CSV."""
        if not Path(input_csv).exists():
            logging.error(f"Input file not found: {input_csv}")
            return False

        try:
            df = pd.read_csv(input_csv)

            # Check required columns
            required_columns = {'Data Link', 'Version Name'}
            if not required_columns.issubset(df.columns):
                missing = required_columns - set(df.columns)
                logging.error(f"CSV file missing required columns: {missing}")
                return False

            stats = {
                'total': 0,
                'success': 0,
                'invalid_url': 0,
                'api_error': 0,
                'empty_response': 0,
                'save_failed': 0
            }

            with tqdm(df.iterrows(), total=len(df), desc=f"Processing {agency_name}") as pbar:
                for _, row in pbar:
                    stats['total'] += 1
                    url = row['Data Link']
                    dataset_name = row['Version Name']

                    # Skip invalid URLs
                    if pd.isna(url) or not str(url).startswith('http'):
                        stats['invalid_url'] += 1
                        self.log_error(
                            agency_name, str(url), dataset_name,
                            'invalid_url', 'Missing or invalid URL'
                        )
                        continue

                    # Fetch API data
                    result = fetch_api_data(url)

                    # Skip empty/invalid responses
                    if result.get('is_empty', False):
                        stats['empty_response'] += 1
                        self.log_error(
                            agency_name, url, dataset_name,
                            'empty_response',
                            result.get('error', 'Empty response'),
                            result.get('status_code')
                        )
                        continue

                    # Handle API errors
                    if result['status'] != 'success':
                        stats['api_error'] += 1
                        self.log_error(
                            agency_name, url, dataset_name,
                            'api_error',
                            result.get('error', 'Unknown API error'),
                            result.get('status_code')
                        )
                        continue

                    # Save dataset using name from source CSV
                    if not self.save_dataset(result['data'], dataset_name, agency_name):
                        stats['save_failed'] += 1
                        self.log_error(
                            agency_name, url, dataset_name,
                            'save_failed', 'Failed to save dataset'
                        )
                        continue

                    stats['success'] += 1

            # Print summary
            logging.info(f"\nProcessing summary for {agency_name}:")
            for stat, count in stats.items():
                logging.info(f"  {stat.replace('_', ' ').title()}: {count}")

            return stats['success'] > 0

        except Exception as e:
            logging.error(f"Error processing {agency_name}: {str(e)}")
            return False


def main():
    extractor = DatasetExtractor()
    agencies = {
        'local_executive': 'data/byMIO.csv',
        'Central_Government': 'data/byCGO.csv',
        'Quasi_Government': 'data/byQuasiOrg.csv'
    }

    print("Kazakhstan Government Data Extractor")
    print("-----------------------------------")

    while True:
        print("\nSelect agency to process:")
        for i, (name, _) in enumerate(agencies.items(), 1):
            print(f"{i}. {name.replace('_', ' ')}")
        print(f"{len(agencies) + 1}. Exit")

        try:
            choice = int(input("Enter choice (1-4): "))
            if choice == len(agencies) + 1:
                break
            elif 1 <= choice <= len(agencies):
                agency = list(agencies.keys())[choice - 1]
                if extractor.process_agency_data(agencies[agency], agency):
                    print(f"\nSuccessfully processed {agency.replace('_', ' ')} datasets")
                else:
                    print(f"\nThere were issues processing {agency.replace('_', ' ')} - check logs")
            else:
                print("Invalid choice, please try again")
        except ValueError:
            print("Please enter a number")


if __name__ == "__main__":
    main()