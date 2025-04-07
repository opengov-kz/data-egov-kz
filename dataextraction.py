#dataextraction.py:
import pandas as pd
import logging
from tqdm import tqdm
import os
import csv
import re
from utils.api_utils import fetch_api_data


class AgencyDatasetExtractor:
    def __init__(self):
        self.base_output_dir = 'results/datasets'
        os.makedirs(self.base_output_dir, exist_ok=True)

    def create_agency_folder(self, gov_agency):
        safe_name = re.sub(r'[^\w\s-]', '', gov_agency).strip().replace(' ', '_')
        folder_path = os.path.join(self.base_output_dir, safe_name)
        os.makedirs(folder_path, exist_ok=True)
        return folder_path

    def save_agency_dataset(self, data, gov_agency, source_url, normalized_url):
        try:
            agency_folder = self.create_agency_folder(gov_agency)

            endpoint = normalized_url.split('?')[0].split('/')[-1] or 'dataset'
            safe_filename = f"{endpoint[:50]}.csv".replace(' ', '_')
            output_path = os.path.join(agency_folder, safe_filename)

            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                logging.error(f"Unexpected data type: {type(data)}")
                return False

            df['source_url'] = source_url
            df['api_endpoint'] = normalized_url
            df['government_agency'] = gov_agency

            df.to_csv(
                output_path,
                index=False,
                encoding='utf-8-sig',
                quoting=csv.QUOTE_NONNUMERIC
            )
            logging.info(f"Saved dataset to {output_path}")
            return True

        except Exception as e:
            logging.error(f"Failed to save dataset: {str(e)}")
            return False

    def process_agency_data(self, input_csv, gov_agency):
        if not os.path.exists(input_csv):
            logging.error(f"Input file not found: {input_csv}")
            return False

        try:
            df = pd.read_csv(input_csv, encoding='utf-8')
            if 'Data Link' not in df.columns:
                logging.error("Missing 'Data Link' column")
                return False

            success_count = 0
            error_count = 0

            with tqdm(total=len(df), desc=f"Processing {gov_agency} datasets") as pbar:
                for _, row in df.iterrows():
                    if pd.isna(row['Data Link']):
                        pbar.update(1)
                        continue

                    result = fetch_api_data(row['Data Link'])
                    if result['status'] == 'success':
                        if self.save_agency_dataset(
                                result['data'],
                                gov_agency,
                                result['source_url'],
                                result['normalized_url']
                        ):
                            success_count += 1
                        else:
                            error_count += 1
                    else:
                        error_count += 1
                    pbar.update(1)

            print(f"\nProcessed {gov_agency}:")
            print(f"  Successfully saved {success_count} datasets")
            print(f"  Failed to process {error_count} URLs")
            return True

        except Exception as e:
            logging.error(f"Error processing {input_csv}: {str(e)}")
            return False


def main():
    print("Government Agency Dataset Extractor")
    print("----------------------------------")

    extractor = AgencyDatasetExtractor()

    agencies = {
        'Local executive Organizations': 'data/byMIO.csv',
        'Central Government Organizations': 'data/byCGO.csv',
        'Quasi-Government Organizations': 'data/byQuasiOrg.csv'
    }

    while True:
        print("\nAvailable government agencies:")
        for i, (agency, _) in enumerate(agencies.items(), 1):
            print(f"{i}. {agency}")
        print(f"{len(agencies) + 1}. Exit")

        choice = input("Select agency (1-4): ").strip()

        if choice == '4':
            break
        elif choice in ['1', '2', '3']:
            agency_name = list(agencies.keys())[int(choice) - 1]
            input_file = agencies[agency_name]
            if not extractor.process_agency_data(input_file, agency_name):
                print("Processing failed - check logs")
        else:
            print("Invalid choice")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('agency_extraction.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    main()