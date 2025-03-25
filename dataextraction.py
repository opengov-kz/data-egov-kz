import pandas as pd
import logging
from tqdm import tqdm
import os
from utils.api_utils import fetch_api_data


class DataExtractor:
    def __init__(self):
        self.all_columns = set()
        self.valid_count = 0
        self.error_count = 0

    def extract_records(self, data):
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            if 'data' in data and isinstance(data['data'], list):
                return data['data']
            return [data]
        return []

    def process_dataset(self, input_csv, output_csv):
        if not os.path.exists(input_csv):
            logging.error(f"Input file not found: {input_csv}")
            return False

        try:
            df = pd.read_csv(input_csv, encoding='utf-8')
            if 'Data Link' not in df.columns:
                logging.error("Missing 'Data Link' column")
                return False

            all_records = []

            with tqdm(total=len(df), desc=f"Processing {os.path.basename(input_csv)}") as pbar:
                for _, row in df.iterrows():
                    if pd.isna(row['Data Link']):
                        pbar.update(1)
                        continue

                    result = fetch_api_data(row['Data Link'])
                    if result['status'] == 'success':
                        records = self.extract_records(result['data'])
                        for record in records:
                            record['source_url'] = result['source_url']
                            all_records.append(record)
                            self.all_columns.update(record.keys())
                        self.valid_count += len(records)
                    else:
                        self.error_count += 1
                    pbar.update(1)

            if all_records:
                final_df = pd.DataFrame(all_records)

                final_df.to_csv(
                    output_csv,
                    index=False,
                    encoding='utf-8-sig',
                    escapechar='\\',
                    quotechar='"',
                    quoting=csv.QUOTE_NONNUMERIC
                )

                logging.info(f"Saved {len(final_df)} records to {output_csv}")
                print(f"\nSample output:\n{final_df.head(3).to_string(index=False)}")

            return True

        except Exception as e:
            logging.error(f"Error processing {input_csv}: {str(e)}")
            return False


def main():
    print("Unicode-Compatible Data Extractor")
    print("--------------------------------")

    extractor = DataExtractor()
    os.makedirs('results/', exist_ok=True)

    while True:
        print("\nAvailable datasets:")
        print("1. byCGO")
        print("2. byMIO")
        print("3. byQuasiOrg")
        print("4. Exit")

        choice = input("Select dataset (1-4): ").strip()

        file_map = {
            '1': ('data/byCGO.csv', 'results/byCGO_utf8.csv'),
            '2': ('data/byMIO.csv', 'results/byMIO_utf8.csv'),
            '3': ('data/byQuasiOrg.csv', 'results/byQuasiOrg_utf8.csv')
        }

        if choice == '4':
            break
        elif choice in file_map:
            input_file, output_file = file_map[choice]
            if extractor.process_dataset(input_file, output_file):
                print(f"\nSuccess: {extractor.valid_count} records")
                if extractor.error_count > 0:
                    print(f"Errors: {extractor.error_count}")
            else:
                print("Processing failed")
        else:
            print("Invalid choice")


if __name__ == "__main__":
    import csv

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/data_extraction_unicode.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    main()