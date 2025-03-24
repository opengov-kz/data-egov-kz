import pandas as pd
import requests
import logging
import json
import os
from tqdm import tqdm

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/extraction_debug.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def extract_data_from_link(data_link):
    try:
        logging.debug(f"Fetching: {data_link}")
        response = requests.get(data_link, timeout=15)
        logging.debug(f"Response status: {response.status_code}")
        logging.debug(f"Headers: {response.headers}")

        sample_response = response.text[:200]
        logging.debug(f"Response sample: {sample_response}")

        response.raise_for_status()

        try:
            data = response.json()
            logging.debug(
                f"Parsed JSON keys: {data.keys() if isinstance(data, dict) else 'List length: ' + str(len(data))}")
            return data
        except json.JSONDecodeError:
            logging.warning(f"Non-JSON response: {sample_response}")
            return None

    except Exception as e:
        logging.error(f"Error fetching {data_link}: {str(e)}", exc_info=True)
        return None


def main():
    print("Debug mode activated. Checking data sources...")

    TEST_URL = "https://data.egov.kz/api/v4/gov_agency_example?apiKey=dcc32505f6134b818ec7ce60b1d5b0c6"  # Replace with a real endpoint
    print(f"\nTesting with sample URL: {TEST_URL}")
    test_data = extract_data_from_link(TEST_URL)
    print(f"Test response: {str(test_data)[:200]}...")

    if test_data is None:
        print("\n⚠️  WARNING: Failed to fetch test URL. Check API connectivity.")
        return

    choice = input("\nProceed with extraction? Enter dataset number (1-3) or 'q' to quit: ")
    if choice.lower() == 'q':
        return

    file_map = {
        "1": ("data/byCGO.csv", "data/extracted_byCGO_debug.csv"),
        "2": ("data/byMIO.csv", "data/extracted_byMIO_debug.csv"),
        "3": ("data/byQuasiOrg.csv", "data/extracted_byQuasiOrg_debug.csv")
    }

    if choice not in file_map:
        print("Invalid choice.")
        return

    input_csv, output_csv = file_map[choice]

    if not os.path.exists(input_csv):
        logging.error(f"Input file missing: {input_csv}")
        print(f"Error: {input_csv} not found!")
        return

    try:
        df = pd.read_csv(input_csv)
        print(f"\nFound {len(df)} records in {input_csv}")
        print("Sample Data Link:", df['Data Link'].iloc[0] if 'Data Link' in df.columns else "NO 'Data Link' COLUMN!")
    except Exception as e:
        logging.error(f"CSV read error: {e}", exc_info=True)
        print("Failed to read CSV. Check format.")
        return

    extracted_data = []
    for i, row in tqdm(df.iterrows(), total=len(df)):
        link = row.get('Data Link')
        if pd.isna(link):
            continue

        data = extract_data_from_link(link)
        if data:
            extracted_data.append({"url": link, "data": data})

    if extracted_data:
        pd.DataFrame(extracted_data).to_csv(output_csv, index=False)
        print(f"\n Saved {len(extracted_data)} records to {output_csv}")
    else:
        print("\n No valid data extracted. Check debug.log")


if __name__ == "__main__":
    main()