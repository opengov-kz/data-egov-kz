from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time
import pandas as pd
import logging
import json
from config import BASE_URL, CGO_DATASOURCE, MIO_DATASOURCE, QUASIORG_DATASOURCE
from webdriver_manager.chrome import ChromeDriverManager
from utils.selenium_utils import get_data_link, get_dataset_links, restart_chrome

# Configure logging
logging.basicConfig(
    filename="logs/error.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_gov_agencies(json_file):
    """Load govAgency values from a JSON file."""
    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return [item['govAgency'] for item in data]

def replace_api_key(file_path):
    """Replace 'yourApiKey' with 'API_KEY' in config.py"""
    with open(file_path, 'r', encoding='utf-8') as file:
        config_data = file.read()
    config_data = config_data.replace("yourApiKey", "API_KEY")
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(config_data)

def save_data_link(output_csv, dataset_link, data_link):
    """Save the dataset link and data link to the corresponding CSV file."""
    try:
        df = pd.DataFrame([{"Dataset URL": dataset_link, "Data Link": data_link}])
        df.to_csv(output_csv, mode='a', index=False, header=not pd.io.common.file_exists(output_csv), encoding='utf-8')
        print(f"Data saved to {output_csv}")
    except Exception as e:
        logging.error(f"Error saving data to {output_csv}: {e}")

def main():
    try:
        replace_api_key("config.py")  # Replace API Key in config.py

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)

        # Define JSON files and their corresponding output CSV file names
        json_files = {
            CGO_DATASOURCE: "data/byCGO.csv",
            MIO_DATASOURCE: "data/byMIO.csv",
            QUASIORG_DATASOURCE: "data/byQuasiOrg.csv"
        }

        for json_file, output_csv in json_files.items():
            gov_agencies = load_gov_agencies(json_file)

            for gov_agency in gov_agencies:
                print(f"Processing govAgency: {gov_agency}")
                retries = 0
                max_retries = 3
                while retries < max_retries:
                    try:
                        dataset_links = get_dataset_links(driver, BASE_URL, gov_agency)
                        if dataset_links:
                            for dataset in dataset_links:
                                save_data_link(output_csv, dataset["Dataset URL"], dataset["Data Link"])
                            break  # Exit the retry loop if data is successfully collected
                        else:
                            print(f"No data links found for govAgency {gov_agency}.")
                            break
                    except Exception as e:
                        retries += 1
                        if retries < max_retries:
                            print(f"Restarting Chrome and retrying govAgency {gov_agency} (attempt {retries + 1})...")
                            driver.quit()  # Quit the current driver
                            driver = restart_chrome()  # Restart Chrome
                            if not driver:
                                logging.error("Failed to restart Chrome. Exiting...")
                                return
                        else:
                            print(f"Max retries reached for govAgency {gov_agency}. Skipping...")
                            break

    except Exception as e:
        logging.error(f"Error in main function: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()