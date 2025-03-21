from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import logging
import json
from config import BASE_URL, CGO_DATASOURCE, MIO_DATASOURCE, QUASIORG_DATASOURCE
from webdriver_manager.chrome import ChromeDriverManager
import random
import selenium.common.exceptions as sel_exceptions

from utils.api_utils import get_api_data

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


def get_data_link(driver, dataset_url):
    """Extract the data link from a dataset page with retry mechanism."""
    retries = 3
    for attempt in range(retries):
        try:
            driver.get(dataset_url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, 'a[target="_blank"][href*="https://data.egov.kz/api/v4/"]')
                )
            )
            data_link_element = driver.find_element(By.CSS_SELECTOR,
                                                    'a[target="_blank"][href*="https://data.egov.kz/api/v4/"]')
            return data_link_element.get_attribute('href')

        except sel_exceptions.TimeoutException:
            try:
                # Alternative search based on image provided
                data_link_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.XPATH, "//td[b[contains(text(), 'Ссылка на сервис')]]/following-sibling::td/a")
                    )
                )
                return data_link_element.get_attribute('href')

            except Exception as e:
                logging.error(f"Data link not found for {dataset_url}: {e}")

        except sel_exceptions.WebDriverException as e:
            if "502 Bad Gateway" in str(e) and attempt < retries - 1:
                wait_time = random.randint(5, 15)  # Random wait to avoid detection
                print(f"502 Bad Gateway encountered. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"Failed after {retries} attempts for {dataset_url}: {e}")
                return None


def get_dataset_links(driver, base_url, gov_agency_id):
    """Collect dataset links page by page until no datasets remain."""
    dataset_links = []
    current_page = 1

    while True:
        search_url = f"{base_url}/datasets/search?text=&expType=1&govAgencyId={gov_agency_id}&category=&pDateBeg=&pDateEnd=&statusType=1&actualType=&datasetSortSelect=createdDateDesc&page={current_page}"
        driver.get(search_url)
        time.sleep(3)  # Wait for the page to load

        # Extract dataset links from the current page
        page_links = [a.get_attribute('href') for a in
                      driver.find_elements(By.CSS_SELECTOR, 'a[href^="/datasets/view?index="]')]
        if not page_links:
            break  # Stop if no new links found

        for dataset_link in page_links:
            print(f"Navigating to dataset: {dataset_link}")
            data_link = get_data_link(driver, dataset_link)
            if data_link:
                dataset_links.append({"Dataset URL": dataset_link, "Data Link": data_link})
                print(f"Extracted Data Link: {data_link}")

        current_page += 1  # Move to the next page

    return dataset_links


def main():
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)

        json_files = [CGO_DATASOURCE, MIO_DATASOURCE, QUASIORG_DATASOURCE]
        all_data_links = []

        for json_file in json_files:
            gov_agencies = load_gov_agencies(json_file)
            for gov_agency in gov_agencies:
                print(f"Processing govAgency: {gov_agency}")
                all_data_links.extend(get_dataset_links(driver, BASE_URL, gov_agency))

        if all_data_links:
            df = pd.DataFrame(all_data_links)
            df.to_csv("data/all_data_links.csv", index=False, encoding='utf-8')
            print("All data links saved to data/all_data_links.csv")
        else:
            print("No data links were extracted.")

    except Exception as e:
        logging.error(f"Error in main function: {e}")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
