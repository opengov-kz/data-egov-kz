# selenium_utils.py (Fixed Version)
import os
import csv
import logging
import threading
from datetime import time

from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from config import BASE_URL

# Configuration
CSV_COLUMNS = [
    "Dataset URL", "Data Link", "Version Name",
    "Version Description", "Version Owner",
    "Categories", "Keywords", "Government Agency"
]
PAGE_LOAD_TIMEOUT = 20  # Increased timeout
REQUEST_DELAY = 2  # Increased delay


class SeleniumManager:
    def __init__(self, max_workers=3):
        self.driver_pool = []
        self.lock = threading.Lock()
        self.max_workers = max_workers

    def create_driver(self):
        """Create optimized Chrome driver instance"""
        options = Options()
        options.add_argument("--headless=new")  # New headless mode
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # Network optimizations
        prefs = {
            "profile.default_content_setting_values.images": 2,
            "profile.default_content_setting_values.javascript": 1,
        }
        options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        return driver

    def get_driver(self):
        """Get driver from pool or create new"""
        with self.lock:
            if self.driver_pool:
                return self.driver_pool.pop()
            return self.create_driver()

    def return_driver(self, driver):
        """Return driver to pool"""
        with self.lock:
            if len(self.driver_pool) < self.max_workers:
                self.driver_pool.append(driver)
            else:
                driver.quit()

    def cleanup(self):
        """Cleanup all drivers"""
        with self.lock:
            for driver in self.driver_pool:
                try:
                    driver.quit()
                except:
                    pass
            self.driver_pool = []


def process_agency_datasets(manager, agency_id, output_csv, pages=5):
    """Process datasets for a single agency"""
    driver = manager.get_driver()
    try:
        base_url = f"{BASE_URL}/datasets/search?statusType=1&govAgencyId={agency_id}"
        print(f"\nStarting processing for agency {agency_id}")

        for page in range(1, pages + 1):
            url = f"{base_url}&page={page}"
            print(f"Processing page {page} for {agency_id}")

            try:
                driver.get(url)
                # Wait for results to load
                WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".dataset-item"))
                )

                # Process each dataset on the page
                datasets = driver.find_elements(By.CSS_SELECTOR, ".dataset-item")
                for dataset in datasets:
                    try:
                        dataset_url = dataset.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                        process_single_dataset(driver, dataset_url, agency_id, output_csv)
                    except Exception as e:
                        logging.error(f"Error processing dataset: {e}")

                time.sleep(REQUEST_DELAY)

            except Exception as e:
                logging.error(f"Error processing page {page}: {e}")
                continue

    except Exception as e:
        logging.error(f"Fatal error processing agency {agency_id}: {e}")
    finally:
        manager.return_driver(driver)
    return True


def process_single_dataset(driver, dataset_url, agency_id, output_csv):
    """Process a single dataset"""
    try:
        driver.get(dataset_url)
        WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-striped"))
        )

        # Extract metadata
        metadata = {
            "Dataset URL": dataset_url,
            "Government Agency": agency_id,
            "Version Name": extract_text(driver, "#versionName"),
            "Version Description": extract_text(driver, "#versionDescription"),
            "Version Owner": extract_text(driver, "#versionOwner"),
            "Categories": extract_adjacent_text(driver, "Категории"),
            "Keywords": extract_text(driver, "#versionKeywordsBlock"),
            "Data Link": extract_data_link(driver)
        }

        save_to_csv(output_csv, metadata)
        time.sleep(REQUEST_DELAY / 2)  # Shorter delay between datasets

    except Exception as e:
        logging.error(f"Error processing {dataset_url}: {e}")
        raise


def extract_text(driver, selector):
    """Helper to extract text safely"""
    try:
        return driver.find_element(By.CSS_SELECTOR, selector).text.strip()
    except:
        return ""


def extract_adjacent_text(driver, text):
    """Extract text from adjacent cell"""
    try:
        return driver.find_element(
            By.XPATH, f"//td[contains(., '{text}')]/following-sibling::td"
        ).text.strip()
    except:
        return ""


def extract_data_link(driver):
    """Extract data download link"""
    try:
        return driver.find_element(
            By.CSS_SELECTOR, 'a[href*="api/v4/"]'
        ).get_attribute("href")
    except:
        return ""


def save_to_csv(output_csv, data_dict):
    """Save data to CSV"""
    try:
        file_exists = os.path.exists(output_csv)
        with open(output_csv, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(data_dict)
    except Exception as e:
        logging.error(f"CSV save error: {e}")
        raise