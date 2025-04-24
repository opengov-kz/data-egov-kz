#selenium_utils.py
import csv
import json
import os
import requests
import time
import logging
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import selenium.common.exceptions as sel_exceptions
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(
    filename='logs/scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Define the CSV columns for saving data
CSV_COLUMNS = [
    "Dataset URL", "Data Link", "Version Name", "Version Description", "Version Owner", "Categories", "Keywords"
]


def normalize_filename(name):
    if not name or not isinstance(name, str):
        return f"dataset_{int(time.time())}"
    try:
        name = name.replace('/', '_').replace('\\', '_').replace(':', '_')
        name = name.replace('*', '_').replace('?', '_').replace('"', '_')
        name = name.replace('<', '_').replace('>', '_').replace('|', '_')
        name = name.strip()
        return name[:100] if name else f"dataset_{int(time.time())}"
    except Exception as e:
        print(f"Warning: Error normalizing filename '{name}': {e}")
        return f"dataset_{int(time.time())}"


def restart_chrome():
    """Restart the Chrome WebDriver."""
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)
        return driver
    except Exception as e:
        logging.error(f"Failed to restart Chrome: {e}")
        return None

def ensure_csv_headers(output_csv):
    """Ensure the CSV file has the correct headers"""
    if not os.path.isfile(output_csv):
        try:
            with open(output_csv, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
                writer.writeheader()
            print(f"CSV headers initialized in {output_csv}")
        except Exception as e:
            logging.error(f"Error ensuring CSV headers: {e}")
            print(f"Failed to initialize CSV headers: {e}")

def get_data_link(driver, dataset_url, max_retries=3):
    """Extract the data link from a dataset page with retry mechanism."""
    retries = 0
    while retries < max_retries:
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
                data_link_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.XPATH, "//td[a[contains(@href, 'https://data.egov.kz/proxy/')]]/a")
                    )
                )
                return data_link_element.get_attribute('href')

            except Exception as e:
                logging.error(f"Data link not found for {dataset_url}: {e}")
                retries += 1
                if retries < max_retries:
                    print(f"Retrying data link extraction for {dataset_url} (attempt {retries + 1})...")
                    driver = restart_chrome()
                    if not driver:
                        return None
                else:
                    return None

        except sel_exceptions.WebDriverException as e:
            if "502 Bad Gateway" in str(e) or "Stacktrace" in str(e):
                logging.error(f"Encountered error: {e}. Restarting Chrome and retrying...")
                driver = restart_chrome()
                if not driver:
                    return None
            else:
                logging.error(f"Failed after {retries} attempts for {dataset_url}: {e}")
                return None
    return None

def extract_base_metadata(driver, dataset_url):
    metadata = {
        "Dataset URL": dataset_url if dataset_url else driver.current_url,
        "Version Name": "",
        "Version Description": "",
        "Version Owner": "",
        "Categories": "",
        "Keywords": ""
    }

    try:
        version_name = driver.find_element(By.ID, "versionName").text.strip()
        if not version_name:
            version_name = driver.find_element(
                By.XPATH, "//td[contains(., 'Наименование')]/following-sibling::td"
            ).text.strip()
        metadata["Version Name"] = version_name
    except:
        pass

    try:
        version_desc = driver.find_element(By.ID, "versionDescription").text.strip()
        if not version_desc:
            version_desc = driver.find_element(
                By.XPATH, "//td[contains(., 'Описание')]/following-sibling::td"
            ).text.strip()
        metadata["Version Description"] = version_desc
    except:
        pass

    try:
        owner = driver.find_element(By.ID, "versionOwner").text.strip()
        if not owner:
            owner = driver.find_element(
                By.XPATH, "//td[contains(., 'Владелец')]/following-sibling::td"
            ).text.strip()
        metadata["Version Owner"] = owner
    except:
        pass

    try:
        keywords = driver.find_element(By.ID, "Keywords").text.strip()
        if not keywords:
            keywords = driver.find_element(
                By.XPATH, "//td[contains(., 'Ключевые слова')]/following-sibling::td"
            ).text.strip()
        metadata["Keywords"] = keywords
    except:
        pass

    try:
        categories = driver.find_element(
            By.XPATH, "//td[contains(., 'Категории')]/following-sibling::td"
        ).text.strip()
        if not categories:
            categories = driver.find_element(
                By.CSS_SELECTOR, ".dataset-categories"
            ).text.strip()
        metadata["Categories"] = categories
    except:
        pass

    return metadata


def save_to_csv(output_csv, data_dict):
    """Save dictionary data to CSV with proper headers"""
    try:
        ensure_csv_headers(output_csv)

        # Prepare the data ensuring all columns are present
        row_data = {col: data_dict.get(col, "") for col in CSV_COLUMNS}

        with open(output_csv, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writerow(row_data)

        print(f"Data successfully saved to {output_csv}")
        return True
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")
        print(f"Failed to save data: {e}")
        return False


def extract_metadata(driver, dataset_url):
    """Robust metadata extraction with multiple fallback strategies"""
    metadata = {
        "Dataset URL": dataset_url,
        "Meta Link": "",
        "Version Name": "",
        "Version Description": "",
        "Version Owner": "",
        "Categories": "",
        "Keywords": ""
    }

    try:
        # Load the page with verification
        driver.get(dataset_url)
        WebDriverWait(driver, 15).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        # Wait for either the metadata table or key elements
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "table.table-striped, #versionName, #versionDescription")
                )
            )
        except:
            print("Warning: Metadata table not found using standard locators")

    except Exception as e:
        logging.error(f"Metadata extraction error for {dataset_url}: {str(e)[:200]}")
        print(f"Error extracting metadata: {e}")

    return metadata


def download_metadata_from_meta_link(driver, meta_link):
    """Download metadata from the metaLink or copy it if download fails"""
    metadata = {}

    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        }
        requests.get(meta_link, headers=headers, timeout=20)

        response = requests.get(meta_link, timeout=10)
        if response.status_code == 200:
            metadata = response.json()
        else:
            print(f"⚠️ Meta link returned status {response.status_code}. Copying metadata instead.")
            metadata = extract_metadata(driver, meta_link)

    except Exception as e:
        print(f"⚠️ Error downloading metadata from {meta_link}: {e}")
        metadata = extract_metadata(driver, meta_link)

    return metadata


def is_session_valid(driver):
    """Check if the session is still valid (e.g., by checking the page title)"""
    try:
        driver.get("about:blank")
        return True
    except:
        return False


def recover_session(driver, dataset_url):
    """Recover the session by restarting the browser or reloading the page"""
    driver.quit()
    driver = restart_chrome()
    if driver:
        driver.get(dataset_url)
    return driver


def refresh_driver():
    """Refresh the driver session"""
    return restart_chrome()


from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import time


def download_json_from_meta_link(meta_link, version_name, output_dir, max_retries=3):
    """Download metadata from metaLink with proper headers and retries"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://data.egov.kz/"
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(meta_link, headers=headers, timeout=20)
            response.raise_for_status()  # Will raise HTTPError for 4XX/5XX status

            metadata = response.json()
            filename = f"{version_name}.json".replace("/", "_")[:150]  # Safe filename
            path = os.path.join(output_dir, filename)

            os.makedirs(output_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            print(f"✅ Metadata saved to {path}")
            return True

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print(f"⚠️ Access forbidden to {meta_link} (attempt {attempt + 1})")
                if attempt == max_retries - 1:
                    print(f"❌ Permanent 403 Forbidden for {meta_link}")
                    return False
            else:
                print(f"⚠️ HTTP error {e.response.status_code} for {meta_link}")
                return False
        except Exception as e:
            print(f"⚠️ Error downloading {meta_link}: {str(e)[:200]}")
            if attempt == max_retries - 1:
                return False

        time.sleep(2 ** (attempt + 1))  # Exponential backoff

    return False
def output_csv(csv_path, version_name, data_link):
    try:
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        file_exists = os.path.isfile(csv_path)
        with open(csv_path, 'a', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['Version', 'Data Link'])
            writer.writerow([version_name, data_link])
    except Exception as e:
        print(f"Failed to save data: {e}")


def extract_metadata_link_and_version(driver, dataset_url):
    driver.get(dataset_url)
    time.sleep(2)  # Add wait if needed

    try:
        meta_td = driver.find_element(By.ID, "metaLink")
        meta_link = meta_td.find_element(By.TAG_NAME, "a").get_attribute("href")
    except:
        return None, None

    try:
        version_name_elem = driver.find_element(By.CLASS_NAME, "dataset-version-name")
        version_name = version_name_elem.text.strip()
    except:
        version_name = "unknown_version"

    return meta_link, version_name

def extract_metadata_with_recovery(driver, dataset_link, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            if not is_session_valid(driver):
                new_driver = refresh_driver()
                if not new_driver:
                    raise sel_exceptions.WebDriverException("Failed to restart Chrome")
                driver = new_driver

            driver.get(dataset_link)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataset-details, #versionName"))
            )

            metadata = {}
            metadata["Dataset URL"] = dataset_link
            # Version Name
            version_name = f"dataset_{int(time.time())}"
            try:
                version_name = driver.find_element(By.ID, 'versionName').text.strip()
                if not version_name:
                    version_name = driver.find_element(By.CSS_SELECTOR, 'h1.dataset-title').text.strip()
            except sel_exceptions.NoSuchElementException:
                pass
            metadata["Version Name"] = version_name
            print(f"Version Name: {version_name}")

            # Data Link
            # Data Link with multiple fallbacks
            data_link = ""
            try:
                # Primary: v4 API format
                data_link_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//a[contains(@href, "api/v4/")]'))
                )
                data_link = data_link_element.get_attribute("href")
            except (sel_exceptions.NoSuchElementException, sel_exceptions.TimeoutException):
                try:
                    # Fallback: proxy format
                    data_link_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '//a[contains(@href, "/proxy/")]')
                        )
                    )
                    data_link = data_link_element.get_attribute("href")
                    metadata["Data Link"] = data_link
                    print(f"Proxy Data Link: {data_link}")

                    # Enrich with base metadata
                    base_metadata = extract_base_metadata(driver, dataset_url=dataset_link)
                    metadata.update(base_metadata)

                    # Save base metadata to JSON
                    os.makedirs("results/metadata/", exist_ok=True)
                    file_path = os.path.join("results/metadata/", f"{normalize_filename(version_name)}.json")
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(metadata, f, indent=2, ensure_ascii=False)
                    print(f"✅ Fallback metadata saved for proxy-only dataset: {file_path}")

                except (sel_exceptions.NoSuchElementException, sel_exceptions.TimeoutException):
                    data_link = ""

            metadata["Data Link"] = data_link
            if data_link:
                print(f"Data Link: {data_link}")
            else:
                print("⚠️ Data Link not found (even with fallback)")

            # Meta Link
            meta_link = ""
            try:
                meta_td = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'td#metaLink a, a.meta-link'))
                )
                meta_link = meta_td.get_attribute("href")
            except (sel_exceptions.NoSuchElementException, sel_exceptions.TimeoutException):
                pass
            metadata["Meta Link"] = meta_link
            if meta_link:
                print(f"Meta Link: {meta_link}")
            else:
                print("⚠️ Meta Link not found")

            return metadata, driver

        except sel_exceptions.WebDriverException as e:
            print(f"❌ Failed to extract metadata from {dataset_link}: {e}")
            new_driver = recover_session(driver, dataset_link)
            if new_driver:
                driver = new_driver
                print("🔁 Recovered ChromeDriver session, retrying metadata extraction...")
                continue  # ← Retry the same attempt without increasing retry count
            else:
                break  # Unable to recover, exit the loop

    return {
        "Version Name": f"dataset_{int(time.time())}",
        "Data Link": "",
        "Meta Link": ""
    }, driver

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

def bypass_captcha_if_present(driver):
    try:
        # Wait briefly for CAPTCHA modal
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.ID, "captchaSuccess"))
        )
        print("🛡️ CAPTCHA modal detected — attempting to bypass")

        # Click CAPTCHA checkbox if it's a custom checkbox
        try:
            checkbox = driver.find_element(By.CSS_SELECTOR, 'input[type="checkbox"]')
            if not checkbox.is_selected():
                checkbox.click()
                print("✅ CAPTCHA checkbox clicked")
        except Exception:
            pass  # Maybe it's not needed or already checked

        # Click the "Готово" button to dismiss the modal
        button = driver.find_element(By.ID, "captchaSuccess")
        button.click()
        print("✅ CAPTCHA dismissed successfully")
        time.sleep(2)
    except Exception:
        pass  # CAPTCHA modal didn't appear


def get_dataset_links(driver, base_url, gov_agency_id, output_csv, metadata_dir, max_retries=3):
    """Scrape dataset links via URL-based pagination, extract metadata, and save results."""
    current_page = 1
    processed_count = 0

    base_search_url = (
        f"{base_url}/datasets/search?"
        "text=&expType=1&category=&pDateBeg=&pDateEnd="
        "statusType=1&actualType=&datasetSortSelect=createdDateDesc&"
        f"govAgencyId={gov_agency_id}&page={{page}}"
    )

    while True:
        search_url = base_search_url.format(page=current_page)
        print(f"\n[Page {current_page}] Fetching: {search_url}")

        # Retry loading page
        for attempt in range(max_retries):
            try:
                if not is_session_valid(driver):
                    driver = refresh_driver()
                driver.get(search_url)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.datasets-table"))
                )
                break
            except Exception as e:
                print(f"[Retry {attempt + 1}] Error loading page: {e}")
                time.sleep(2 ** attempt)
                driver = refresh_driver()
        else:
            print(f"Failed to load page after {max_retries} attempts.")
            break

        # Extract dataset links
        dataset_links = [
            a.get_attribute("href") for a in driver.find_elements(
                By.CSS_SELECTOR, 'a[href^="/datasets/view?index="]'
            )
        ]

        if not dataset_links:
            print("No dataset links found. End of pages reached.")
            break

        for dataset_relative_url in dataset_links:
            dataset_url = base_url + dataset_relative_url
            print(f"Processing dataset: {dataset_url}")
            try:
                metadata = extract_metadata_with_recovery(driver, dataset_url)
                meta_link = metadata.get("Meta Link", "")

                if meta_link:
                    metadata = download_metadata_from_meta_link(driver, meta_link)

                save_to_csv(output_csv, metadata)

                # Save to JSON
                dataset_id = dataset_relative_url.split("index=")[-1]
                metadata_file = os.path.join(metadata_dir, f"{gov_agency_id}_{dataset_id}.json")
                os.makedirs(metadata_dir, exist_ok=True)
                with open(metadata_file, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)

                processed_count += 1
                print(f"Processed {processed_count} datasets")

            except Exception as e:
                logging.error(f"Failed to process dataset {dataset_url}: {e}")
                print(f"Failed to process dataset: {e}")

        current_page += 1
