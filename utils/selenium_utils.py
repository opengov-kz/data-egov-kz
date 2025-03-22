from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import logging
import selenium.common.exceptions as sel_exceptions
from webdriver_manager.chrome import ChromeDriverManager

def restart_chrome():
    """Restart the Chrome WebDriver."""
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)
        return driver
    except Exception as e:
        logging.error(f"Failed to restart Chrome: {e}")
        return None

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
                    driver = restart_chrome()  # Restart Chrome and retry
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

def get_dataset_links(driver, base_url, gov_agency_id, max_retries=3):
    """Collect dataset links page by page until no datasets remain."""
    dataset_links = []
    current_page = 1

    while True:
        search_url = f"{base_url}/datasets/search?text=&expType=1&govAgencyId={gov_agency_id}&category=&pDateBeg=&pDateEnd=&statusType=1&actualType=&datasetSortSelect=createdDateDesc&page={current_page}"
        try:
            driver.get(search_url)
            time.sleep(3)

            print(f"Page source for {search_url}:")
            print(driver.page_source[:1000])  # Print the first 1000 characters of the page source

            page_links = [a.get_attribute('href') for a in
                          driver.find_elements(By.CSS_SELECTOR, 'a[href^="/datasets/view?index="]')]
            if not page_links:
                print(f"No dataset links found on page {current_page} for govAgency {gov_agency_id}.")
                break

            for dataset_link in page_links:
                print(f"Navigating to dataset: {dataset_link}")
                data_link = get_data_link(driver, dataset_link)
                if data_link:
                    dataset_links.append({"Dataset URL": dataset_link, "Data Link": data_link})
                    print(f"Extracted Data Link: {data_link}")
                else:
                    print(f"No data link found for {dataset_link}. Skipping...")

            current_page += 1

        except sel_exceptions.WebDriverException as e:
            if "502 Bad Gateway" in str(e) or "Stacktrace" in str(e):
                logging.error(f"Encountered error: {e}. Restarting Chrome and retrying...")
                driver = restart_chrome()
                if not driver:
                    return None
            else:
                logging.error(f"Error processing page {current_page} for govAgency {gov_agency_id}: {e}")
                continue

    return dataset_links