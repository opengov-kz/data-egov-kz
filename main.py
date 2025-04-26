# main.py
import os
import json
import time
import logging
import requests
import csv
from selenium.webdriver.common.by import By

from config import BASE_URL, CGO_DATASOURCE, MIO_DATASOURCE, QUASIORG_DATASOURCE
from utils.selenium_utils import (
    restart_chrome,
    extract_metadata_with_recovery, is_session_valid, bypass_captcha_if_present, extract_base_metadata,
    normalize_filename
)
import logging
from datetime import datetime

def log_diagnostic(message: str, log_file="logs/diagnostics.log"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def download_metadata_from_link(meta_link, output_dir, version_name):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json"
        }
        response = requests.get(meta_link, headers=headers, timeout=20)
        if response.status_code == 200:
            metadata = response.json()
            file_name = normalize_filename(version_name) + ".json"
            full_path = os.path.join(output_dir, file_name)
            with open(full_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            print(f"✅ Metadata saved to {full_path}")
        else:
            print(f"⚠️ Failed to download metadata. Status: {response.status_code}")
    except Exception as e:
        print(f"❌ Error downloading metadata from {meta_link}: {e}")
        logging.error(f"Download error: {e}")

def save_to_csv(csv_path, version_name, data_link,meta_link, dataset_url=""):
    file_exists = os.path.isfile(csv_path)
    with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['Version Name', 'Data Link', 'Dataset URL'])
        writer.writerow([version_name, data_link,meta_link, dataset_url])

def load_gov_agencies(json_file):
    with open(json_file, 'r', encoding='utf-8') as file:
        return [item['govAgency'] for item in json.load(file)]

def process_dataset_page(driver, agency, metadata_dir, current_page, csv_path):
    from utils.selenium_utils import refresh_driver
    search_url = (
        f"{BASE_URL}/datasets/search?text=&expType=1&govAgencyId={agency}&category="
        "&pDateBeg=&pDateEnd=&statusType=1&actualType=&datasetSortSelect=createdDateDesc&"
        f"page={current_page}"
    )
    log_diagnostic(f"[{agency}] 🔎 Page {current_page} → {search_url}")
    print(f"[{agency}] 📄 Page {current_page} → {search_url}")

    MAX_PAGE_RETRIES = 3
    retry_count = 0
    dataset_links = []

    while retry_count < MAX_PAGE_RETRIES:
        try:
            if not is_session_valid(driver):
                log_diagnostic(f"[{agency}] Session invalid. Restarting driver.")
                driver = refresh_driver()

            driver.get(search_url)
            time.sleep(3)

            # CAPTCHA bypass
            bypass_captcha_if_present(driver)

            if "502 Bad Gateway" in driver.page_source:
                raise Exception("Detected 502 error")

            dataset_links = [el.get_attribute("href") for el in
                             driver.find_elements("css selector", 'a[href^="/datasets/view?index="]')]

            if dataset_links:
                break
            else:
                log_diagnostic(f"[{agency}] ⚠️ No datasets found on page {current_page}, retrying...")

        except Exception as e:
            log_diagnostic(f"[{agency}] ❌ Exception on page {current_page} retry {retry_count+1}: {e}")
            driver = refresh_driver()

        retry_count += 1
        time.sleep(2 ** retry_count)

    if not dataset_links:
        log_diagnostic(f"[{agency}] 🛑 Gave up on page {current_page} after {MAX_PAGE_RETRIES} retries.")
        return False, None, driver

    processed = 0

    for link in dataset_links:
        try:
            metadata, driver = extract_metadata_with_recovery(driver, link)
            meta_link = metadata.get("Meta Link", "")
            version_name = metadata.get("Version Name", "dataset")
            data_link = metadata.get("Data Link", "")

            log_diagnostic(
                f"[{agency}] Processing: {version_name} — DataLink: {bool(data_link)}, MetaLink: {bool(meta_link)}")

            # Save to CSV regardless of whether there's metadata link or not
            save_to_csv(csv_path, version_name, data_link, meta_link, link)

            # Download metadata from meta link if available
            if meta_link:
                os.makedirs(metadata_dir, exist_ok=True)
                download_metadata_from_link(meta_link, metadata_dir, version_name)

            # Fallback: if both meta and data links are missing, extract base metadata manually
            if not meta_link and not data_link:
                base_metadata = extract_base_metadata(driver, dataset_url=link)
                os.makedirs(metadata_dir, exist_ok=True)
                file_path = os.path.join(metadata_dir, f"{normalize_filename(version_name)}.json")
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(base_metadata, f, indent=2, ensure_ascii=False)
                log_diagnostic(f"[{agency}] 📦 Saved fallback base metadata for {version_name}")

            processed += 1

        except Exception as e:
            log_diagnostic(f"[{agency}] ❌ Failed to process {link}: {e}")

    log_diagnostic(f"[{agency}] ✅ Page {current_page} complete. {processed} datasets processed.")
    return True, current_page + 1, driver


def main():
    driver = restart_chrome()
    if not driver:
        print("❌ Could not start browser")
        return

    datasets = {
        #CGO_DATASOURCE: "data/byCGO.csv",
        #MIO_DATASOURCE: "data/byMIO.csv",
        QUASIORG_DATASOURCE: "data/byQuasiOrg.csv"
    }

    for json_file, csv_path in datasets.items():
        gov_agencies = load_gov_agencies(json_file)

        for agency in gov_agencies:
            print(f"\n🔍 Processing agency: {agency}")
            metadata_dir = os.path.join("results/metadata")
            os.makedirs(metadata_dir, exist_ok=True)

            page = 1
            while True:
                has_more, next_page, driver = process_dataset_page(driver, agency, metadata_dir, page, csv_path)
                if not has_more:
                    break
                page = next_page or (page + 1)
                time.sleep(1)

    driver.quit()

if __name__ == "__main__":
    main()
