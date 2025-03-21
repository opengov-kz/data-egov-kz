from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.select import Select
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import logging
from config import BASE_URL, LIST_URL
from utils.selenium_utils import bypass_recaptcha, go_to_next_page
from utils.api_utils import get_api_data

logging.basicConfig(
    filename="logs/error.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def apply_actual_filter(driver):
    try:
        status_dropdown = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "statusType"))
        )
        select = Select(status_dropdown)
        select.select_by_value("1")
        print("Фильтр по актуальности 'Да' применен")
        time.sleep(3)
    except Exception as e:
        print(f"Ошибка при применении фильтра: {e}")

def main():
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)
        driver.get(LIST_URL)
        time.sleep(5)

        apply_actual_filter(driver)

        all_data = []
        current_page = 1

        while True:  # Continue until there's no next pag
            dataset_links = []
            for a in driver.find_elements(By.CSS_SELECTOR, 'a[href^="/datasets/view?index="]'):
                href = a.get_attribute('href')
                dataset_links.append(href)

            for link in dataset_links:
                if len(all_data) >= 50:
                    break

                print(f"Обработка набора данных: {link}")
                driver.get(link)
                time.sleep(3)

                if len(driver.find_elements(By.CSS_SELECTOR, '.recaptcha-checkbox-border')) > 0:
                    print("Обнаружена reCAPTCHA, решаем...")
                    bypass_recaptcha(driver)

                try:
                    api_link = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="https://data.egov.kz/api/v4/"]'))
                    ).get_attribute('href')
                    print(f"Ссылка на API: {api_link}")

                    api_data = get_api_data(api_link)
                    if api_data:
                        all_data.append(api_data)
                except Exception as e:
                    print(f"Ссылка на API не найдена: {e}")

            if not go_to_next_page(driver, current_page):
                print("Достигнут конец страниц или ошибка при переходе.")
                break

            current_page += 1
            time.sleep(3)

        if all_data:
            df = pd.json_normalize(all_data[:50])
            df.to_csv("data/filtered_api_data.csv", index=False)
            print("Первые 50 записей сохранены в файл data/filtered_api_data.csv")
        else:
            print("Нет данных для сохранения")

    except Exception as e:
        logging.error(f"Ошибка в основной функции: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()