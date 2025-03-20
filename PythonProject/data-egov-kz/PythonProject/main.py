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
from utils.selenium_utils import bypass_recaptcha
from utils.api_utils import get_api_data

# Настройка логирования
logging.basicConfig(
    filename="logs/error.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def apply_actual_filter(driver):
    """Применяет фильтр по актуальности 'Да'."""
    try:
        # Находим выпадающий список
        status_dropdown = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "statusType"))
        )

        # Создаем объект Select
        select = Select(status_dropdown)

        # Выбираем значение "Да" (значение "1")
        select.select_by_value("1")
        print("Фильтр по актуальности 'Да' применен")

        # Ждем, чтобы фильтр применился
        time.sleep(3)
    except Exception as e:
        print(f"Ошибка при применении фильтра: {e}")

def go_to_next_page(driver):
    """Переходит на следующую страницу."""
    try:
        # Ищем кнопку "Cледующая"
        next_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//li[@class="next"]/a'))
        )

        # Прокручиваем страницу до кнопки
        driver.execute_script("arguments[0].scrollIntoView();", next_button)
        time.sleep(1)  # Ждем, чтобы страница прокрутилась

        # Проверяем, доступна ли кнопка для взаимодействия
        if next_button.is_displayed() and next_button.is_enabled():
            next_button.click()
            print("Переход на следующую страницу")
            return True
        else:
            print("Кнопка 'Cледующая' не доступна для взаимодействия.")
            return False
    except Exception as e:
        print(f"Не удалось перейти на следующую страницу: {e}")
        return False

def main():
    try:
        # Автоматическая установка и настройка драйвера
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)

        # Открываем страницу со списком наборов данных
        driver.get(LIST_URL)
        time.sleep(5)

        # Применяем фильтр по актуальности 'Да'
        apply_actual_filter(driver)

        # Создаем список для хранения данных
        all_data = []

        while len(all_data) < 50:  # Собираем данные, пока не наберется 50 записей
            # Ищем все ссылки на наборы данных на текущей странице
            dataset_links = []
            for a in driver.find_elements(By.CSS_SELECTOR, 'a[href^="/datasets/view?index="]'):
                href = a.get_attribute('href')
                dataset_links.append(href)

            # Обрабатываем каждый набор данных на текущей странице
            for link in dataset_links:
                if len(all_data) >= 50:
                    break  # Прерываем, если собрали 50 записей

                print(f"Обработка набора данных: {link}")

                # Переходим на страницу набора данных
                driver.get(link)
                time.sleep(3)

                # Проверяем наличие reCAPTCHA
                if len(driver.find_elements(By.CSS_SELECTOR, '.recaptcha-checkbox-border')) > 0:
                    print("Обнаружена reCAPTCHA, решаем...")
                    bypass_recaptcha(driver)

                # Получаем данные со страницы (например, ссылку на API)
                try:
                    api_link = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="https://data.egov.kz/api/v4/"]'))
                    ).get_attribute('href')
                    print(f"Ссылка на API: {api_link}")

                    # Получаем данные по API
                    api_data = get_api_data(api_link)
                    if api_data:
                        all_data.append(api_data)  # Добавляем данные в список
                except Exception as e:
                    print(f"Ссылка на API не найдена: {e}")

            # Переходим на следующую страницу
            if not go_to_next_page(driver):
                break  # Если следующей страницы нет, завершаем цикл

        # Сохраняем первые 50 записей в CSV-файл
        if all_data:
            # Преобразуем список данных в DataFrame
            df = pd.json_normalize(all_data[:50])  # Ограничиваемся первыми 50 записями
            # Сохраняем DataFrame в CSV-файл
            df.to_csv("data/filtered_api_data.csv", index=False)
            print("Первые 50 записей сохранены в файл data/filtered_api_data.csv")
        else:
            print("Нет данных для сохранения")

    except Exception as e:
        logging.error(f"Ошибка в основной функции: {e}")
    finally:
        # Закрываем браузер
        driver.quit()

if __name__ == "__main__":
    main()