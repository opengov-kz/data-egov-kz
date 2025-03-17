from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import requests

API_KEY = "324aa0a952ad40f2834502df3ecd5727"

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)
def solve_recaptcha(site_key, page_url):
    try:
        response = requests.post(
            f"http://2captcha.com/in.php?key={API_KEY}&method=userrecaptcha&googlekey={site_key}&pageurl={page_url}"
        ).text
        if "ERROR" in response:
            print(f"Ошибка от 2Captcha: {response}")
            return None
        captcha_id = response.split("|")[1]
        solution = None
        while True:
            response = requests.get(f"http://2captcha.com/res.php?key={API_KEY}&action=get&id={captcha_id}").text
            if response == "CAPCHA_NOT_READY":
                time.sleep(5)
            elif "ERROR" in response:
                print(f"Ошибка при получении решения: {response}")
                return None
            else:
                solution = response.split("|")[1]
                break

        return solution
    except Exception as e:
        print(f"Ошибка в функции solve_recaptcha: {e}")
        return None


def bypass_recaptcha(driver):
    try:
        site_key = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.g-recaptcha'))
        ).get_attribute('data-sitekey')
        page_url = driver.current_url

        solution = solve_recaptcha(site_key, page_url)
        if not solution:
            print("Не удалось решить reCAPTCHA")
            return

        driver.execute_script(
            f'document.getElementById("g-recaptcha-response").innerHTML="{solution}";'
        )
        time.sleep(2)

        submit_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[type="submit"]'))
        )
        submit_button.click()
    except Exception as e:
        print(f"Ошибка при обходе reCAPTCHA: {e}")

# Основная функция
def main():
    try:
        # Открываем страницу со списком наборов данных
        driver.get("https://data.egov.kz/datasets/listbygovagency")
        time.sleep(5)

        # Ищем все ссылки на наборы данных
        dataset_links = []
        for a in driver.find_elements(By.CSS_SELECTOR, 'a[href^="/datasets/view?index="]'):
            href = a.get_attribute('href')
            dataset_links.append(href)

        # Обрабатываем каждый набор данных
        for link in dataset_links:
            print(f"Обработка набора данных: {link}")

            # Переходим на страницу набора данных
            driver.get(link)
            time.sleep(3)

            # Проверяем наличие reCAPTCHA
            if len(driver.find_elements(By.CSS_SELECTOR, '.g-recaptcha')) > 0:
                print("Обнаружена reCAPTCHA, решаем...")
                bypass_recaptcha(driver)

            # Получаем данные со страницы (ссылку на API)
            try:
                api_link = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="api"]'))
                ).get_attribute('href')
                print(f"Ссылка на API: {api_link}")
            except Exception as e:
                print(f"Ссылка на API не найдена: {e}")

    except Exception as e:
        print(f"Ошибка в основной функции: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
