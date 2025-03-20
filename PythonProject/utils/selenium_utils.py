from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def bypass_recaptcha(driver):
    try:
        checkbox = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.recaptcha-checkbox-border'))
        )
        checkbox.click()
        print("Чекбокс 'Я не робот' нажат")

        ready_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[type="submit"]'))
        )
        ready_button.click()
        print("Кнопка 'ГОТОВО' нажата")
    except Exception as e:
        print(f"Ошибка при обходе reCAPTCHA: {e}")

def go_to_next_page(driver, current_page):
    try:
        if current_page == 1:
            next_link = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//li[@class="next"]/a'))
            )
            next_url = next_link.get_attribute('href')
            if next_url:
                driver.get(next_url)  # Переходим по ссылке
                print("Переход на следующую страницу")
                return True
            else:
                print("Ссылка 'Следующая' не найдена.")
                return False
        else:
            page_link = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, f'//li/a[text()="{current_page + 1}"]'))
            )
            page_url = page_link.get_attribute('href')
            if page_url:
                driver.get(page_url)
                print(f"Переход на страницу {current_page + 1}")
                return True
            else:
                print(f"Ссылка для страницы {current_page + 1} не найдена.")
                return False
    except Exception as e:
        print(f"Не удалось перейти на следующую страницу: {e}")
        return False