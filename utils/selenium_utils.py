#selenium_utils.py
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
