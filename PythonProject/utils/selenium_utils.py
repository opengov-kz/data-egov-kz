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

        next_button = driver.find_elements(By.XPATH, '//li[@class="next"]/a')
        if next_button:
            next_button[0].click()
            time.sleep(3)

        next_page_number = current_page + 1
        page_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f'//li/a[text()="{next_page_number}"]'))
        )
        page_link.click()
        print(f"Перешли на страницу {next_page_number}")

        return True
    except Exception as e:
        print(f"Ошибка при переходе на следующую страницу: {e}")
        return False
