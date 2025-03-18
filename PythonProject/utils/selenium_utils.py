from _pydatetime import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

def go_to_next_page(driver):
    try:
        next_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//li[@class="next"]/a[contains(text(), "Следующая")]'))
        )

        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
        time.sleep(1)
        if next_button.is_displayed() and next_button.is_enabled():

            next_button.click()
            print("Переход на следующую страницу")
            return True
        else:
            print("Кнопка 'Следующая' не доступна для взаимодействия.")
            return False
    except Exception as e:
        print(f"Не удалось перейти на следующую страницу: {e}")
        return False