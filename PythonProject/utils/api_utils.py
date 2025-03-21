import requests
from config import API_KEY, HEADERS

def get_api_data(api_url):
    try:

        api_url = api_url.replace("yourApiKey", API_KEY)

        response = requests.get(api_url, headers=HEADERS)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Ошибка при запросе API: {response.status_code}, URL: {api_url}")
            return None
    except Exception as e:
        print(f"Ошибка при получении данных: {e}")
        return None