import requests
from config import CKAN_BASE_URL, CKAN_HEADERS


def create_dataset(dataset_id, title, owner_org):
    url = f"{CKAN_BASE_URL}/api/3/action/package_create"
    data = {
        "name": dataset_id,
        "title": title,
        "owner_org": owner_org
    }
    response = requests.post(url, headers=CKAN_HEADERS, json=data)
    return response.ok or response.status_code == 409


def get_existing_resources(dataset_id):
    url = f"{CKAN_BASE_URL}/api/3/action/package_show?id={dataset_id}"
    response = requests.get(url, headers=CKAN_HEADERS)
    if response.ok:
        resources = response.json()["result"].get("resources", [])
        return {res["name"]: res["id"] for res in resources}
    return {}
