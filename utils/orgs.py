import requests
from config import CKAN_BASE_URL, CKAN_HEADERS


def get_organization_list():
    url = f"{CKAN_BASE_URL}/api/3/action/organization_list"
    response = requests.get(url, headers=CKAN_HEADERS)
    if response.ok:
        return response.json()["result"]
    return []


def create_organization(name, title):
    url = f"{CKAN_BASE_URL}/api/3/action/organization_create"
    data = {"name": name, "title": title}
    response = requests.post(url, headers=CKAN_HEADERS, json=data)
    return response.ok


def ensure_organization_exists(name, title):
    orgs = get_organization_list()
    if name in orgs:
        return name
    return name if create_organization(name, title) else None
