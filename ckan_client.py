
# ckan_client.py
import os
import requests
from utils.helpers import slugify
from config import OPENGOV_API_KEY, CKAN_BASE_URL

API_HEADERS = {
    'Authorization': OPENGOV_API_KEY
}

class CKANClient:
    def __init__(self):
        self.base_url = CKAN_BASE_URL

    def get_or_create_organization(self, title):
        org_id = slugify(title)
        res = requests.get(f"{self.base_url}/api/3/action/organization_show?id={org_id}", headers=API_HEADERS)
        if res.status_code == 200:
            return res.json()['result']['id']
        elif res.status_code == 404:
            payload = {
                "name": org_id,
                "title": title
            }
            res = requests.post(f"{self.base_url}/api/3/action/organization_create", json=payload, headers=API_HEADERS)
            if res.status_code == 200:
                return res.json()['result']['id']
            else:
                print(f"[ERROR] Failed to create organization: {title}")
                print(f"→ Status: {res.status_code}\n→ Response: {res.text}")
        return None

    def dataset_exists(self, dataset_id):
        res = requests.get(f"{self.base_url}/api/3/action/package_show?id={dataset_id}", headers=API_HEADERS)
        return res.status_code == 200

    def create_dataset(self, dataset_id, title, owner_org):
        payload = {
            "name": dataset_id,
            "title": title,
            "owner_org": owner_org
        }
        res = requests.post(f"{self.base_url}/api/3/action/package_create", json=payload, headers=API_HEADERS)
        if res.status_code != 200:
            print(f"[ERROR] Failed to create dataset: {dataset_id}")
            print(f"→ Status: {res.status_code}\n→ Response: {res.text}")
        return res.status_code == 200

    def resource_exists(self, dataset_id, filename):
        res = requests.get(f"{self.base_url}/api/3/action/package_show?id={dataset_id}", headers=API_HEADERS)
        if res.status_code != 200:
            return None
        resources = res.json()['result']['resources']
        for resource in resources:
            if resource['name'].lower() == filename.lower():
                return resource['id']
        return None

    def create_or_update_resource(self, dataset_id, filename, file_path):
        resource_id = self.resource_exists(dataset_id, filename)
        with open(file_path, 'rb') as f:
            files = {
                'upload': (filename, f, 'text/csv')
            }
            data = {
                'package_id': dataset_id,
                'name': filename
            }
            if resource_id:
                data['id'] = resource_id
                res = requests.post(f"{self.base_url}/api/3/action/resource_update", headers=API_HEADERS, data=data, files=files)
            else:
                res = requests.post(f"{self.base_url}/api/3/action/resource_create", headers=API_HEADERS, data=data, files=files)

        if res.status_code == 200:
            action = "updated" if resource_id else "created"
            print(f"  ✅ Resource {action}: {filename}")
        else:
            print(f"  ❌ Failed to upload resource: {filename}")
            print(f"    → Status: {res.status_code}\n    → Response: {res.text}")
