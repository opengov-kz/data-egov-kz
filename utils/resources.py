import os
import requests
from config import CKAN_BASE_URL, CKAN_HEADERS


def upload_or_update_resource(dataset_id, file_path, existing_resources):
    file_name = os.path.basename(file_path)
    with open(file_path, "rb") as f:
        files = {"upload": (file_name, f)}
        data = {
            "package_id": dataset_id,
            "name": file_name
        }

        if file_name in existing_resources:
            data["id"] = existing_resources[file_name]
            url = f"{CKAN_BASE_URL}/api/3/action/resource_update"
        else:
            url = f"{CKAN_BASE_URL}/api/3/action/resource_create"

        response = requests.post(url, headers=CKAN_HEADERS, data=data, files=files)
        return response.ok
