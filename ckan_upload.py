import os
import requests
from slugify import slugify

CKAN_BASE_URL = "https://data.opengov.kz"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJBeTdFMnRzNDhxZVhyRWtNbFBwUXpHRUU4ZFpyNkh2cFUzZnhGREJSZ0NNIiwiaWF0IjoxNzQzOTgyNzY2fQ.JrmmLm-Fkh1TEfGCH7Fy8VHHlLWoJE8GStk-O947xY4"

HEADERS = {
    "Authorization": API_KEY
}

# Change if you want a different organization name
ORGANIZATION_NAME = "smart_services_integration"
ORGANIZATION_TITLE = "Smart Services Integration"

def get_organization_list():
    response = requests.get(f"{CKAN_BASE_URL}/api/3/action/organization_list", headers=HEADERS)
    if response.status_code == 200:
        return response.json()["result"]
    else:
        print("[ERROR] Could not fetch organization list.")
        print(response.text)
        return []

def create_organization(name, title):
    data = {
        "name": name,
        "title": title
    }
    response = requests.post(f"{CKAN_BASE_URL}/api/3/action/organization_create", headers=HEADERS, json=data)
    if response.status_code == 200:
        print(f"✔ Created new organization: {name}")
        return name
    else:
        print("[ERROR] Failed to create organization.")
        print(response.text)
        return None

def ensure_organization_exists(name, title):
    orgs = get_organization_list()
    if name in orgs:
        print(f"✔ Organization already exists: {name}")
        return name
    else:
        return create_organization(name, title)

def create_dataset(dataset_id, title, owner_org):
    data = {
        "name": dataset_id,
        "title": title,
        "owner_org": owner_org
    }

    response = requests.post(f"{CKAN_BASE_URL}/api/3/action/package_create", headers=HEADERS, json=data)

    if response.status_code == 200:
        print(f"✔ Created dataset: {dataset_id}")
        return True
    else:
        print(f"[ERROR] Failed to create dataset {dataset_id}")
        print(f"→ Status: {response.status_code}")
        print(f"→ Response: {response.text}")
        return False

def upload_resource(dataset_id, file_path):
    file_name = os.path.basename(file_path)

    data = {
        "package_id": dataset_id,
        "name": file_name
    }

    with open(file_path, "rb") as f:
        files = {"upload": (file_name, f)}
        response = requests.post(f"{CKAN_BASE_URL}/api/3/action/resource_create", headers=HEADERS, data=data, files=files)

    if response.status_code == 200:
        print(f"  ↳ Uploaded resource: {file_name}")
    else:
        print(f"[ERROR] Failed to upload resource: {file_name}")
        print(f"→ Status: {response.status_code}")
        print(f"→ Response: {response.text}")

def main():
    print("Step 1: Ensuring organization exists...")
    owner_org = ensure_organization_exists(ORGANIZATION_NAME, ORGANIZATION_TITLE)
    if not owner_org:
        print("[FATAL] Cannot proceed without valid organization.")
        return

    print("\nStep 2: Uploading datasets and resources...\n")

    datasets_base = "results/datasets"
    for folder_name in os.listdir(datasets_base):
        folder_path = os.path.join(datasets_base, folder_name)
        if os.path.isdir(folder_path):
            dataset_id = slugify(folder_name)
            title = folder_name.replace("_", " ").title()

            if create_dataset(dataset_id, title, owner_org):
                for file in os.listdir(folder_path):
                    if file.endswith(".csv"):
                        file_path = os.path.join(folder_path, file)
                        upload_resource(dataset_id, file_path)

if __name__ == "__main__":
    main()
