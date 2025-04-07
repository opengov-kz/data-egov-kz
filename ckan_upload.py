# ckan_upload.py (Main Entry Point)
import os
from ckan_client import CKANClient
from utils.helpers import slugify

DATASETS_PATH = "results/datasets"

def main():
    client = CKANClient()
    print("\n🚀 Starting CKAN Upload Process...")

    for agency_folder in os.listdir(DATASETS_PATH):
        agency_path = os.path.join(DATASETS_PATH, agency_folder)
        if not os.path.isdir(agency_path):
            continue

        print(f"\n📁 Processing: {agency_folder}")
        org_id = client.get_or_create_organization(agency_folder)
        if not org_id:
            print(f"[ERROR] Could not create or find organization: {agency_folder}")
            continue

        dataset_id = slugify(agency_folder)
        dataset_exists = client.dataset_exists(dataset_id)

        if not dataset_exists:
            success = client.create_dataset(dataset_id, agency_folder, org_id)
            if not success:
                print(f"[ERROR] Could not create dataset: {dataset_id}")
                continue

        for file in os.listdir(agency_path):
            file_path = os.path.join(agency_path, file)
            if file.endswith(".csv"):
                client.create_or_update_resource(dataset_id, file, file_path)

if __name__ == '__main__':
    main()