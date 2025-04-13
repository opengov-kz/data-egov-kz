import os
import csv
from datetime import datetime
from utils.ckan_client import CKANClient
from utils.ckan_utils import clean_keywords, generate_valid_ckan_id
from utils.helpers import load_metadata_reference_files, normalize_url

DATASETS_PATH = "extracted_datasets"
DETAILS_CSV_PATH = "tools/details.csv"


def load_details_reference():
    details_ref = {}
    if os.path.exists(DETAILS_CSV_PATH):
        with open(DETAILS_CSV_PATH, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                if url := row.get('Data Link'):
                    details_ref[normalize_url(url)] = {
                        'version_description': row.get('Version Description', ''),
                        'version_keywords': row.get('Keywords', '')
                    }
    return details_ref


def extract_csv_metadata(file_path, org_name):
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            first_row = next(reader, None)
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            source_url = first_row.get('source_url', '') if first_row else ''

            metadata = {
                'base_name': base_name,
                'source_url': source_url,
                'version_description': f"Данные: {base_name}",
                'version_keywords': ['government-data'],
                'metadata_source': 'defaults'
            }

            # Check details.csv first
            if source_url and (details := load_details_reference().get(normalize_url(source_url))):
                metadata.update({
                    'version_description': details.get('version_description', metadata['version_description']),
                    'version_keywords': clean_keywords(details.get('version_keywords', '')),
                    'metadata_source': 'details_csv'
                })

            # Fallback to CSV values
            if metadata['metadata_source'] == 'defaults' and first_row:
                if first_row.get('version_description'):
                    metadata['version_description'] = first_row['version_description']
                    metadata['metadata_source'] = 'csv_file'
                if first_row.get('version_keywords'):
                    metadata['version_keywords'] = clean_keywords(first_row['version_keywords'])

            print(f"  🔍 Metadata: {metadata['version_description'][:60]}...")
            print(f"  📌 Keywords: {metadata['version_keywords'][:3]}")
            return metadata

    except Exception as e:
        print(f"❌ Metadata error: {str(e)}")
        return {
            'base_name': os.path.splitext(os.path.basename(file_path))[0],
            'version_description': f"Данные: {os.path.splitext(os.path.basename(file_path))[0]}",
            'version_keywords': ['government-data']
        }


def process_organization(client, org_name):
    org_path = os.path.join(DATASETS_PATH, org_name)
    if not os.path.isdir(org_path):
        return False

    org_id = client.get_or_create_organization(org_name)
    if not org_id:
        return False

    for file in [f for f in os.listdir(org_path) if f.endswith(".csv")]:
        file_path = os.path.join(org_path, file)
        print(f"\n📄 Processing: {file}")

        metadata = extract_csv_metadata(file_path, org_name)
        dataset_id = generate_valid_ckan_id(file_path)

        if verified_id := client.create_dataset(
                name=dataset_id,
                title=metadata['base_name'],
                owner_org=org_id,
                description=metadata['version_description'],
                tags=metadata['version_keywords']
        ):
            client.upload_resource(
                dataset_id=verified_id,
                file_path=file_path,
                file_name=file
            )

    return True


def main():
    client = CKANClient()
    print("\n🚀 CKAN Dataset Upload Processor")

    orgs = [d for d in os.listdir(DATASETS_PATH) if os.path.isdir(os.path.join(DATASETS_PATH, d))]
    if not orgs:
        print("❌ No organizations found")
        return

    for i, org in enumerate(orgs, 1):
        print(f"{i}. {org}")
    selection = input("\nSelect organizations (comma-separated): ")

    selected_orgs = [orgs[int(num) - 1] for num in selection.split(',') if num.isdigit() and 0 < int(num) <= len(orgs)]
    for org in selected_orgs:
        print(f"\n=== Processing: {org} ===")
        process_organization(client, org)


if __name__ == '__main__':
    main()