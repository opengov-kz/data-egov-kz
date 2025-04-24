import os
import csv
import json

from IPython.core.release import author

from utils.ckan_client import CKANClient
from utils.ckan_utils import clean_keywords, generate_valid_ckan_id
from utils.helpers import normalize_url

DATASETS_PATH = "extracted_datasets"


def load_metadata_from_json(agency_id, dataset_name):
    import glob

    metadata_path = os.path.join("metadata_json", agency_id)
    if not os.path.exists(metadata_path):
        return None

    normalized_name = dataset_name.lower()
    for file in glob.glob(os.path.join(metadata_path, "*.json")):
        if normalized_name in os.path.basename(file).lower():
            with open(file, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    return {
                        "title": data.get("title") or dataset_name,
                        "description": data.get("description", ""),
                        "keywords": data.get("keywords") or [],
                        "owner": data.get("version_owner") or agency_id
                    }
                except json.JSONDecodeError:
                    print(f"❌ Could not decode JSON in {file}")
    return None


def load_json_metadata(org_name, dataset_name):
    json_path = os.path.join("results/metadata", f"{dataset_name}.json")
    if not os.path.exists(json_path):
        print(f"⚠️ Metadata JSON not found: {json_path}")
        return {}
    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def extract_csv_metadata(file_path, org_name):
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    metadata_json = load_json_metadata(org_name, base_name)

    # Default fallback
    url = ""
    meta_link = ""

    org_map = {
        "local_executive": "data/byMIO.csv",
        "central_government": "data/byCGO.csv",
        "quasi_government": "data/byQuasiOrg.csv",
    }

    normalized_org = org_name.strip().lower().replace(" ", "_")
    metadata_csv_path = org_map.get(normalized_org)

    if metadata_csv_path and os.path.exists(metadata_csv_path):
        try:
            with open(metadata_csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    version_name = row.get('Version Name', '').strip().lower()
                    if version_name == base_name.lower():
                        url = row.get('Data Url', '')
                        meta_link = row.get('Meta Link', '')
                        break
                else:
                    print(f"⚠️ No matching 'Version Name' found for '{base_name}' in {metadata_csv_path}")
        except Exception as e:
            print(f"⚠️ Error reading metadata CSV ({metadata_csv_path}): {e}")
    else:
        print(f"⚠️ Metadata CSV not found for org: {org_name}")

    metadata = {
        'base_name': base_name,
        'url': url,
        'meta_link': meta_link,
        'version_description': metadata_json.get('descriptionRu', '') + "\n" + metadata_json.get('descriptionKk', ''),
        'version_keywords': clean_keywords(metadata_json.get('keywords', '')),
        'organization': metadata_json.get('owner', {}).get('fullnameRu', org_name),
        'author_': metadata_json.get('responsible', {}).get('fullnameRu', ''),
        'author_email': metadata_json.get('responsible', {}).get('email', ''),
        'metadata_source': 'json_metadata'
    }

    print(f"  📝 Description: {metadata['version_description'][:60]}...")
    print(f"  👤 Author: {metadata['author_']} ({metadata['author_email']})")
    print(f"  🔗 Source: {metadata['url']}")
    print(f"  🧷 Meta Link: {metadata['meta_link']}")
    print(f"  📌 Tags: {metadata['version_keywords'][:3]}")

    return metadata


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
                source_url=metadata['url'],
                title=metadata['base_name'],
                author_=metadata['author_'],
                authoremail=metadata['author_email'],
                owner_org=org_id,
                description=metadata['version_description'],
                tags=metadata['version_keywords']
        ):
            client.upload_resource(
                dataset_id=verified_id,
                file_path=file_path,
                file_name=file,
                description=f"Источник: {metadata['url']} \nАвтор: {metadata['author_']} ({metadata['author_email']})"
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