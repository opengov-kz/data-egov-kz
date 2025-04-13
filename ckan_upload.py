#ckan_upload
import os
import csv
import json
import re
from datetime import datetime, time
from utils.helpers import load_metadata_reference_files, normalize_url
from unidecode import unidecode

from ckan_client import CKANClient

DATASETS_PATH = "extracted_datasets"


def generate_valid_ckan_id(filename):
    """Generate a CKAN-compatible ID from filename"""
    base = os.path.splitext(os.path.basename(filename))[0]

    # Convert to ASCII and lowercase
    ascii_name = unidecode(base).lower()

    # Remove special characters and replace with single hyphen
    clean = re.sub(r'[^a-z0-9]+', '-', ascii_name).strip('-')

    # Extract numeric prefix if exists (like "117._")
    num_prefix = ''
    num_match = re.match(r'^(\d+)', clean)
    if num_match:
        num_prefix = f"{num_match.group(1)}-"
        clean = clean[len(num_match.group(1)):]

    # Shorten long names by keeping first 3 words
    if len(clean) > 50:
        parts = clean.split('-')
        if len(parts) > 3:
            clean = '-'.join(parts[:3])

    # Combine with org prefix and ensure max length
    org_prefix = os.path.dirname(filename).split(os.sep)[-1][:3].lower()
    clean = f"{org_prefix}-{num_prefix}{clean}"[:100]

    # Final validation
    if len(clean) < 3:
        clean = f"ds-{hash(filename) % 10000:04d}"

    return clean

METADATA_REF = load_metadata_reference_files()


def get_metadata_from_reference(source_url, org_type):
    """Extract metadata from reference files"""
    org_type = org_type.replace('_', '')  # Convert Local_Executive to LocalExecutive
    for possible_org in [org_type, 'CGO', 'MIO', 'QuasiOrg']:
        if possible_org in METADATA_REF and source_url in METADATA_REF[possible_org]:
            return METADATA_REF[possible_org][source_url]
    return None


def clean_keywords(keywords):
    """Ensure keywords are properly formatted and valid for CKAN"""
    if not keywords:
        return []

    # Handle string cases
    if isinstance(keywords, str):
        # Remove any JSON-like brackets and quotes
        keywords = re.sub(r'[\[\]\'\"]', '', keywords)
        # Split by comma and clean
        keywords = [kw.strip() for kw in keywords.split(',') if kw.strip()]

    # Handle list cases
    elif isinstance(keywords, list):
        keywords = [str(kw).strip() for kw in keywords if kw]

    # Final validation - remove empty and invalid tags
    valid_keywords = []
    for kw in keywords:
        if not kw:
            continue
        # Remove any remaining special characters
        clean_kw = re.sub(r'[^a-zA-Z0-9 \-_\.]', '', kw)
        if clean_kw:
            valid_keywords.append(clean_kw[:50])  # Limit keyword length

    return valid_keywords[:30]  # Limit number of tags

def extract_csv_metadata(file_path, org_name):
    """Robust metadata extraction with validation"""
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            first_row = next(reader, None)

            base_name = os.path.splitext(os.path.basename(file_path))[0]
            source_url = first_row.get('source_url', '') if first_row else ''

            # Initialize with defaults
            result = {
                'base_name': base_name,
                'source_url': source_url,
                'version_description': f"Данные: {base_name}",
                'version_keywords': ['government-data'],  # Default keyword
                'metadata_source': 'defaults'
            }

            # 1. Try reference files
            if source_url:
                norm_url = normalize_url(source_url)
                ref_data = None
                for org_type in [org_name.replace('_', ''), 'CGO', 'MIO', 'QuasiOrg']:
                    if org_type in METADATA_REF and norm_url in METADATA_REF[org_type]:
                        ref_data = METADATA_REF[org_type][norm_url]
                        break

                if ref_data:
                    if ref_data.get('version_description'):
                        result['version_description'] = ref_data['version_description']
                        result['metadata_source'] = 'reference_file'

                    if ref_data.get('version_keywords'):
                        keywords = clean_keywords(ref_data['version_keywords'])
                        if keywords:  # Only use if we got valid keywords
                            result['version_keywords'] = keywords

            # 2. Fallback to CSV values if no reference data
            if result['metadata_source'] == 'defaults' and first_row:
                if first_row.get('version_description'):
                    result['version_description'] = first_row['version_description']
                    result['metadata_source'] = 'csv_file'

                if first_row.get('version_keywords'):
                    keywords = clean_keywords(first_row['version_keywords'])
                    if keywords:
                        result['version_keywords'] = keywords

            # Final validation
            if not result['version_description'].strip():
                result['version_description'] = f"Данные: {base_name}"

            # Ensure we have at least one valid keyword
            if not result['version_keywords']:
                result['version_keywords'] = ['government-data']

            print(f"  🔍 Metadata: {result['version_description'][:60]}...")
            print(f"  📌 Keywords: {result['version_keywords'][:3]} (source: {result['metadata_source']})")

            return {
                **result,
                'headers': reader.fieldnames or [],
                'created': datetime.now().isoformat()
            }

    except Exception as e:
        print(f"❌ Metadata error: {str(e)}")
        return {
            'base_name': os.path.splitext(os.path.basename(file_path))[0],
            'source_url': '',
            'version_description': f"Данные: {os.path.splitext(os.path.basename(file_path))[0]}",
            'version_keywords': ['government-data'],  # Default fallback
            'headers': [],
            'created': datetime.now().isoformat(),
            'metadata_source': 'error_fallback'
        }

def verify_dataset_metadata(client, dataset_id, expected_description):
    """Confirm metadata was properly saved"""
    dataset = client._make_request('package_show', params={'id': dataset_id})
    if not dataset:
        print(f"❌ Verification failed: Could not fetch dataset {dataset_id}")
        return False

    saved_description = dataset.get('notes', '')
    if saved_description.strip() == expected_description.strip():
        print(f"✅ Verified metadata matches for {dataset_id}")
        return True
    else:
        print(f"⚠️ Metadata mismatch!\nSent: {expected_description[:60]}...\nSaved: {saved_description[:60]}...")
        return False

def select_organizations():
    """Display interactive menu to select organizations"""
    available_orgs = [d for d in os.listdir(DATASETS_PATH)
                      if os.path.isdir(os.path.join(DATASETS_PATH, d))]

    if not available_orgs:
        print("❌ No organizations found in datasets directory!")
        return []

    print("\nAvailable Organizations:")
    for i, org in enumerate(available_orgs, 1):
        print(f"{i}. {org}")
    print(f"{len(available_orgs) + 1}. Process All")
    print(f"{len(available_orgs) + 2}. Exit")

    while True:
        try:
            choice = input("\nSelect organizations (comma-separated numbers or 'all'): ").strip().lower()

            if choice == 'exit':
                return []
            elif choice == 'all':
                return available_orgs

            selected = []
            for num in choice.split(','):
                num = int(num.strip())
                if 1 <= num <= len(available_orgs):
                    selected.append(available_orgs[num - 1])
                elif num == len(available_orgs) + 1:
                    return available_orgs
                elif num == len(available_orgs) + 2:
                    return []
                else:
                    print(f"⚠️ Invalid selection: {num}")

            return selected if selected else available_orgs

        except ValueError:
            print("⚠️ Please enter valid numbers separated by commas")


def process_organization(client, org_name):
    org_path = os.path.join(DATASETS_PATH, org_name)
    if not os.path.isdir(org_path):
        print(f"❌ Organization directory not found: {org_name}")
        return False

    print(f"\n📁 Processing organization: {org_name}")

    org_id = client.get_or_create_organization(org_name)
    if not org_id:
        print(f"❌ Failed to get/create organization: {org_name}")
        return False

    processed = 0
    csv_files = [f for f in os.listdir(org_path) if f.endswith(".csv")]

    if not csv_files:
        print(f"⚠️ No CSV files found in organization directory: {org_name}")
        return False

    for file in sorted(csv_files):
        file_path = os.path.join(org_path, file)
        print(f"\n📄 Processing: {file}")

        try:
            dataset_id = generate_valid_ckan_id(file)
            metadata = extract_csv_metadata(file_path, org_name)

            # Validate metadata
            if not metadata.get('version_description'):
                metadata['version_description'] = f"Данные: {metadata['base_name']}"

            # Ensure keywords are properly formatted
            if not isinstance(metadata['version_keywords'], list):
                metadata['version_keywords'] = []

            # Create dataset
            success = client.create_dataset(
                name=dataset_id,
                title=metadata['base_name'][:100],  # Limit title length
                owner_org=org_id,
                description=metadata['version_description'][:2000],  # Limit description
                tags=metadata['version_keywords']
            )

            if not success:
                print(f"  ❌ Failed to create dataset: {file}")
                continue

            # Upload resource
            resource_success = client.upload_resource(
                dataset_id=dataset_id,
                file_path=file_path,
                file_name=file,
                description=f"CSV resource for {metadata['base_name'][:200]}"
            )

            if resource_success:
                processed += 1
                print(f"  ✅ Successfully processed: {file}")
            else:
                print(f"  ❌ Failed to upload resource: {file}")

        except Exception as e:
            print(f"  ⚠️ Unexpected error processing {file}: {str(e)}")
            continue

    return processed > 0

def main():
    client = CKANClient()
    print("\n🚀 CKAN Dataset Upload Processor")
    print("-----------------------------")

    if not os.path.exists(DATASETS_PATH):
        print(f"❌ Dataset directory not found: {DATASETS_PATH}")
        return

    # Load metadata reference files
    global METADATA_REF
    METADATA_REF = load_metadata_reference_files()

    # Select organizations to process
    orgs_to_process = select_organizations()
    if not orgs_to_process:
        print("\nExiting...")
        return

    print(f"\nSelected organizations: {', '.join(orgs_to_process)}")
    confirm = input("Proceed with upload? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Aborted by user")
        return

    # Process selected organizations
    success_count = 0
    for org in orgs_to_process:
        print(f"\n=== Processing organization: {org} ===")
        success = process_organization(client, org)
        if success:
            success_count += 1
            print(f"✔️ Completed: {org}")
        else:
            print(f"⚠️ Finished with errors: {org}")

    print(f"\nProcessing complete. Successfully processed {success_count}/{len(orgs_to_process)} organizations")


if __name__ == '__main__':
    main()