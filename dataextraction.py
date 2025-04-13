import os
import csv
import json
from unidecode import unidecode
import re

# Configuration
DATA_PATH = "data"  # Path to your data directory
OUTPUT_FILE = "metadata_export.json"  # Output file name


def clean_keywords(keywords):
    """Ensure keywords are properly formatted"""
    if not keywords:
        return []

    if isinstance(keywords, str):
        keywords = re.sub(r'[\[\]\'\"]', '', keywords)
        return [kw.strip() for kw in keywords.split(',') if kw.strip()]
    elif isinstance(keywords, list):
        return [str(kw).strip() for kw in keywords if kw]
    return []


def extract_metadata_from_csv(file_path):
    """Extract metadata from a single CSV file"""
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            first_row = next(reader, None)

            version_name = os.path.splitext(os.path.basename(file_path))[0]

            # Default values
            metadata = {
                'version_name': version_name,
                'version_description': f"Данные: {version_name}",
                'version_keywords': [],
                'source_file': os.path.basename(file_path)
            }

            if first_row:
                if 'version_description' in first_row and first_row['version_description'].strip():
                    metadata['version_description'] = first_row['version_description'].strip()

                if 'version_keywords' in first_row and first_row['version_keywords'].strip():
                    metadata['version_keywords'] = clean_keywords(first_row['version_keywords'])

            return metadata

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return {
            'version_name': os.path.splitext(os.path.basename(file_path))[0],
            'version_description': f"Данные: {os.path.splitext(os.path.basename(file_path))[0]}",
            'version_keywords': [],
            'source_file': os.path.basename(file_path)
        }


def process_all_files():
    """Process all CSV files in the data directory"""
    all_metadata = {}

    target_files = ['byMIO.csv', 'byCGO.csv', 'byQuasiOrg.csv']

    for file_name in target_files:
        file_path = os.path.join(DATA_PATH, file_name)
        if not os.path.exists(file_path):
            print(f"Warning: File not found - {file_path}")
            continue

        print(f"Processing file: {file_name}")

        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'Data Link' not in row:
                        continue

                    version_name = os.path.basename(row['Data Link']).split('/')[-1]
                    if '.' in version_name:
                        version_name = version_name.split('.')[0]

                    metadata = {
                        'version_name': version_name,
                        'version_description': row.get('version_description', f"Данные: {version_name}"),
                        'version_keywords': clean_keywords(row.get('version_keywords', '')),
                        'source_file': file_name,
                        'Data Link': row['Data Link']
                    }

                    all_metadata[version_name] = metadata

        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")

    return all_metadata


def save_metadata_to_file(metadata, output_file):
    """Save metadata to JSON file"""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print(f"\nMetadata saved to {output_file}")


def main():
    if not os.path.exists(DATA_PATH):
        print(f"Error: Data directory not found: {DATA_PATH}")
        return

    print("Starting metadata extraction...")
    metadata = process_all_files()
    save_metadata_to_file(metadata, OUTPUT_FILE)

    # Print summary
    print("\nExtraction complete!")
    print(f"Total datasets processed: {len(metadata)}")
    print(f"Output file: {os.path.abspath(OUTPUT_FILE)}")


if __name__ == '__main__':
    main()