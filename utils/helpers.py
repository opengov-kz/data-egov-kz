import os
from slugify import slugify
import re
from unidecode import unidecode
import csv


def is_valid_russian(text):
    """Check if text contains valid Cyrillic characters"""
    return bool(re.search('[а-яА-ЯёЁ]', str(text)))

def normalize_url(url):
    """Normalize URLs for consistent matching"""
    if not url:
        return ""
    return (url.strip()
            .lower()
            .replace(" ", "%20")
            .replace("https://", "")
            .replace("http://", "")
            .split("?")[0]  # Remove query parameters
            .rstrip("/"))


def load_metadata_reference_files():
    """Load all reference files with aggressive URL matching"""
    metadata = {}
    ref_files = {
        'CGO': 'data/byCGO.csv',
        'MIO': 'data/byMIO.csv',
        'QuasiOrg': 'data/byQuasiOrg.csv'
    }

    for org_type, file_path in ref_files.items():
        if not os.path.exists(file_path):
            print(f"⚠️ Metadata file not found: {file_path}")
            continue

        try:
            # Try multiple encodings
            for encoding in ['utf-8-sig', 'windows-1251', 'cp1251', 'utf-16']:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        reader = csv.DictReader(f)
                        for i, row in enumerate(reader, 1):
                            source_url = row.get('Data Link', '').strip()
                            if not source_url:
                                continue

                            norm_url = normalize_url(source_url)
                            metadata[norm_url] = {
                                'version_description': row.get('version_description', '').strip(),
                                'version_keywords': [
                                    kw.strip()
                                    for kw in row.get('version_keywords', '').split(',')
                                    if kw.strip()
                                ],
                                'source_row': f"{file_path}:{i}"
                            }
                    break  # Successfully read with this encoding
                except UnicodeDecodeError:
                    continue

        except Exception as e:
            print(f"❌ Error reading {file_path}: {str(e)}")

    print(f"✅ Loaded metadata for {len(metadata)} URLs from reference files")
    return metadata

def get_dataset_folders(base_path):
    for folder_name in os.listdir(base_path):
        folder_path = os.path.join(base_path, folder_name)
        if os.path.isdir(folder_path):
            yield folder_name, folder_path


def make_slug(name):
    return slugify(name)


def slugify(name):
    name = name.strip().lower()
    name = re.sub(r'[^a-z0-9]+', '-', name)
    return name.strip('-')


def generate_ckan_id(filename, max_length=90):
    """
    Generate a valid CKAN ID from filename with:
    - ASCII characters only
    - Lowercase
    - Hyphens instead of spaces/special chars
    - Limited length
    """
    # Remove extension and convert to ASCII
    base = os.path.splitext(os.path.basename(filename))[0]
    ascii_name = unidecode(base)

    # Replace special chars and spaces with hyphens
    clean = re.sub(r'[^a-zA-Z0-9-]', '-', ascii_name)

    # Remove consecutive hyphens and trim
    clean = re.sub(r'-+', '-', clean).strip('-')

    # Convert to lowercase
    clean = clean.lower()

    # Shorten if needed while preserving word boundaries
    if len(clean) > max_length:
        clean = clean[:max_length].rsplit('-', 1)[0]

    # Ensure we have a valid ID
    if not clean or len(clean) < 2:
        clean = f"dataset-{abs(hash(filename)) % 10000:04d}"

    return clean
#
# def get_organization_metadata(org_name):
#     """Get organization metadata from data/organization.csv files"""
#     org_files = {
#         'CGO': 'byCGO.csv',
#         'MIO': 'byMIO.csv',
#         'QuasiOrg': 'byQuasiOrg.csv'
#     }
#
#     org_file = org_files.get(org_name)
#     if not org_file:
#         return None, None
#
#     org_metadata_path = os.path.join('data', org_file)
#     if not os.path.exists(org_metadata_path):
#         return None, None
#
#     try:
#         with open(org_metadata_path, 'r', encoding='utf-8-sig') as f:
#             reader = csv.DictReader(f)
#             first_row = next(reader, None)
#             if not first_row:
#                 return None, None
#
#             description = first_row.get('version_description', '')
#             keywords = first_row.get('version_keywords', '')
#
#             # Process keywords
#             if keywords:
#                 try:
#                     if keywords.startswith(('{', '[')):
#                         keywords = json.loads(keywords)
#                     else:
#                         keywords = [kw.strip() for kw in keywords.split(',') if kw.strip()]
#                 except (json.JSONDecodeError, AttributeError):
#                     keywords = []
#             else:
#                 keywords = []
#
#             return description, keywords
#     except Exception as e:
#         print(f"Error reading organization metadata: {e}")
#         return None, None
