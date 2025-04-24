import os
from slugify import slugify
import re
from unidecode import unidecode

def is_valid_russian(text):
    return bool(re.search('[а-яА-ЯёЁ]', str(text)))

def normalize_url(url):
    if not url:
        return ""
    return (url.strip()
            .lower()
            .replace(" ", "%20")
            .replace("https://", "")
            .replace("http://", "")
            .split("?")[0]  # Remove query parameters
            .rstrip("/"))

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

    base = os.path.splitext(os.path.basename(filename))[0]
    ascii_name = unidecode(base)

    clean = re.sub(r'[^a-zA-Z0-9-]', '-', ascii_name)

    clean = re.sub(r'-+', '-', clean).strip('-')

    clean = clean.lower()

    if len(clean) > max_length:
        clean = clean[:max_length].rsplit('-', 1)[0]

    if not clean or len(clean) < 2:
        clean = f"dataset-{abs(hash(filename)) % 10000:04d}"

    return clean
