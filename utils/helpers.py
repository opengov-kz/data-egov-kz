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


import re
import unicodedata


def slugify(text, max_length=100):
    """Convert text to a valid CKAN ID with proper Cyrillic handling"""
    if not text:
        return ""

    # Convert to ASCII (transliterates Cyrillic)
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')

    # Remove special characters and lowercase
    text = re.sub(r'[^\w\s-]', '', text.lower())
    text = re.sub(r'[-\s]+', '_', text).strip('-_')

    # Apply government-specific abbreviations
    abbrev_rules = [
        (r'republic of kazakhstan', 'rk'),
        (r'ministry|ministerstvo', 'min'),
        (r'department|departament', 'dep'),
        (r'agency|agentstvo', 'agent'),
        (r'bureau|byuro', 'bur'),
        (r'committee|komitet', 'com'),
        (r'national|nacionalnyj', 'nat'),
        (r'regulation|regulirovanie', 'reg'),
        (r'development|razvitie', 'dev'),
        (r'financial|finansovyj', 'fin'),
    ]

    for pattern, repl in abbrev_rules:
        text = re.sub(pattern, repl, text)

    # Final length limit
    return text[:max_length]


def transliterate(text):
    """Convert Cyrillic text to Latin characters"""
    translit_map = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd',
        'е': 'e', 'ё': 'yo', 'ж': 'zh', 'з': 'z', 'и': 'i',
        'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n',
        'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't',
        'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch',
        'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '',
        'э': 'e', 'ю': 'yu', 'я': 'ya'
    }

    result = []
    for char in text.lower():
        result.append(translit_map.get(char, char))
    return ''.join(result)

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
