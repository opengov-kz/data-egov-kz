#cakn_utils.py:
import os
import re
from unidecode import unidecode


def clean_keywords(keywords):
    """Clean and format keywords for CKAN with Cyrillic support"""
    if not keywords:
        return ['government-data']

    if isinstance(keywords, str):
        keywords = re.sub(r'[\[\]\'\"]', '', keywords)
        keywords = [kw.strip() for kw in keywords.split(',') if kw.strip()]
    elif isinstance(keywords, list):
        keywords = [str(kw).strip() for kw in keywords if kw]

    valid_keywords = []
    for kw in keywords:
        clean_kw = re.sub(r'[^\w \-_\.]', '', kw, flags=re.UNICODE).strip()
        if clean_kw:
            valid_keywords.append(clean_kw[:50])

    return valid_keywords[:30] or ['government-data']


def generate_valid_ckan_id(filename):
    """Generate consistent CKAN IDs from filenames"""
    base = os.path.splitext(os.path.basename(filename))[0]
    ascii_name = unidecode(base).lower()
    clean = re.sub(r'[^a-z0-9\-]+', '-', ascii_name).strip('-')

    num_prefix = ''
    if match := re.match(r'^(\d+)[_\-]', base):
        num_prefix = f"{match.group(1)}-"

    org_prefix = os.path.dirname(filename).split(os.sep)[-1][:3].lower()
    clean = f"{org_prefix}-{num_prefix}{clean}"[:80]

    return clean or f"ds-{hash(filename) % 10000:04d}"