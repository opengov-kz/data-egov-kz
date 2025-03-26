import requests
import logging
from urllib.parse import urlparse, urlunparse
from config import API_KEY, HEADERS


def normalize_api_url(url):
    """Standardize API URLs and ensure proper API key inclusion"""
    try:
        parsed = urlparse(url)
        if parsed.scheme != 'https' or parsed.netloc != 'data.egov.kz':
            return None
        if not any(parsed.path.startswith(p) for p in ['/api/v4/', '/proxy/']):
            return None

        query = f"{parsed.query}&apiKey={API_KEY}" if parsed.query else f"apiKey={API_KEY}"
        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            query,
            parsed.fragment
        ))
    except Exception as e:
        logging.error(f"URL normalization failed: {str(e)}")
        return None


def fetch_api_data(url):
    """Fetch data from API endpoint"""
    normalized_url = normalize_api_url(url)
    if not normalized_url:
        return {'status': 'error', 'error': 'Invalid URL'}

    try:
        response = requests.get(normalized_url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        if response.encoding is None:
            response.encoding = 'utf-8'
        return {
            'status': 'success',
            'data': response.json(),
            'source_url': url,
            'normalized_url': normalized_url
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'source_url': url
        }