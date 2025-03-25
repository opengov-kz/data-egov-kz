import requests
import logging
from urllib.parse import urlparse, urlunparse
from config import API_KEY, HEADERS
import json


def normalize_api_url(url):

    try:
        parsed = urlparse(url)
        base_domain = 'data.egov.kz'
        valid_schemes = ['https']
        valid_paths = ['/api/v4/', '/proxy/']

        if parsed.scheme not in valid_schemes:
            return None
        if parsed.netloc != base_domain:
            return None
        if not any(parsed.path.startswith(p) for p in valid_paths):
            return None

        query = parsed.query
        if 'apiKey=' not in query:
            query = f"{query}&apiKey={API_KEY}" if query else f"apiKey={API_KEY}"

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
            'source_url': url
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'source_url': url
        }