# api_utils.py
import requests
import logging
import re
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from datetime import datetime
from config import API_KEY, HEADERS


def sanitize_filename(name):
    """Create safe filenames from version names."""
    if not name or not isinstance(name, str):
        return "unnamed_dataset"

    name = re.sub(r'[^\w\-_\. ]', '', name.strip())[:100]
    return name.replace(' ', '_')


def is_valid_response(data):
    """Check if the API response contains actual data."""
    if data is None:
        return False

    if isinstance(data, dict):

        if 'error' in data or 'status' in data and data['status'] != 'success':
            return False
        return len(data) > sum(1 for k in data if k.startswith(('version_', 'api_', 'extraction_')))

    if isinstance(data, list):
        return len(data) > 0

    return True


def get_nested_value(data, *keys):
    """Safely get nested value from dict or list."""
    for key in keys:
        try:
            if isinstance(data, dict):
                data = data.get(key)
            elif isinstance(data, list) and isinstance(key, int) and 0 <= key < len(data):
                data = data[key]
            else:
                return None
        except (AttributeError, TypeError, IndexError):
            return None
    return data


def normalize_api_url(url):
    """Normalize API URL with improved validation."""
    try:
        if not url or not isinstance(url, str) or not url.startswith(('http://', 'https://')):
            logging.warning(f"Invalid URL format: {url}")
            return None

        parsed = urlparse(url)

        if not parsed.netloc.endswith(('data.egov.kz', 'egov.kz')):
            logging.warning(f"Unsupported domain: {parsed.netloc}")
            return None

        query_params = parse_qs(parsed.query)
        query_params['apiKey'] = [API_KEY]  # Force our API key

        normalized_url = urlunparse((
            'https',  # Force HTTPS
            'data.egov.kz',  # Standard domain
            parsed.path,
            parsed.params,
            urlencode(query_params, doseq=True),
            parsed.fragment
        ))

        return normalized_url
    except Exception as e:
        logging.error(f"URL normalization failed: {str(e)}")
        return None


def fetch_api_data(url):
    """Fetch API data with robust empty response handling."""
    normalized_url = normalize_api_url(url)
    if not normalized_url:
        return {
            'status': 'error',
            'error': 'Invalid URL',
            'source_url': url,
            'is_empty': True
        }

    try:
        response = requests.get(
            normalized_url,
            headers=HEADERS,
            timeout=15,
            verify=True
        )

        if response.status_code != 200:
            error_msg = f"HTTP {response.status_code}"
            if response.text:
                try:
                    error_data = response.json()
                    error_msg = get_nested_value(error_data, 'message') or get_nested_value(error_data,
                                                                                            'error') or error_msg
                except ValueError:
                    error_msg = f"{error_msg}: {response.text[:200]}"

            return {
                'status': 'error',
                'error': error_msg,
                'status_code': response.status_code,
                'source_url': url,
                'normalized_url': normalized_url,
                'is_empty': response.status_code == 404  # Mark 404s as empty
            }

        try:
            data = response.json()
        except ValueError as e:
            return {
                'status': 'error',
                'error': f'Invalid JSON: {str(e)}',
                'source_url': url,
                'normalized_url': normalized_url,
                'is_empty': True
            }

        if not is_valid_response(data):
            return {
                'status': 'error',
                'error': 'Empty or invalid response data',
                'source_url': url,
                'normalized_url': normalized_url,
                'is_empty': True
            }

        version_name = sanitize_filename(
            response.headers.get('X-Version-Name') or
            get_nested_value(data, 'version_name') or
            get_nested_value(data, 'name') or
            url.split('/')[-1].split('?')[0]
        )

        metadata = {
            'version_name': version_name,
            'version_description':
                response.headers.get('X-Version-Description') or
                get_nested_value(data, 'version_description') or
                get_nested_value(data, 'description') or '',
            'version_keywords':
                response.headers.get('X-Version-Keywords', '').split(',') or
                get_nested_value(data, 'version_keywords', []) or
                get_nested_value(data, 'keywords', []),
            'extraction_date': datetime.now().isoformat(),
            'api_endpoint': normalized_url,
            'source_url': url
        }

        if isinstance(data, dict):
            data.update(metadata)
        elif isinstance(data, list) and data and isinstance(data[0], dict):
            for item in data:
                item.update(metadata)

        return {
            'status': 'success',
            'data': data,
            'source_url': url,
            'normalized_url': normalized_url,
            'version_name': version_name,
            'is_empty': False
        }

    except requests.exceptions.RequestException as e:
        return {
            'status': 'error',
            'error': f'Request failed: {str(e)}',
            'source_url': url,
            'normalized_url': normalized_url,
            'is_empty': True
        }