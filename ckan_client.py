#ckan_client.py :
import os
import time

import requests
import json
from unidecode import unidecode
import re
from config import OPENGOV_API_KEY, CKAN_BASE_URL
from utils.helpers import slugify


class CKANClient:
    def __init__(self):
        self.base_url = CKAN_BASE_URL.rstrip('/')
        self.api_headers = {
            'Authorization': OPENGOV_API_KEY,
            'Content-Type': 'application/json'
        }
        self.last_error = None  # Initialize error tracking

    def _make_request(self, endpoint, method='get', params=None, json_data=None, files=None):
        """Improved request handler with better error reporting"""
        url = f"{self.base_url}/api/3/action/{endpoint}"
        self.last_error = None

        try:
            if method.lower() == 'get':
                response = requests.get(url, headers=self.api_headers, params=params, timeout=30)
            elif method.lower() == 'post':
                if files:
                    headers = {'Authorization': OPENGOV_API_KEY}
                    response = requests.post(url, headers=headers, data=params, files=files, timeout=60)
                else:
                    response = requests.post(url, headers=self.api_headers, json=json_data, timeout=30)
            else:
                raise ValueError(f"Unsupported method: {method}")

            # Parse response
            response_data = response.json()

            if response.status_code == 200:
                return response_data.get('result')

            # Enhanced error reporting
            error_info = {
                'status': response.status_code,
                'message': response_data.get('error', {}).get('message', 'Unknown error'),
                'full_error': response_data.get('error', {}),
                'request_data': {'endpoint': endpoint, 'params': params, 'json': json_data}
            }
            self.last_error = error_info

            # Log the full error for debugging
            print(f"⛔ CKAN API Error: {json.dumps(error_info, indent=2, ensure_ascii=False)}")
            return None

        except Exception as e:
            error_info = {
                'exception': str(e),
                'type': type(e).__name__,
                'request': {'url': url, 'method': method}
            }
            self.last_error = error_info
            print(f"⛔ Request Exception: {json.dumps(error_info, indent=2)}")
            return None

    def purge_dataset(self, dataset_id):
        """Complete dataset removal with verification"""
        print(f"Attempting to purge dataset: {dataset_id}")
        result = self._make_request('dataset_purge', method='post', json_data={'id': dataset_id})

        if not result:
            print(f"❌ Purge failed for {dataset_id}")
            if self.last_error:
                print(f"Error details: {self.last_error}")
            return False

        # Verify purge was successful
        verify = self._make_request('package_show', params={'id': dataset_id, 'include_deleted': True})
        if not verify:
            print(f"✅ Verified purge of {dataset_id}")
            return True

        print(f"❌ Purge verification failed for {dataset_id}")
        return False

    def create_dataset(self, name, title, owner_org, description, tags):
        """Robust dataset creation with strict validation"""
        # Validate and clean inputs
        name = name.strip().lower()[:100]  # Ensure length limit
        title = title.strip()[:200]  # CKAN typically allows longer titles
        description = description.strip()[:2000]

        # Clean and validate tags
        valid_tags = []
        for tag in tags[:30]:  # Limit number of tags
            if isinstance(tag, str) and tag.strip():
                # Clean tag according to CKAN requirements
                clean_tag = re.sub(r'[^a-zA-Z0-9 \-_\.]', '', tag.strip())[:50]
                if clean_tag:
                    valid_tags.append({'name': clean_tag})

        # If no valid tags, use a default
        if not valid_tags:
            valid_tags = [{'name': 'government-data'}]

        payload = {
            "name": name,
            "title": title,
            "owner_org": owner_org,
            "notes": description,
            "tags": valid_tags,
            "license_id": "cc-by",
            "state": "active",
            "type": "dataset",
            "resources": []
        }

        result = self._make_request('package_create', method='post', json_data=payload)

        if result:
            return True

        if self.last_error:
            print(f"❌ Dataset creation failed: {self.last_error.get('message')}")
            if 'full_error' in self.last_error:
                for field, errors in self.last_error['full_error'].items():
                    if field not in ['__type']:
                        print(f"  - {field}: {', '.join(errors)}")
        return False

    def dataset_exists(self, dataset_id):
        """Check dataset existence with proper state handling"""
        result = self._make_request('package_show',
                                    params={'id': dataset_id, 'include_deleted': True})
        if not result:
            return False

        if result.get('state') == 'deleted':
            print(f"♻️ Found deleted dataset: {dataset_id}")
            if self.purge_dataset(dataset_id):
                return False  # Consider non-existent after purge
            return True  # Still exists if purge failed

        return True

    def check_dataset_state(self, dataset_id):
        """Improved dataset state detection with precise error handling"""
        result = self._make_request('package_show',
                                    params={'id': dataset_id, 'include_deleted': True})

        if not result:
            if self.last_error and self.last_error.get('status') == 404:
                return 'not_found'
            return 'unknown_error'

        return result.get('state', 'exists_unknown_state')

    def handle_existing_dataset(self, dataset_id):
        """Comprehensive dataset existence handling"""
        state = self.check_dataset_state(dataset_id)

        if state == 'not_found':
            return 'create'  # Doesn't exist, safe to create
        elif state == 'active':
            return 'exists_active'
        elif state == 'deleted':
            if self.purge_dataset(dataset_id):
                return 'purged'
            return 'purge_failed'
        else:
            print(f"⚠️ Unknown dataset state: {state}")
            return 'unknown_state'

    def organization_exists(self, org_name):
        """More reliable organization existence check"""
        org_id = slugify(org_name)
        result = self._make_request('organization_show', params={'id': org_id})
        if result:
            # Verify the organization wasn't deleted
            if result.get('state') == 'deleted':
                print(f"⚠️ Organization exists but is deleted: {org_name}")
                return False
            return True
        return False


    def get_or_create_organization(self, org_name):
        """Improved organization handling with state checking"""
        org_id = slugify(org_name)

        # First check if organization exists (including deleted ones)
        result = self._make_request('organization_show', params={'id': org_id, 'include_deleted': True})

        if result:
            if result.get('state') == 'deleted':
                print(f"♻️ Reactivating deleted organization: {org_name}")
                # Undelete the organization
                update_result = self._make_request('organization_patch', method='post',
                                                   json_data={'id': org_id, 'state': 'active'})
                if update_result:
                    return org_id
            else:
                print(f"ℹ️ Organization exists: {org_name}")
                return org_id

        # Create new organization if it doesn't exist
        payload = {
            "name": org_id,
            "title": org_name,
            "description": f"Organization for {org_name}",
            "state": "active"
        }
        result = self._make_request('organization_create', method='post', json_data=payload)
        if result:
            print(f"✅ Created organization: {org_name}")
            return org_id

        print(f"❌ Failed to create organization: {org_name}")
        return None

    def upload_resource(self, dataset_id, file_path, file_name, description=""):
        """Upload a resource only if the dataset exists"""
        if not self.dataset_exists(dataset_id):
            print(f"❌ Dataset does not exist, skipping resource upload: {dataset_id}")
            return False

        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return False

        payload = {
            'package_id': dataset_id,
            'name': file_name,
            'description': description,
            'format': 'CSV',
            'mimetype': 'text/csv'
        }

        with open(file_path, 'rb') as f:
            files = {'upload': (file_name, f, 'text/csv')}
            result = self._make_request('resource_create', method='post', params=payload, files=files)

        if result:
            print(f"✅ Successfully uploaded resource: {file_name}")
            return True
        print(f"❌ Failed to upload resource: {file_name}")
        return False
