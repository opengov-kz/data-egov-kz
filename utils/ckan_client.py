#ckan_client.py:
import os
import requests
import json

from IPython.core.release import author, author_email
from torch.fx.experimental.unification.multipledispatch.dispatcher import source

from utils.ckan_utils import clean_keywords, generate_valid_ckan_id
from config import OPENGOV_API_KEY, CKAN_BASE_URL
from utils.helpers import slugify


class CKANClient:
    def __init__(self):
        self.base_url = CKAN_BASE_URL.rstrip('/')
        self.api_headers = {'Authorization': OPENGOV_API_KEY, 'Content-Type': 'application/json'}
        self.last_error = None

    def _make_request(self, endpoint, method='get', params=None, json_data=None, files=None):
        url = f"{self.base_url}/api/3/action/{endpoint}"
        try:
            if method.lower() == 'get':
                response = requests.get(url, headers=self.api_headers, params=params, timeout=30)
            elif method.lower() == 'post':
                if files:
                    response = requests.post(url, headers={'Authorization': OPENGOV_API_KEY},
                                             data=params, files=files, timeout=60)
                else:
                    response = requests.post(url, headers=self.api_headers, json=json_data, timeout=30)
            else:
                raise ValueError(f"Unsupported method: {method}")

            if response.status_code == 200:
                return response.json().get('result')

            self.last_error = {
                'status': response.status_code,
                'message': response.json().get('error', {}).get('message', 'Unknown error'),
                'full_error': response.json().get('error', {})
            }
            return None

        except Exception as e:
            self.last_error = {'exception': str(e)}
            return None

    def create_dataset(self, name, title, owner_org, source_url , author_, authoremail, description, tags):
        ckan_id = generate_valid_ckan_id(f"{owner_org}/{name}.csv")
        if self.dataset_exists(ckan_id):
            print(f"⚠️ Dataset exists: {ckan_id}")
            return ckan_id

        payload = {
            "name": ckan_id,
            "title": title[:200],
            "owner_org": owner_org,
            "url":source_url,
            "author": author_,
            "author_email": authoremail,
            "notes": description[:2000],
            "tags": [{"name": kw} for kw in clean_keywords(tags)[:30]],
            "license_id": "cc-by",
            "state": "active"
        }

        if not self._make_request('package_create', method='post', json_data=payload):
            print(f"❌ Failed to create dataset: {ckan_id}")
            return None
        return ckan_id

    def dataset_exists(self, dataset_id):
        result = self._make_request('package_show', params={'id': dataset_id, 'include_deleted': True})
        return bool(result) and result.get('state') != 'deleted'

    def upload_resource(self, dataset_id, file_path, file_name, description=""):
        if not dataset_id or not os.path.exists(file_path):
            return False

        with open(file_path, 'rb') as f:
            return bool(self._make_request(
                'resource_create',
                method='post',
                params={
                    'package_id': dataset_id,
                    'name': file_name,
                    'description': description[:200],
                    'format': 'CSV'
                },
                files={'upload': (file_name, f, 'text/csv')}
            ))

    def get_or_create_organization(self, org_name):
        org_id = slugify(org_name)
        result = self._make_request('organization_show', params={'id': org_id, 'include_deleted': True})

        if result:
            if result.get('state') == 'deleted':
                self._make_request('organization_patch', method='post',
                                   json_data={'id': org_id, 'state': 'active'})
            return org_id

        return self._make_request('organization_create', method='post', json_data={
            "name": org_id,
            "title": org_name,
            "state": "active"
        }) and org_id