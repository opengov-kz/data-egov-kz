#ckan_client.py:
import os
import re
import requests
import json
from IPython.core.release import author, author_email
from torch.fx.experimental.unification.multipledispatch.dispatcher import source

from utils.ckan_utils import clean_keywords, generate_valid_ckan_id
from config import OPENGOV_API_KEY, CKAN_BASE_URL
from utils.helpers import slugify, transliterate


class CKANClient:
    def __init__(self):
        self.base_url = CKAN_BASE_URL.rstrip('/')
        self.api_headers = {'Authorization': OPENGOV_API_KEY, 'Content-Type': 'application/json'}
        self.last_error = None
        self._initialize_parent_organizations()
        # Add this line


    def _post(self, endpoint, json):
        url = f"{self.base_url}/api/3/action/{endpoint}"
        response = requests.post(url, json=json, headers=self.api_headers)
        response.raise_for_status()  # raise exception for HTTP errors
        return response.json()

    def _initialize_parent_organizations(self):
        """Create parent organizations if they don't exist"""
        parent_orgs = [
            "government_ministry",
            "government_agency",
            "government_department",
            "government_bureau",
            "government_committee"
        ]

        for org in parent_orgs:
            if not self._make_request('organization_show', params={'id': org}):
                self._make_request('organization_create', method='post', json_data={
                    "name": org,
                    "title": org.replace('_', ' ').title(),
                    "is_organization": True,
                    "state": "active"
                })
                print(f"✅ Created parent organization: {org}")
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

    def _detect_org_type(self, org_name):
        """Detect the type of government organization based on its name"""
        org_name = org_name.lower()
        if 'министерство' in org_name or 'мин' in org_name:
            return 'ministry'
        elif 'агентство' in org_name or 'агент' in org_name:
            return 'agency'
        elif 'департамент' in org_name or 'деп' in org_name:
            return 'department'
        elif 'бюро' in org_name:
            return 'bureau'
        elif 'комитет' in org_name:
            return 'committee'
        return None

    def _process_government_name(self, full_name):
        """Process long government organization names into standardized formats"""
        abbreviations = {
            'Республики Казахстан': 'РК',
            'Министерство': 'Мин',
            'Департамент': 'Деп',
            'Агентство': 'Агент',
            'Верховного Суда': 'ВС',
            'по обеспечению': '',
            'деятельности': '',
            'Бюро': 'Бюро',
            'Аппарат': 'Апп',
            'национальной': 'нац',
            'статистики': 'стат',
            'стратегическому': 'страт',
            'планированию': 'план',
            'реформам': 'реформ'
        }

        # Apply abbreviations
        for long, short in abbreviations.items():
            full_name = full_name.replace(long, short)

        # Remove special characters and extra spaces
        full_name = re.sub(r'[^\w\s]', '', full_name)
        full_name = re.sub(r'\s+', ' ', full_name).strip()

        return full_name

    def delete_datasets_by_organization(self, org_id):
        """Delete all datasets belonging to a specific organization."""
        datasets = self._make_request('package_search', method='get',
                                      params={'fq': f'owner_org:"{org_id}"', 'rows': 1000})
        if not datasets or not datasets.get('results'):
            print(f"❌ No datasets found for organization: {org_id}")
            return False

        deleted_count = 0
        for dataset in datasets['results']:
            dataset_id = dataset['id']
            result = self._make_request('dataset_purge', method='post', json_data={'id': dataset_id})
            if result:
                print(f"🗑️ Deleted dataset: {dataset_id}")
                deleted_count += 1
            else:
                print(f"⚠️ Failed to delete dataset: {dataset_id}")

        print(f"\n✅ Total datasets deleted from {org_id}: {deleted_count}")
        return True


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

    parent_orgs = [
        "government_ministry",
        "government_agency",
        "government_department",
        "government_bureau",
        "government_committee"
    ]


    def _get_organization_by_title(self, title):
        """Helper to find organization by title if name lookup fails"""
        results = self._make_request('organization_search', params={'q': f'title:"{title}"'})
        if results and results.get('count', 0) > 0:
            return results['results'][0]['name']
        return None

    def get_or_create_organization(self, org_name):
        if not org_name:
            print("❌ Organization name is empty")
            return None

        # Generate valid org_id
        org_id = generate_valid_ckan_id(org_name)
        print(f"🆔 Trying to use organization ID: {org_id}")

        # Try to find organization by ID
        result = self._make_request('organization_show', params={'id': org_id})
        if result:
            print(f"✅ Organization found by ID: {org_id}")
            return result['id']

        # Try to find by title if ID failed
        org_slug_by_title = self._get_organization_by_title(org_name)
        if org_slug_by_title:
            print(f"✅ Organization found by title: {org_slug_by_title}")
            return org_slug_by_title

        # Prepare org creation data
        create_data = {
            "name": org_id,
            "title": org_name[:200],
            "description": f"Government organization: {org_name[:300]}",
            "is_organization": True,
            "state": "active"
        }

        # Detect and assign parent org type
        org_type = self._detect_org_type(org_name)
        if org_type:
            parent_org = f"government_{org_type}"
            if self._make_request('organization_show', params={'id': parent_org}):
                create_data["groups"] = [{"name": parent_org}]

        # Try to create the organization
        if self._make_request('organization_create', method='post', json_data=create_data):
            print(f"✅ Created new organization: {org_id}")
            return org_id

        # Try again with transliterated title
        print("⚠️ Retrying with English transliteration...")
        create_data["title"] = transliterate(org_name)[:200]
        translit_id = generate_valid_ckan_id(create_data["title"])
        create_data["name"] = translit_id

        if self._make_request('organization_create', method='post', json_data=create_data):
            print(f"✅ Created organization with transliterated ID: {translit_id}")
            return translit_id

        # Final fallback to parent org
        if org_type:
            parent_org = f"government_{org_type}"
            print(f"⚠️ Using parent organization: {parent_org}")
            return parent_org

        print(f"❌ Could not create or retrieve organization: {org_name}")
        return None

    def create_dataset(
            self,
            name,
            title,
            owner_org,
            author,
            authoremail,
            notes,
            tags,
            url=None,
            extras=None
    ):
        payload = {
            "name": name,
            "title": title,
            "owner_org": owner_org,
            "author": author,
            "author_email": authoremail,
            "notes": notes,
            "tags": [{"name": tag} for tag in tags],
            "license_id": "cc-by",
            "state": "active"
        }

        if url:
            payload["url"] = url
        if extras:
            payload["extras"] = extras

        try:
            response = self._post("package_create", json=payload)
            print(f"✅ Dataset created: {name}")
            if isinstance(response, dict):
                return response.get("result", {}).get("id")
            elif hasattr(response, "json"):
                return response.json().get("result", {}).get("id")
            else:
                return None


        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                print(f"⚠️ Dataset '{name}' already exists. Updating instead...")

                # Add required fields for update
                payload["id"] = name
                try:
                    response = self._post("package_update", json=payload)
                    print(f"✅ Dataset updated: {name}")
                    if isinstance(response, dict):
                        return response.get("result", {}).get("id")
                    elif hasattr(response, "json"):
                        return response.json().get("result", {}).get("id")
                    else:
                        return None

                except requests.exceptions.HTTPError as e:
                    print(f"❌ Failed to update dataset '{name}': {e}")
                    print(f"🔴 Server said: {e.response.text}")
                    return None
            else:
                print(f"❌ Failed to create dataset '{name}': {e}")
                print(f"🔴 Server said: {e.response.text}")
                return None

    def _create_dataset_internal(self, name, title, owner_org, kwargs):
        payload = {
            "name": name,
            "title": title[:200],
            "owner_org": owner_org,
            **kwargs,
            "license_id": "cc-by",
            "state": "active"
        }
        return self._make_request('package_create', method='post', json_data=payload)