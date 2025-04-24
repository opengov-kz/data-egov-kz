API_KEY = "dcc32505f6134b818ec7ce60b1d5b0c6"
BASE_URL = "https://data.egov.kz"
# In your config or at the start of the script
BASE_SEARCH_URL = (
    "https://data.egov.kz/datasets/search?"
    "text=&"
    "expType=1&"
    "category=&"
    "pDateBeg=&"
    "pDateEnd=&"
    "statusType=1&"
    "actualType=&"
    "datasetSortSelect=createdDateDesc&"
    "govAgencyId={gov_agency_id}&"
    "page={page}"
)
LIST_URL = "https://data.egov.kz/search"
CKAN_BASE_URL = "https://data.opengov.kz"
OPENGOV_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJpYjV4VE9QLWpqU0xSQ1ZNU2Q1cVlEUkwzTjVmUGt4SUozVmJaakgyM0RjIiwiaWF0IjoxNzQ0Mjg0Nzc2fQ.Ks8RdNnyPAtBuq2urL7wdXtIf5HPHJm3yuZ_kQiA6ns"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

REQUIRED_PERMISSIONS = [
    'package_create',
    'package_update',
    'package_delete',
    'dataset_purge'
]

CKAN_HEADERS = {
    "Authorization": API_KEY
}

CGO_DATASOURCE = "datasources/getstatisticsbycgogovagencies.json"
MIO_DATASOURCE = "datasources/getstatisticsbymiogovagencies.json"
QUASIORG_DATASOURCE = "datasources/getstatisticsbyquasiorganizations.json"