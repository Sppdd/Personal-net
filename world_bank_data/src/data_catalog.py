import requests
from typing import Dict, List, Optional

class DataCatalogAPI:
    def __init__(self):
        self.base_url = "https://api.worldbank.org/v2/sources/2"
        self.headers = {
            "Accept": "application/json"
        }

    def get_datasets(self, page: int = 1, per_page: int = 100) -> Dict:
        """Fetch datasets from the Data Catalog API"""
        params = {
            "format": "json",
            "page": page,
            "per_page": per_page
        }
        
        full_url = requests.Request('GET', self.base_url, params=params).prepare().url
        print(f"Requesting URL: {full_url}")  # Debug print
        
        response = requests.get(self.base_url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_dataset_details(self, dataset_id: str) -> Dict:
        """Get detailed information about a specific dataset"""
        url = f"{self.base_url}/{dataset_id}"
        params = {"format": "json"}
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json() 