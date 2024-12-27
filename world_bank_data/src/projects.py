import requests
from typing import Dict, List, Optional

class ProjectsAPI:
    def __init__(self):
        self.base_url = "https://search.worldbank.org/api/v2/projects"
        self.headers = {
            "Accept": "application/json"
        }

    def get_projects(self, country_code: Optional[str] = None, 
                    status: Optional[str] = None,
                    page: int = 1) -> Dict:
        """Fetch projects with optional filters"""
        params = {
            "format": "json",
            "page": page,
            "rows": 100  # Number of results per page
        }
        
        if country_code:
            params["country"] = country_code
        if status:
            params["status"] = status

        full_url = requests.Request('GET', self.base_url, params=params).prepare().url
        print(f"Requesting URL: {full_url}")  # Debug print

        response = requests.get(self.base_url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_project_details(self, project_id: str) -> Dict:
        """Get detailed information about a specific project"""
        url = f"{self.base_url}/{project_id}"
        params = {"format": "json"}
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json() 