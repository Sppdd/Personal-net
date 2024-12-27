import requests
from typing import Dict, List, Optional

class FinancesAPI:
    def __init__(self):
        # Updated to use the correct finances API endpoint
        self.base_url = "https://financesdata.worldbank.org/resource"
        self.headers = {
            "Accept": "application/json"
        }

    def get_loan_data(self, country_code: Optional[str] = None, 
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None) -> Dict:
        """Fetch loan data with optional filters"""
        url = f"{self.base_url}/g5kz-zdi6.json"  # IBRD Statement of Loans endpoint
        params = {
            "$limit": 1000
        }
        
        where_conditions = []
        if country_code:
            where_conditions.append(f"country_code = '{country_code}'")
        if start_date and end_date:
            where_conditions.append(f"due_date between '{start_date}T00:00:00' and '{end_date}T23:59:59'")
        
        if where_conditions:
            params["$where"] = " AND ".join(where_conditions)

        full_url = requests.Request('GET', url, params=params).prepare().url
        print(f"Requesting URL: {full_url}")  # Debug print

        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_disbursement_data(self, country_code: Optional[str] = None) -> Dict:
        """Get disbursement data for a country"""
        url = f"{self.base_url}/dd6r-pdh4.json"  # IDA Statement of Credits and Grants endpoint
        params = {
            "$limit": 1000
        }
        
        if country_code:
            params["$where"] = f"country_code = '{country_code}'"

        full_url = requests.Request('GET', url, params=params).prepare().url
        print(f"Requesting URL: {full_url}")  # Debug print

        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json() 