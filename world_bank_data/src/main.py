from data_catalog import DataCatalogAPI
from projects import ProjectsAPI
from finances import FinancesAPI
import json
from pathlib import Path
import requests
import os

def save_to_json(data: dict, filename: str) -> str:
    """
    Save data to a JSON file and return the full path
    """
    # Create output directory in the current working directory
    output_dir = Path.cwd() / "output"
    output_dir.mkdir(exist_ok=True)
    
    filepath = output_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    
    return str(filepath)

def main():
    # Initialize API clients
    data_catalog = DataCatalogAPI()
    projects = ProjectsAPI()
    finances = FinancesAPI()

    print(f"Working directory: {os.getcwd()}")
    print("Starting data collection...")

    try:
        # Fetch and save data catalog datasets
        try:
            datasets = data_catalog.get_datasets()
            filepath = save_to_json(datasets, "datasets.json")
            print(f"Successfully saved datasets to: {filepath}")
            print(f"Number of datasets: {len(datasets) if isinstance(datasets, list) else 'N/A'}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching datasets: {str(e)}")
            if hasattr(e, 'response'):
                print(f"Response status code: {e.response.status_code}")
                print(f"Response content: {e.response.text}")

        # Fetch and save projects data
        try:
            all_projects = projects.get_projects()
            filepath = save_to_json(all_projects, "projects.json")
            print(f"Successfully saved projects to: {filepath}")
            print(f"Number of projects: {len(all_projects) if isinstance(all_projects, list) else 'N/A'}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching projects: {str(e)}")
            if hasattr(e, 'response'):
                print(f"Response status code: {e.response.status_code}")
                print(f"Response content: {e.response.text}")

        # Fetch and save financial data
        try:
            loan_data = finances.get_loan_data()
            filepath = save_to_json(loan_data, "loans.json")
            print(f"Successfully saved loan data to: {filepath}")
            print(f"Number of loan records: {len(loan_data) if isinstance(loan_data, list) else 'N/A'}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching loans: {str(e)}")
            if hasattr(e, 'response'):
                print(f"Response status code: {e.response.status_code}")
                print(f"Response content: {e.response.text}")

        try:
            disbursement_data = finances.get_disbursement_data()
            filepath = save_to_json(disbursement_data, "disbursements.json")
            print(f"Successfully saved disbursement data to: {filepath}")
            print(f"Number of disbursement records: {len(disbursement_data) if isinstance(disbursement_data, list) else 'N/A'}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching disbursements: {str(e)}")
            if hasattr(e, 'response'):
                print(f"Response status code: {e.response.status_code}")
                print(f"Response content: {e.response.text}")

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main() 