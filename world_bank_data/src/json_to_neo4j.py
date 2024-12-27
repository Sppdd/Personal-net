import json
import pandas as pd
from pathlib import Path
import os
from typing import Dict, List
from neo4j import GraphDatabase
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class JsonToNeo4jConverter:
    def __init__(self):
        # Get credentials from environment variables
        uri = os.getenv('NEO4J_URI')
        user = os.getenv('NEO4J_USER')
        password = os.getenv('NEO4J_PASSWORD')
        
        # Validate credentials
        if not all([uri, user, password]):
            raise ValueError("Missing Neo4j credentials. Please check your .env file.")
        
        # Create driver with basic authentication
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            
            # Test connection with explicit database
            with self.driver.session(database="neo4j") as session:
                result = session.run("RETURN 1 as num")
                result.single()
                logging.info("Successfully connected to Neo4j Aura")
                
        except Exception as e:
            logging.error(f"Failed to connect to Neo4j: {str(e)}")
            raise
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def close(self):
        """Close the driver connection"""
        if self.driver:
            self.driver.close()

    def json_to_csv(self, input_dir: str = "output", output_dir: str = "csv_output") -> Dict[str, str]:
        """Convert JSON files to CSV format"""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        csv_files = {}
        
        for json_file in input_path.glob("*.json"):
            try:
                # Read JSON file
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Convert to DataFrame
                df = pd.json_normalize(data)
                
                # Fill empty values with 'nil'
                df.fillna('nil', inplace=True)
                
                # Save as CSV
                csv_filename = output_path / f"{json_file.stem}.csv"
                df.to_csv(csv_filename, index=False)
                
                csv_files[json_file.stem] = str(csv_filename)
                self.logger.info(f"Converted {json_file.name} to {csv_filename}")
                
            except Exception as e:
                self.logger.error(f"Error converting {json_file.name}: {str(e)}")
        
        return csv_files

    def load_to_neo4j(self, csv_files: Dict[str, str]):
        """Load CSV files into Neo4j"""
        try:
            # Explicitly specify database
            with self.driver.session(database="neo4j") as session:
                # Create constraints
                self._create_constraints(session)
                
                # Load each file
                for file_type, file_path in csv_files.items():
                    self.logger.info(f"Loading {file_type} from {file_path}")
                    
                    # Read CSV file into DataFrame
                    df = pd.read_csv(file_path)
                    
                    # Load data based on type
                    if file_type == 'datasets':
                        self._load_datasets_from_df(session, df)
                    elif file_type == 'projects':
                        self._load_projects_from_df(session, df)
                    elif file_type == 'loans':
                        self._load_loans_from_df(session, df)
                    elif file_type == 'disbursements':
                        self._load_disbursements_from_df(session, df)
                    
        except Exception as e:
            self.logger.error(f"Error loading data to Neo4j: {str(e)}")
            raise

    def _create_constraints(self, session):
        """Create Neo4j constraints"""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Dataset) REQUIRE d.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Project) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (l:Loan) REQUIRE l.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Country) REQUIRE c.code IS UNIQUE"
        ]
        
        for constraint in constraints:
            session.run(constraint)

    def _load_datasets_from_df(self, session, df):
        """Load datasets from DataFrame"""
        for _, row in df.iterrows():
            query = """
            MERGE (d:Dataset {id: $id})
            SET d += {
                name: $name,
                description: $description,
                lastUpdated: $lastUpdated
            }
            """
            session.run(query, dict(row))

    def _load_projects_from_df(self, session, df):
        """Load projects from DataFrame"""
        for _, row in df.iterrows():
            query = """
            MERGE (p:Project {id: $id})
            SET p += {
                name: $name,
                status: $status,
                startDate: $start_date,
                endDate: $end_date
            }
            WITH p
            MERGE (c:Country {code: $country_code})
            SET c.name = $country_name
            MERGE (p)-[:IMPLEMENTED_IN]->(c)
            """
            session.run(query, dict(row))

    def _load_loans_from_df(self, session, df):
        """Load loans from DataFrame"""
        for _, row in df.iterrows():
            query = """
            MERGE (l:Loan {id: $loan_number})
            SET l += {
                amount: toFloat($original_principal_amount),
                status: $loan_status,
                approvalDate: $approval_date
            }
            WITH l
            MERGE (c:Country {code: $country_code})
            SET c.name = $country_name
            MERGE (l)-[:ISSUED_TO]->(c)
            """
            session.run(query, dict(row))

    def _load_disbursements_from_df(self, session, df):
        """Load disbursements from DataFrame"""
        for _, row in df.iterrows():
            query = """
            MERGE (d:Disbursement {id: $disbursement_id})
            SET d += {
                amount: toFloat($amount),
                date: $disbursement_date
            }
            WITH d
            MATCH (l:Loan {id: $loan_number})
            MERGE (l)-[:HAS_DISBURSEMENT]->(d)
            """
            session.run(query, dict(row))

def main():
    converter = None
    try:
        # Initialize converter
        converter = JsonToNeo4jConverter()
        print("Successfully connected to Neo4j Aura")
        
        # Convert JSON to CSV
        print("Converting JSON files to CSV...")
        csv_files = converter.json_to_csv()
        print(f"Converted files: {list(csv_files.keys())}")
        
        # Load data into Neo4j
        print("Loading data into Neo4j...")
        converter.load_to_neo4j(csv_files)
        print("Data loading completed successfully")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        if hasattr(e, 'message'):
            print(f"Details: {e.message}")
    finally:
        # Ensure driver is closed
        if converter:
            converter.close()

if __name__ == "__main__":
    main() 