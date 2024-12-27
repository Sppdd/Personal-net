from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_connection():
    uri = os.getenv('NEO4J_URI')
    user = os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    
    print(f"Connecting to: {uri}")
    
    try:
        # Create driver with neo4j+s scheme
        driver = GraphDatabase.driver(
            uri,
            auth=(user, password)
        )
        
        # Test connection
        with driver.session(database="neo4j") as session:  # Explicitly specify database
            result = session.run("RETURN 1 as num")
            value = result.single()["num"]
            print(f"Successfully connected! Test query result: {value}")
            
    except Exception as e:
        print(f"Connection failed: {str(e)}")
    finally:
        try:
            driver.close()
        except:
            pass

if __name__ == "__main__":
    test_connection() 