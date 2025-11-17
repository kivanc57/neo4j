import os
from dotenv import load_dotenv
import pandas as pd
from neo4j import GraphDatabase

load_dotenv()

URI = os.getenv('NEO4J_URI')
USERNAME = os.getenv('NEO4J_USERNAME')
PASSWORD = os.getenv('NEO4J_PASSWORD')

class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver= None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
    
    def verify_conn(self):
        try:
            self.__driver.verify_connectivity()
            print("Connection successful!")
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, databases=None):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session(database=databases) as session:
                return list(session.run(query, parameters))
        except Exception as e:
            print(f"Query failed: {e}")
        return None

def insert_data(conn):
    db = "neo4j"
    excel_path = os.path.join("import", "sample.xlsx")
    df = pd.read_excel(excel_path)

    query = """
    CREATE (kdo:Kdo {name: $kdo_name})
    CREATE (koho:Koho {name: $koho_name})
    CREATE (kdo)-[:CITED {format: $format, rok: $rok, číslo: $cislo, časopis: $casopis}]->(koho)
    """

    for _, row in df.iterrows():
        parameters = {
            "kdo_name": row["KDO"],
            "koho_name": row["KOHO"],
            "format": int(row["FORMAT"]),
            "rok": int(row["ROK"]),
            "cislo": int(row["ČÍSLO"]),
            "casopis": row["ČASOPIS"],
        }
        conn.query(query, parameters=parameters, databases=db)

def main():
    conn = Neo4jConnection(uri=URI, user=USERNAME, pwd=PASSWORD)
    conn.verify_conn()
    insert_data(conn)
    conn.close()

if __name__ == "__main__":
    main()

