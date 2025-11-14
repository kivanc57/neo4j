import os
from dotenv import load_dotenv
import pandas as pd
from neo4j import GraphDatabase

load_dotenv()

URI = os.getenv('NEO4J_URI')
USERNAME = os.getenv('NEO4J_USERNAME')
PASSWORD = os.getenv('NEO4J_PASSWORD')

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

try:
    driver.verify_connectivity()
    print("Connection successful!")
except Exception as e:
    print(f"Failed to connect to Neo4j: {e}")

excel_path = os.path.join("import", "sample.xlsx")
df = pd.read_excel(excel_path)

with driver.session(database="neo4j") as session:
    for _, cols in df.iterrows():
        query = """
        CREATE (kdo:Kdo {name: $kdo_name})
        -[t:CITED {format: $format, rok: $rok, číslo: $cislo, časopis: $casopis}]->
        (koho:Koho {name: $koho_name})
        """
        session.run(
            query,
            parameters={
                "kdo_name": cols["KDO"],
                "koho_name": cols["KOHO"],
                "format": int(cols["FORMAT"]),
                "rok": int(cols["ROK"]),
                "cislo": int(cols["ČÍSLO"]),
                "casopis": cols["ČASOPIS"],
            }
        )

driver.close()

