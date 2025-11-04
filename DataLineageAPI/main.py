from SourceConnector import DbtSourceConnector, RedshiftSourceConnector
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
import os

load_dotenv()

DBT_PROJECT_PATH = os.getenv("DBT_PROJECT_PATH")

NEO4J_URI=os.getenv("NEO4J_URI")
NEO4J_USERNAME=os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD=os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE=os.getenv("NEO4J_DATABASE")

RED_SHIFT_CLIENT_TYPE = os.getenv("RED_SHIFT_CLIENT_TYPE")
RED_SHIFT_REGION_NAME = os.getenv("RED_SHIFT_REGION_NAME")
RED_SHIFT_HOST = os.getenv("RED_SHIFT_HOST")
RED_SHIFT_DBNAME = os.getenv("RED_SHIFT_DBNAME")
RED_SHIFT_USER = os.getenv("RED_SHIFT_USER")
RED_SHIFT_PASSWORD = os.getenv("RED_SHIFT_PASSWORD")
RED_SHIFT_SCHEMA = os.getenv("RED_SHIFT_SCHEMA")
app = FastAPI(title="Metadata → Neo4j API")


dbt_connector = DbtSourceConnector()
redshift_connector = RedshiftSourceConnector()

@app.get("/")
def home():
    return {"message": "Welcome to Metadata → Neo4j API"}

@app.post("/compile_dbt")
def compile_models():
    try:
        dbt_connector.compile_dbt_model()
        return {"status": "success", "message": "DBT models compiled successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/import_dbt")
def import_metadata(r):
    try:
        dbt_connector.import_metadata_neo4j()
        return {"status": "success", "message": "Metadata from dbt imported into Neo4j."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/import_redshift")
def import_metadata():
    try:
        redshift_connector.import_metadata_neo4j()
        return {"status": "success", "message": "Metadata from redshift imported into Neo4j."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/import")
def import_metadata():
    try:
        dbt_connector.import_metadata_neo4j()
        redshift_connector.import_metadata_neo4j()
        return {"status": "success", "message": "Metadata from all sources imported into Neo4j."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    