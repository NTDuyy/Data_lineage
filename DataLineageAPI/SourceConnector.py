import json
import subprocess
from pathlib import Path
from neo4j import GraphDatabase

import psycopg2
import pandas as pd

import os
from dotenv import load_dotenv

load_dotenv()

# Import sensitive information
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


class DbtSourceConnector:
    def __init__(self, dbt_project_path = DBT_PROJECT_PATH, neo4j_uri=NEO4J_URI, neo4j_user=NEO4J_USERNAME, neo4j_password=NEO4J_PASSWORD):
        self.project_path = Path(dbt_project_path)
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def compile_dbt_model(self):
        MODELS_PATH = self.project_path / "models"
        sql_files = list(MODELS_PATH.rglob("*.sql"))
        print(f"Found {len(sql_files)} model(s) to compile.")
    
        for sql_file in sql_files:
            relative_path = sql_file.relative_to(self.project_path)
            print(f"Compiling {relative_path} ...")
    
            result = subprocess.run(
                ["dbt", "compile", "--select", str(relative_path)],
                cwd=self.project_path,
                capture_output=True,
                text=True
            )
    
            if result.returncode != 0:
                print(f"❌ Error compiling {relative_path}:")
                print(result.stderr)
                continue
    
            print(f"✅ Compiled {relative_path}")
            print(result.stdout)

    def load_manifest(self):
        manifest_path = self.project_path / "target" / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"{manifest_path} not found. Run dbt first!")
        with open(manifest_path) as f:
            manifest = json.load(f)
        return manifest

    def import_metadata_neo4j(self):
        """Import dbt model metadata and lineage (models, sources, dependencies, generated tables) into Neo4j."""
        manifest = self.load_manifest()
        nodes = manifest.get("nodes", {})
    
        if not nodes:
            print("⚠️ No models found in manifest.json.")
            return
    
        print(f"📦 Importing {len(nodes)} models into Neo4j...")
    
        # --- Transaction helper functions ---
        def create_models_and_sources(tx, models, sources):
            """Create DbtModel nodes and Source/Table nodes."""
            for m in models:
                tx.run(
                    """
                    MERGE (model:DbtModel {id: $model_id})
                    SET model.name = $model_name,
                        model.description = $description,
                        model.tags = $tags,
                        model.materialized = $materialized
                    """,
                    model_id=m["model_id"],
                    model_name=m["model_name"],
                    description=m["description"],
                    tags=m["tags"],
                    materialized=m["materialized"]
                )
            for s in sources:
                tx.run(
                    """
                    MERGE (t:Table {name: $table_name, schema: $schema_name, db_name: $db_name, source: $source})
                    """,
                    table_name=s["table_name"],
                    schema_name=s["schema_name"],
                    db_name=s["db_name"],
                    source=s["source"]
                )
    
        def create_relationships(tx, model_sources, model_deps, generates):
            """Create relationships between sources and models, models, and generated tables."""
            # FEEDS_DATA_INTO: source -> model
            for rel in model_sources:
                tx.run(
                    """
                    MATCH (m:DbtModel {id: $model_id})
                    MATCH (s:Table {name: $table_name, schema: $schema_name, db_name: $db_name})
                    MERGE (s)-[:FEEDS_DATA_INTO]->(m)
                    """,
                    model_id=rel["model_id"],
                    table_name=rel["table_name"],
                    schema_name=rel["schema_name"],
                    db_name=rel["db_name"]
                )
    
            # PROCEEDS_TO: model -> model
            for rel in model_deps:
                tx.run(
                    """
                    MATCH (m1:DbtModel {id: $model_id})
                    MATCH (m2:DbtModel {id: $depends_on_id})
                    MERGE (m2)-[:PROCEEDS_TO]->(m1)
                    """,
                    model_id=rel["model_id"],
                    depends_on_id=rel["depends_on_id"]
                )
    
            # GENERATES_TO: model -> physical table/view
            for rel in generates:
                tx.run(
                    """
                    MATCH (m:DbtModel {id: $model_id})
                    MERGE (t:Table {name: $table_name, schema: $schema_name, db_name: $db_name, source: $source})
                    MERGE (m)-[:GENERATES]->(t)
                    """,
                    model_id=rel["model_id"],
                    table_name=rel["table_name"],
                    schema_name=rel["schema_name"],
                    db_name=rel["db_name"],
                    source=rel["source_name"]
                )
    
        # --- Prepare data ---
        model_records = []
        source_records = set()
        model_source_rels = []
        model_dependency_rels = []
        generates_rels = []
        source_name = manifest["metadata"]["adapter_type"]
        for model_id, model_data in nodes.items():
            if model_data.get("resource_type") != "model":
                continue  # skip tests, macros, etc.
    
            config = model_data.get("config", {})
            materialized = config.get("materialized", "table")
            relation_name = model_data.get("relation_name")  # physical table/view if exists
    
            # Model node
            model_records.append({
                "model_id": model_id,
                "model_name": model_data.get("name", ""),
                "description": model_data.get("description", ""),
                "tags": model_data.get("tags", []),
                "materialized": materialized
            })
            
            # Handle dependencies
            depends_on_nodes = model_data.get("depends_on", {}).get("nodes", [])
            for dep in depends_on_nodes:
                if dep.startswith("source."):
                    parts = dep.split(".")
                    if len(parts) >= 4:
                        _, project, source_name, table_name = parts[-4:]
                        source_records.add((source_name, table_name, model_data.get("database", ""), model_data.get("schema", "")))
                        model_source_rels.append({
                            "model_id": model_id,
                            "source_name": source_name,
                            "table_name": table_name,
                            "db_name": model_data.get("database", ""),
                            "schema_name": model_data.get("schema", "")
                        })
                elif dep.startswith("model."):
                    model_dependency_rels.append({
                        "model_id": model_id,
                        "depends_on_id": dep
                    })
    
            # Handle GENERATES_TO relationship
            if relation_name:
                parts = relation_name.replace('"', '').split(".")
                if len(parts) == 3:
                    db_name, schema_name, table_name = parts
                    generates_rels.append({
                        "model_id": model_id,
                        "source_name": source_name,
                        "table_name": table_name,
                        "schema_name": schema_name,
                        "db_name": db_name
                    })
    
        # Convert source_records to list of dicts
        source_dicts = [
            {"source": src, "table_name": tbl, "db_name": db, "schema_name": schema}
            for src, tbl, db, schema in source_records
        ]
    
        # --- Execute transactions ---
        with self.driver.session() as session:
            print("🧩 Creating DbtModel and Table nodes...")
            session.execute_write(create_models_and_sources, model_records, source_dicts)
    
            print("🔗 Creating relationships (FEEDS_DATA_INTO, PROCEEDS_TO, GENERATES_TO)...")
            session.execute_write(create_relationships, model_source_rels, model_dependency_rels, generates_rels)
    
        print("✅ Metadata successfully imported into Neo4j.")


class RedshiftSourceConnector():
    def __init__(self, client_type = RED_SHIFT_CLIENT_TYPE
                 , region_name = RED_SHIFT_REGION_NAME
                 , host = RED_SHIFT_HOST
                 , username = RED_SHIFT_USER
                 , password = RED_SHIFT_PASSWORD
                 , db_name = RED_SHIFT_DBNAME
                 , schema_name = RED_SHIFT_SCHEMA 
                 , neo4j_uri=NEO4J_URI
                 , neo4j_user=NEO4J_USERNAME
                 , neo4j_password=NEO4J_PASSWORD):
        self.db_name = db_name
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        # Connect to Redshift
        self.conn = psycopg2.connect(
            host=host,
            port=5439,
            dbname=db_name,
            user=username,
            password=password
        )
        self.schema_name = schema_name

        # Create DataFrames
        self.columns_df = self._get_columns()

        # Close connection
        self.conn.close()

    def _get_columns(self):
        """Fetch columns and constraints (including FK references) for tables in the schema."""
        query = """
            SELECT
                c.table_schema AS schema_name,
                c.table_name AS table_name,
                c.column_name AS column_name,
                c.data_type AS data_type,
                c.is_nullable AS is_nullable,
                tc.constraint_type AS constraint_type,
                kcu.constraint_name AS constraint_name,
                rc.unique_constraint_schema AS referenced_schema,
                kcu2.table_name AS referenced_table,
                kcu2.column_name AS referenced_column,
                t.table_type AS object_type 
            FROM information_schema.columns c
            LEFT JOIN information_schema.key_column_usage kcu
                ON c.table_schema = kcu.table_schema
                AND c.table_name = kcu.table_name
                AND c.column_name = kcu.column_name
            LEFT JOIN information_schema.table_constraints tc
                ON kcu.constraint_schema = tc.table_schema
                AND kcu.constraint_name = tc.constraint_name
            LEFT JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
                AND tc.table_schema = rc.constraint_schema
            LEFT JOIN information_schema.key_column_usage kcu2
                ON rc.unique_constraint_name = kcu2.constraint_name
                AND rc.unique_constraint_schema = kcu2.table_schema
            LEFT JOIN information_schema.tables t
                ON c.table_schema = t.table_schema
                AND c.table_name = t.table_name
            WHERE c.table_schema = %s
            ORDER BY c.table_name, c.ordinal_position;
        """
    
        with self.conn.cursor() as cur:
            cur.execute(query, (self.schema_name,))
            rows = cur.fetchall()
    
        # Correct column names for DataFrame
        columns = [
            "schema_name",
            "table_name",
            "column_name",
            "data_type",
            "is_nullable",
            "constraint_type",
            "constraint_name",
            "referenced_schema",
            "referenced_table",
            "referenced_column",
            "object_type"
        ]
    
        return pd.DataFrame(rows, columns=columns)

    def load_tables_and_relationships(self):
        def create_nodes(tx, rows):
            for _, row in rows.iterrows():
                tx.run(
                    """
                    MERGE (t:Table {name: $table_name, schema: $schema_name, db_name: $db_name, source: 'redshift'})
                    SET t.object_type = $object_type
                    """,
                    table_name=row["table_name"],
                    schema_name=row["schema_name"],
                    db_name=self.db_name,
                    object_type=row["object_type"]
                )
                tx.run(
                    """
                    MERGE (t:Table {name: $table_name, schema: $schema_name, source: 'redshift'})
                    MERGE (c:Column {name: $column_name, table_name: $table_name, schema: $schema_name})
                    SET c.data_type = $data_type,
                        c.is_nullable = $is_nullable,
                        c.constraint_type = $constraint_type,
                        c.constraint_name = $constraint_name
                    MERGE (c)-[:BELONGS_TO]->(t)
                    """,
                    column_name=row["column_name"],
                    table_name=row["table_name"],
                    schema_name=row["schema_name"],
                    data_type=row["data_type"],
                    is_nullable=row["is_nullable"],
                    constraint_type=row["constraint_type"],
                    constraint_name=row["constraint_name"]
                )

        def create_foreign_key_relationships(tx, rows):
            for _, row in rows.iterrows():
                if row["constraint_type"] == "FOREIGN KEY" and row["referenced_table"]:
                    tx.run(
                        """
                        MATCH (c1:Column {name: $column_name, table_name: $table_name, schema: $schema_name})
                        MATCH (c2:Column {name: $referenced_column, table_name: $referenced_table, schema: $referenced_schema})
                        MERGE (c1)-[:REFERENCES]->(c2)
                        """,
                        column_name=row["column_name"],
                        table_name=row["table_name"],
                        schema_name=row["schema_name"],
                        referenced_column=row["referenced_column"],
                        referenced_table=row["referenced_table"],
                        referenced_schema=row["referenced_schema"]
                    )

        with self.driver.session() as session:
            print("🧩 Creating table and column nodes...")
            session.execute_write(create_nodes, self.columns_df)
            print("🔗 Creating foreign key relationships...")
            session.execute_write(create_foreign_key_relationships, self.columns_df)
        print("✅ Tables and relationships imported into Neo4j.")
