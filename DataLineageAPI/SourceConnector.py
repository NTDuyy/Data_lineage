import json
import subprocess
from pathlib import Path
from neo4j import GraphDatabase

import sqlglot
from sqlglot import parse_one, exp

import boto3
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

def safe_name(part):
    """Return cleaned identifier whether it's a string or AST node."""
    if part is None:
        return None
    if hasattr(part, "sql"):
        return part.sql().replace('"', '')
    return str(part).replace('"', '')


def extract_source_from_sql(sql):
    ast = parse_one(sql)

    # --- 1. Extract tables including DB + schema ---
    alias_to_table = {}

    for table in ast.find_all(exp.Table):

        alias = table.alias or table.name

        db = safe_name(table.catalog)  
        schema = safe_name(table.db)  
        name = safe_name(table.name)

        parts = [p for p in [db, schema, name] if p]
        full_name = ".".join(parts)

        alias_to_table[alias] = full_name

    # --- 2. SELECT list ---
    select_exprs = ast.selects

    # --- 3. Handle GROUP BY numeric indexes ---
    group_positions = []
    if ast.args.get("group"):
        for g in ast.args["group"].expressions:
            if isinstance(g, exp.Literal) and g.is_int:
                group_positions.append(int(g.this) - 1)

    # --- 4. Build lineage ---
    column_lineage = {}

    for i, sel in enumerate(select_exprs):
        # Target column name
        if isinstance(sel, exp.Alias):
            target = sel.alias
            expr = sel.this
        else:
            target = sel.sql()
            expr = sel

        # Extract all source columns used within the expression
        source_cols = list(expr.find_all(exp.Column))

        sources = []
        for col in source_cols:
            alias = col.table or list(alias_to_table.keys())[0]  # fallback
            sources.append({
                "table": alias_to_table.get(alias, alias),
                "column": col.name
            })

        column_lineage[target] = sources

    return column_lineage



class DbtSourceConnector:
    def __init__(self, dbt_project_path, neo4j_uri=NEO4J_URI, neo4j_user=NEO4J_USERNAME, neo4j_password=NEO4J_PASSWORD):
        self.project_path = Path(dbt_project_path)
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def compile_dbt_model(self):
        MODELS_PATH = self.project_path / "models"
        sql_files = list(MODELS_PATH.rglob("*.sql"))
    
    
        result = subprocess.run(
            ["dbt", "compile"],
            cwd=self.project_path,
            capture_output=True,
            text=True
        )
    
        if result.returncode != 0:
            print(f"❌ Error compiling dbt project")
            print(result.stderr)
        else:
            print(f"✅ Compiled dbt project")
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
            
        def create_column_nodes(tx, db_name, schema, table_name, name):
            """
            Create or merge a Column node.
            """
            tx.run(
                """
                MERGE (c:Column {
                    id: $id
                })
                SET c.name = $column_name,
                    c.table_name = $table_name,
                    c.schema = $schema_name,
                    c.db_name = $db_name
                """,
                id=f"{db_name}.{schema}.{table_name}.{name}",
                column_name=name,
                table_name=table_name,
                schema_name=schema,
                db_name = db_name
            )


        def create_column_relationships(tx, source_dbname, source_schema, source_table, source_col, target_dbname, target_schema, target_table, target_col, source):
            """
            Create lineage between columns:
                (source_column) -[:TRANSFORMED_TO]-> (target_column)
            """
            tx.run(
                """
                MATCH (src:Column {id: $source_id})
                MATCH (tgt:Column {id: $target_id})
                MERGE (src)-[:TRANSFORMED_TO]->(tgt)
                """,
                source_id=f"{source_dbname}.{source_schema}.{source_table}.{source_col}",
                target_id=f"{target_dbname}.{target_schema}.{target_table}.{target_col}"
            )

            tx.run(
                    """
                    MATCH (m:Column {id: $source_id})
                    MERGE (t:Table {name: $source_table, schema: $source_schema, db_name: $source_dbname, source: $source})
                    MERGE (m)-[:BELONGS_TO]->(t)
                    """,
                    source_table=source_table,
                    source_schema=source_schema,
                    source_dbname=source_dbname,
                    source_id=f"{source_dbname}.{source_schema}.{source_table}.{source_col}",
                    source=source
                )
            tx.run(
                    """
                    MATCH (m:Column {id: $target_id})
                    MERGE (t:Table {name: $target_table, schema: $target_schema, db_name: $target_dbname, source: $source})
                    MERGE (m)-[:BELONGS_TO]->(t)
                    """,
                    target_table=target_table,
                    target_schema=target_schema,
                    target_dbname=target_dbname,
                    target_id=f"{target_dbname}.{target_schema}.{target_table}.{target_col}",
                    source=source
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
            # Extract columns and their source
            column_source = extract_source_from_sql(model_data["compiled_code"])
            
            for output_column, sources in column_source.items():
                with self.driver.session() as session:
                    # Create target (output) column node
                    session.execute_write(
                        create_column_nodes,
                        model_data["database"],
                        model_data["schema"],
                        model_id.split(".")[-1],
                        output_column
                    )

                    # Create source columns + relationships
                    for related_col in sources:
                        src_table_parts = related_col["table"].split(".")
                        src_table = src_table_parts[2]
                        src_schema = src_table_parts[1]
                        src_dbname = src_table_parts[0]
                        src_column = related_col["column"]

                        # Create source column node
                        session.execute_write(
                            create_column_nodes,
                            src_dbname,
                            src_schema,
                            src_table,
                            src_column
                        )

                        # Create lineage relationship
                        session.execute_write(
                            create_column_relationships,
                            src_dbname,
                            src_schema,
                            src_table,
                            src_column,
                            model_data["database"],
                            model_data["schema"],
                            model_id.split(".")[-1],
                            output_column,
                            source_name
                        )


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
    def __init__(self, host = RED_SHIFT_HOST
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

    def import_metadata_neo4j(self):
        def create_nodes(tx, rows):
            for _, row in rows.iterrows():
                # Create Table node (once per row)
                tx.run(
                    """
                    MERGE (t:Table {
                        name: $table_name,
                        schema: $schema_name,
                        db_name: $db_name,
                        source: 'redshift'
                    })
                    SET t.object_type = $object_type
                    """,
                    table_name=row["table_name"],
                    schema_name=row["schema_name"],
                    db_name=self.db_name,
                    object_type=row["object_type"]
                )

                # Create Column node and relationship
                tx.run(
                    """
                    MERGE (c:Column {id: $id})
                    SET c.name = $column_name,
                        c.table_name = $table_name,
                        c.schema = $schema_name,
                        c.db_name = $db_name,
                        c.data_type = $data_type,
                        c.is_nullable = $is_nullable,
                        c.constraint_type = $constraint_type,
                        c.constraint_name = $constraint_name
                    WITH c
                    MATCH (t:Table {name: $table_name, schema: $schema_name, db_name: $db_name, source: 'redshift'})
                    MERGE (c)-[:BELONGS_TO]->(t)
                    """,
                    id=f"{self.db_name}.{row['schema_name']}.{row['table_name']}.{row['column_name']}",
                    column_name=row["column_name"],
                    table_name=row["table_name"],
                    schema_name=row["schema_name"],
                    data_type=row["data_type"],
                    is_nullable=row["is_nullable"],
                    constraint_type=row["constraint_type"],
                    constraint_name=row["constraint_name"],
                    db_name=self.db_name
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
