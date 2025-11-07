## 📌 Overview

The **Data_lineage** project is designed to track, document, and visualize data lineage across a data warehouse and analytics environment. It aims to provide clarity about how data flows, transforms, and is consumed — helping teams understand dependencies, maintain data quality, and support auditing/compliance needs.

Key features include:
- Extraction of lineage metadata from a database (e.g., using SQL stored procedures or system tables).  
- A simple API layer (under the `DataLineageAPI/` folder) that serves lineage information.  
- Jupyter notebooks (under `notebook/`) demonstrating lineage analysis, visualization, and reporting.  
- A sample dbt project (under `sales_project/`) to illustrate lineage in a real-world context.  
- Configuration via `requirements.txt` of the Python dependencies for the project.

---

## 🗂 Project Structure
Data_lineage/
├── DataLineageAPI/
├── Redshift Database/
├── notebook/
├── sales_project/
├── requirements.txt
└── README.md

### Folder Details

- **DataLineageAPI/**  
  FastAPI providing upstream/downstream lineage endpoints.

- **Redshift Database/**  
  SQL scripts for extracting metadata (stored procedures, dependencies, table usage, etc.).

- **notebook/**  
  Jupyter notebooks for analysis, visualization, and lineage documentation.

- **sales_project/**  
  Sample dbt project showing raw → staging → fact/dim → analytics.

- **requirements.txt**  
  Python dependencies.
