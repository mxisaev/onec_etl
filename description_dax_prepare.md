Yes, you're absolutely right! Let me break down the actual process:
Copy DAX from PowerBI ✅
Copy the raw DAX query from PowerBI
This is the exact query as it appears in PowerBI
Paste to dax_queries.py ✅
File: /docker/dags/oneC_etl/config/dax_queries.py
This is the raw storage for DAX queries
Each query is stored as a Python string variable
Example: COMPANY_PRODUCTS_ETL_QUERY = """..."""
Script to make string ✅
File: /docker/dags/oneC_etl/scripts/prepare_dax.py
This script:
Reads the raw queries from dax_queries.py
Cleans them (removes extra spaces, normalizes line endings)
Converts them to a proper string format
Creates a JSON structure
Save to Airflow Variables ✅
The script saves the processed queries to Airflow Variables
Uses the dax_queries variable name
Stores both the query and its description
The process is exactly as you described! This is a good workflow because:
Raw DAX queries are stored in code (version controlled)
The cleaning/processing is automated
The final format is stored in Airflow Variables for runtime use