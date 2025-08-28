# DAX Query Preparation

This directory contains tools for preparing DAX queries from PowerBI and loading them into Airflow Variables.

## Directory Structure

- `raw_queries.py` - Store your raw DAX queries here
- `prepare_dax.py` - Script to process and clean DAX queries
- `update_airflow.sh` - Script to update Airflow Variables
- `dax_queries.json` - Generated file with processed queries (not automatically removed due to permission issues)

## How to Use

### 1. Add Your DAX Query

Copy your DAX query from PowerBI and add it to `raw_queries.py`:

```python
YOUR_QUERY_NAME_QUERY = """
EVALUATE
SUMMARIZECOLUMNS(
    'Table'[Column],
    'AnotherTable'[AnotherColumn],
    FILTER(
        KEEPFILTERS('Table'),
        [Measure] = 1
    )
)
"""
```

### 2. Process and Load Queries

Run the update script from the docker directory:

```bash
cd /var/www/vhosts/itland.uk/docker
docker exec docker-airflow-webserver-1 bash /opt/airflow/dags/suppliers_etl/dax_prepare/update_airflow.sh
```

Or run individual steps:

```bash
# Step 1: Prepare queries
docker exec docker-airflow-webserver-1 python3 /opt/airflow/dags/suppliers_etl/dax_prepare/prepare_dax.py

# Step 2: Load into Airflow Variables
docker exec docker-airflow-webserver-1 bash /opt/airflow/dags/suppliers_etl/dax_prepare/update_airflow.sh
```

## Naming Convention

- Query variables must end with `_QUERY`
- The name will be converted to lowercase and `_query` will be removed for the Airflow Variable
- Example: `COMPANY_PRODUCTS_QUERY` becomes `company_products` in Airflow Variables

## Current Queries

### company_products
Complex query using `SUMMARIZECOLUMNS` with filtering and table relationships:

```dax
EVALUATE
SUMMARIZECOLUMNS(
    'CompanyProducts'[ID],
    'CompanyProducts'[Description],
    'CompanyProducts'[Brand],
    'CompanyProducts'[Category],
    'CompanyProducts'[Withdrawn_from_range],
    'CompanyProducts'[item_number],
    "Product_Properties", 'УТ_Номенклатура'[Product Properties],
    FILTER(
        KEEPFILTERS('УТ_Номенклатура'),
        [Выводится_без_остатков] = 0
    )
)
```

## Troubleshooting

### Permission Issues
If you see "Permission denied" errors:
- The script runs as `airflow` user inside the container
- Files created on the host may have different permissions
- The cleanup step (removing dax_queries.json) is disabled to avoid permission issues

### Missing Dependencies
If you see "ModuleNotFoundError":
- The script uses standard Python libraries
- No additional dependencies required

## Example Workflow

1. **Copy DAX query from PowerBI** (e.g., from DAX Studio or PowerBI Desktop)

2. **Add to raw_queries.py**:
   ```python
   COMPANY_PRODUCTS_QUERY = """
   EVALUATE
   SUMMARIZECOLUMNS(
       'CompanyProducts'[ID],
       'CompanyProducts'[Description]
   )
   """
   ```

3. **Process and load**:
   ```bash
   docker exec docker-airflow-webserver-1 bash /opt/airflow/dags/suppliers_etl/dax_prepare/update_airflow.sh
   ```

4. **Use in DAG**:
   ```python
   from airflow.models import Variable
   import json
   
   dax_queries = json.loads(Variable.get('dax_queries'))
   query = dax_queries['company_products']['query']
   ```

5. **Test the DAG**:
   ```bash
   docker exec docker-airflow-webserver-1 airflow dags trigger SuppliersETL
   ``` 