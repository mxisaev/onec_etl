#!/bin/bash

# Script to update DAX queries in Airflow Variables
# Updates only the 'company_products' key, preserving other keys

# Set the working directory
cd "$(dirname "$0")"

# Check if we're in Docker environment
if [ -f "/.dockerenv" ]; then
    echo "Running inside Docker container"
    AIRFLOW_CMD="airflow"
    PYTHON_CMD="python3"
else
    echo "Running on host - will execute in Docker container"
    # Check if docker is available
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker command not found. Please ensure Docker is installed."
        exit 1
    fi
    
    # Execute this script inside the Airflow container
    echo "Executing script inside Airflow webserver container..."
    docker exec -i docker-airflow-webserver-1 bash -c "cd /opt/airflow/dags/oneC_etl/dax_prepare && ./update_airflow.sh"
    exit $?
fi

# Prepare DAX queries
echo "Preparing DAX queries..."
python3 prepare_dax.py

# Check if the JSON file was created successfully
if [ ! -f "dax_queries.json" ]; then
    echo "Error: Failed to create dax_queries.json"
    exit 1
fi

# Extract only the company_products query from the JSON
echo "🔍 Extracting company_products query..."
COMPANY_PRODUCTS_QUERY=$(python3 -c "
import json
with open('dax_queries.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    if 'company_products' in data:
        print(json.dumps(data['company_products'], ensure_ascii=False))
    else:
        print('Error: company_products key not found in JSON')
        exit(1)
")

if [ $? -ne 0 ]; then
    echo "❌ Failed to extract company_products query from JSON"
    exit 1
fi

# Check current Airflow variables using Python API
echo "🔍 Checking current Airflow Variables..."
VARIABLE_CHECK=$(python3 -c "
from airflow.models import Variable
import json
try:
    data = Variable.get('dax_queries', deserialize_json=True, default_var=None)
    if data is not None:
        print('EXISTS')
        print(json.dumps(list(data.keys())))
        if 'company_products' in data:
            print('UPDATE')
        else:
            print('ADD')
    else:
        print('NOT_EXISTS')
except Exception as e:
    print('ERROR')
    print(str(e))
")

if echo "$VARIABLE_CHECK" | grep -q "EXISTS"; then
    echo "📋 Current dax_queries variable exists"
    echo "📊 Current keys:"
    KEYS=$(echo "$VARIABLE_CHECK" | sed -n '2p' | python3 -c "import json, sys; keys=json.load(sys.stdin); [print(f'   • {key}') for key in keys]")
    if echo "$VARIABLE_CHECK" | grep -q "UPDATE"; then
        echo "   🔄 company_products: will be updated"
    else
        echo "   ➕ company_products: will be added"
    fi
else
    echo "📋 No existing dax_queries variable found - will create new with company_products key"
fi

# Show what will be updated
echo ""
echo "🔄 Company Products query to be updated:"
echo "   • Description: $(echo "$COMPANY_PRODUCTS_QUERY" | python3 -c "import json, sys; print(json.load(sys.stdin)['description'])")"
echo ""

# Ask for confirmation
read -p "🤔 Do you want to update the 'company_products' key in Airflow Variables? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Operation cancelled by user"
    exit 0
fi

# Update Airflow Variables
echo "🔄 Updating Airflow Variables..."

# Get current variables or create new
CURRENT_VARS=$(python3 -c "
from airflow.models import Variable
import json
try:
    data = Variable.get('dax_queries', deserialize_json=True, default_var={})
    print(json.dumps(data))
except Exception as e:
    print('{}')
")

# Update with new company_products query
UPDATED_VARS=$(python3 -c "
import json
import sys
current = json.loads(sys.argv[1])
company_products = json.loads(sys.argv[2])
current['company_products'] = company_products
print(json.dumps(current, ensure_ascii=False))
" "$CURRENT_VARS" "$COMPANY_PRODUCTS_QUERY")

# Set the updated variables
echo "📝 Setting updated dax_queries variable..."
airflow variables set dax_queries "$UPDATED_VARS"

if [ $? -eq 0 ]; then
    echo "✅ Successfully updated Airflow Variables!"
    echo "📊 Updated keys:"
    echo "$UPDATED_VARS" | python3 -c "import json, sys; keys=json.load(sys.stdin).keys(); [print(f'   • {key}') for key in keys]"
else
    echo "❌ Failed to update Airflow Variables"
    exit 1
fi

echo ""
echo "🎉 DAX queries have been updated successfully!"
echo "💡 You can now run the ETL process to use the new queries." 