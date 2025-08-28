#!/bin/bash

# Script to update DAX queries in Airflow Variables
# Updates only the 'partners' key, preserving other keys

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
    docker exec -i docker-airflow-webserver-1 bash -c "cd /opt/airflow/dags/suppliers_etl/dax_prepare && ./update_airflow.sh"
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

# Extract only the partners query from the JSON
echo "🔍 Extracting partners query..."
PARTNERS_QUERY=$(python3 -c "
import json
with open('dax_queries.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    if 'partners' in data:
        print(json.dumps(data['partners'], ensure_ascii=False))
    else:
        print('Error: partners key not found in JSON')
        exit(1)
")

if [ $? -ne 0 ]; then
    echo "❌ Failed to extract partners query from JSON"
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
        if 'partners' in data:
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
        echo "   🔄 partners: will be updated"
    else
        echo "   ➕ partners: will be added"
    fi
else
    echo "📋 No existing dax_queries variable found - will create new with partners key"
fi

# Show what will be updated
echo ""
echo "🔄 Partners query to be updated:"
echo "   • TOPN limit: $(echo "$PARTNERS_QUERY" | grep -o 'TOPN([^,]*' | head -1)"
echo "   • Description: $(echo "$PARTNERS_QUERY" | python3 -c "import json, sys; print(json.load(sys.stdin)['description'])")"
echo ""

# Ask for confirmation
read -p "🤔 Do you want to update the 'partners' key in Airflow Variables? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Update cancelled by user"
    exit 0
fi

# Update only the partners key in dax_queries variable
echo "🔄 Updating partners key in dax_queries variable..."

# Get current variables and update only partners key using Python API
if echo "$VARIABLE_CHECK" | grep -q "EXISTS"; then
    # Variable exists - update only partners key
    UPDATED_VARS=$(python3 -c "
from airflow.models import Variable
import json, sys
try:
    data = Variable.get('dax_queries', deserialize_json=True)
    # Update only the partners key (or add if doesn't exist)
    data['partners'] = json.loads(sys.argv[1])
    print(json.dumps(data, ensure_ascii=False, indent=2))
except Exception as e:
    print(f'Error updating variables: {e}', file=sys.stderr)
    exit(1)
" "$PARTNERS_QUERY")
    
    if [ $? -ne 0 ]; then
        echo "❌ Failed to update existing variables"
        exit 1
    fi
    
    # Set updated variables using Python API to avoid database prompts
    if echo "$UPDATED_VARS" | python3 -c "
import json, sys
from airflow.models import Variable
try:
    data = json.load(sys.stdin)
    Variable.set('dax_queries', data, serialize_json=True)
    print('Variable updated successfully')
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    exit(1)
"; then
        echo "✅ Partners key updated successfully in existing dax_queries variable"
    else
        echo "❌ Failed to update dax_queries variable"
        exit 1
    fi
else
    # Variable doesn't exist - create new with only partners key
    NEW_VARS=$(python3 -c "
import json
import sys
try:
    # Read the PARTNERS_QUERY from command line argument
    partners_query = sys.argv[1]
    data = {'partners': json.loads(partners_query)}
    print(json.dumps(data, ensure_ascii=False, indent=2))
except Exception as e:
    print(f'Error creating new variables: {e}', file=sys.stderr)
    exit(1)
" "$PARTNERS_QUERY")
    
    if echo "$NEW_VARS" | python3 -c "
import json, sys
from airflow.models import Variable
try:
    data = json.load(sys.stdin)
    Variable.set('dax_queries', data, serialize_json=True)
    print('Variable created successfully')
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    exit(1)
"; then
        echo "✅ New dax_queries variable created with partners key"
    else
        echo "❌ Failed to create dax_queries variable"
        exit 1
    fi
fi

# Verify the update using Python API
echo "🔍 Verifying update..."
VERIFY_RESULT=$(python3 -c "
from airflow.models import Variable
import json
try:
    data = Variable.get('dax_queries', deserialize_json=True)
    print('SUCCESS')
    for key in data.keys():
        print(f'   • {key}')
    if 'partners' in data:
        partners = data['partners']
        print(f'   ✅ partners: {partners[\"description\"]}')
        if 'TOPN(' in partners['query']:
            topn_part = partners['query'].split('TOPN(')[1].split(',')[0].strip()
            print(f'   📏 TOPN limit: {topn_part}')
        else:
            print(f'   📏 No TOPN limit found')
    else:
        print('ERROR')
        print('partners key not found after update')
except Exception as e:
    print('ERROR')
    print(str(e))
")

if echo "$VERIFY_RESULT" | grep -q "SUCCESS"; then
    echo "📊 Updated content preview:"
    echo "   Keys in dax_queries:"
    echo "$VERIFY_RESULT" | grep -v "SUCCESS"
    echo "✅ Verification successful - partners key updated"
else
    echo "❌ Verification failed:"
    echo "$VERIFY_RESULT" | grep -v "ERROR"
    exit 1
fi

# Clean up
echo "Cleaning up..."
if [ -f "dax_queries.json" ]; then
    rm "dax_queries.json"
    echo "✅ Temporary files cleaned up"
fi

echo ""
echo "🎉 Partners query has been updated successfully!"
echo "📋 You can now run the SuppliersETL DAG to test the new query."
echo ""
echo "🚀 Next steps:"
echo "   1. Run: airflow dags trigger SuppliersETL"
echo "   2. Check logs: docker/dags/suppliers_etl/logs/suppliers_etl.log"
echo "   3. Verify row count > 501 in logs (should be much higher now)"
echo ""
echo "📊 What was updated:"
echo "   • Only 'partners' key in dax_queries variable"
echo "   • Other keys (if any) were preserved"
echo "   • TOPN limit increased from 501 to 10000" 