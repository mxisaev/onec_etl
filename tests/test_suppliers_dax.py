#!/usr/bin/env python3
"""
Test script for suppliers DAX query
This script tests the Power BI connection and executes the suppliers DAX query
"""

import sys
import os
import json
from pathlib import Path

# Add parent directory to path to import modules
sys.path.append(str(Path(__file__).parent.parent))

def test_suppliers_dax():
    """Test the suppliers DAX query"""
    try:
        # Import required modules
        from suppliers_etl.services.powerbi.client import PowerBIClient
        from airflow.models import Variable
        
        print("🔍 Testing suppliers DAX query...")
        
        # Get DAX query from Airflow Variables
        dax_queries = Variable.get("dax_queries", deserialize_json=True)
        suppliers_query = dax_queries['suppliers']['query']
        
        print(f"✅ Got suppliers DAX query from Airflow Variables")
        print(f"📝 Query description: {dax_queries['suppliers']['description']}")
        
        # Get dataset configuration
        datasets = Variable.get("datasets", deserialize_json=True)
        suppliers_dataset = datasets['suppliers']
        
        print(f"📊 Dataset ID: {suppliers_dataset['id']}")
        print(f"📊 Source table: {suppliers_dataset['source_table']}")
        print(f"📊 Target table: {suppliers_dataset['target_table']}")
        
        # Initialize PowerBI client
        client = PowerBIClient()
        print("✅ PowerBI client initialized")
        
        # Execute DAX query
        print("🚀 Executing suppliers DAX query...")
        result = client.execute_query(
            dataset_id=suppliers_dataset['id'],
            query=suppliers_query
        )
        
        print(f"✅ Query executed successfully!")
        print(f"📊 Result type: {type(result)}")
        print(f"📊 Result length: {len(result) if result else 0}")
        
        if result:
            print(f"📊 First row: {result[0] if len(result) > 0 else 'No data'}")
            print(f"📊 Columns: {list(result[0].keys()) if result and len(result) > 0 else 'No columns'}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error testing suppliers DAX query: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    print("🧪 Starting suppliers DAX test...")
    result = test_suppliers_dax()
    
    if result:
        print(f"\n🎉 Test completed successfully!")
        print(f"📊 Total rows returned: {len(result)}")
    else:
        print(f"\n💥 Test failed!")
        sys.exit(1)
