#!/usr/bin/env python3
"""
Test script for partners DAX query
This script tests the Power BI connection and executes the partners DAX query
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_partners_dax():
    """Test the partners DAX query"""
    try:
        from airflow.models import Variable
        from suppliers_etl.services.powerbi.client import PowerBIClient
        
        print("🔍 Testing partners DAX query...")
        
        # Получаем DAX запрос из Airflow Variables
        dax_queries = Variable.get('dax_queries', deserialize_json=True)
        partners_query = dax_queries['partners']['query']
        
        print(f"✅ Got partners DAX query from Airflow Variables")
        print(f"📝 Query description: {dax_queries['partners']['description']}")
        
        # Получаем конфигурацию dataset
        datasets = Variable.get('datasets', deserialize_json=True)
        partners_dataset = datasets['partners']
        
        print(f"📊 Dataset ID: {partners_dataset['id']}")
        print(f"📊 Source table: {partners_dataset['source_table']}")
        print(f"📊 Target table: {partners_dataset['target_table']}")
        
        # Инициализируем PowerBI клиент
        client = PowerBIClient()
        
        # Сначала протестируем простой запрос
        print("🧪 Testing simple DAX query first...")
        simple_query = "EVALUATE ROW(\"Test\", \"Hello World\")"
        
        try:
            simple_result = client.execute_query(
                dataset_id=partners_dataset['id'],
                query=simple_query
            )
            print(f"✅ Simple query works! Result: {simple_result}")
        except Exception as e:
            print(f"❌ Simple query failed: {e}")
            return
        
        print("🚀 Executing partners DAX query...")
        
        # Выполняем запрос
        result = client.execute_query(
            dataset_id=partners_dataset['id'],
            query=partners_query
        )
        
        if result:
            print(f"✅ Query executed successfully")
            print(f"📊 Rows returned: {len(result)}")
            
            if result:
                print(f"📋 Sample data:")
                for i, row in enumerate(result[:3]):
                    print(f"   Row {i+1}: {row}")
        else:
            print("⚠️ No data returned from query")
            
    except Exception as e:
        print(f"❌ Error testing partners DAX query: {str(e)}")
        raise

if __name__ == "__main__":
    print("🧪 Starting partners DAX test...")
    result = test_partners_dax()
