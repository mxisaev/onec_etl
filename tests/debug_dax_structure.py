#!/usr/bin/env python3
"""
Debug script to analyze Power BI response structure with new DAX query
"""

import json
import requests
from airflow.models import Variable

def get_access_token():
    """Get Power BI access token using MSAL"""
    try:
        import msal
        
        # Get credentials from Airflow Variables
        client_id = Variable.get('powerbi_client_id')
        client_secret = Variable.get('powerbi_client_secret')
        tenant_id = Variable.get('powerbi_tenant_id')
        
        # Initialize MSAL client
        app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}"
        )
        
        # Get token
        result = app.acquire_token_for_client(
            scopes=["https://analysis.windows.net/powerbi/api/.default"]
        )
        
        if "access_token" not in result:
            raise ValueError(f"Failed to acquire token: {result}")
            
        return result["access_token"]
        
    except Exception as e:
        print(f"❌ Ошибка получения токена: {e}")
        raise

def test_dax_structure():
    """Test DAX query and analyze response structure"""
    try:
        # Get credentials
        access_token = get_access_token()
        workspace_id = Variable.get('powerbi_workspace_id')
        dataset_id = '022e7796-b30f-44d4-b076-15331e612d47'
        
        # Get DAX from Airflow Variables
        dax_queries = json.loads(Variable.get('dax_queries'))
        query = dax_queries['company_products']['query']
        
        print("=== Тест структуры ответа Power BI ===")
        print(f"DAX запрос: {query[:100]}...")
        
        # Execute query
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        body = {
            "queries": [{
                "query": query,
                "kind": "DAX"
            }],
            "serializerSettings": {
                "includeNulls": True
            }
        }
        
        response = requests.post(url, headers=headers, json=body)
        
        print(f"Статус: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ Ошибка: {response.text}")
            return
            
        # Parse response
        result = response.json()
        
        print("\n=== Структура ответа ===")
        print(f"Ключи верхнего уровня: {list(result.keys())}")
        
        if 'results' in result:
            print(f"Количество results: {len(result['results'])}")
            
            for i, res in enumerate(result['results']):
                print(f"\n--- Result {i} ---")
                print(f"Ключи: {list(res.keys())}")
                
                if 'tables' in res:
                    print(f"Количество tables: {len(res['tables'])}")
                    
                    for j, table in enumerate(res['tables']):
                        print(f"\n--- Table {j} ---")
                        print(f"Ключи: {list(table.keys())}")
                        
                        if 'rows' in table:
                            print(f"Количество rows: {len(table['rows'])}")
                            if table['rows']:
                                print(f"Первая строка: {table['rows'][0]}")
                        else:
                            print("❌ Нет ключа 'rows'")
                else:
                    print("❌ Нет ключа 'tables'")
        else:
            print("❌ Нет ключа 'results'")
            
        # Check if structure matches ETL expectations
        print("\n=== Проверка структуры для ETL ===")
        
        if 'results' in result and result['results']:
            if 'tables' in result['results'][0] and result['results'][0]['tables']:
                if 'rows' in result['results'][0]['tables'][0]:
                    print("✅ Структура подходит для ETL")
                    rows = result['results'][0]['tables'][0]['rows']
                    print(f"✅ Количество строк: {len(rows)}")
                    
                    # Check for specific item
                    for row in rows:
                        if isinstance(row, dict) and 'CompanyProducts[item_number]' in row:
                            if row['CompanyProducts[item_number]'] == '20.235.A0615.305':
                                print(f"✅ Найден товар 20.235.A0615.305: {row}")
                                break
                    else:
                        print("❌ Товар 20.235.A0615.305 не найден")
                        
                else:
                    print("❌ Нет ключа 'rows' в первой таблице")
            else:
                print("❌ Нет ключа 'tables' в первом результате")
        else:
            print("❌ Нет ключа 'results' или он пустой")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_dax_structure()

