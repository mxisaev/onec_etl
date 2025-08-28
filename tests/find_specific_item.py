#!/usr/bin/env python3
"""
Поиск конкретного товара в Power BI
"""

import requests
import sys
import json

# Добавляем путь к Airflow
sys.path.append('/opt/airflow')

from airflow.models import Variable

def get_access_token():
    """Получаем access token для Power BI API"""
    tenant_id = Variable.get('powerbi_tenant_id')
    client_id = Variable.get('powerbi_client_id')
    client_secret = Variable.get('powerbi_client_secret')
    
    token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': 'https://analysis.windows.net/powerbi/api'
    }
    
    resp = requests.post(token_url, data=token_data)
    resp.raise_for_status()
    return resp.json()['access_token']

def find_specific_item(access_token, workspace_id, dataset_id, item_number):
    """Ищем конкретный товар по item_number"""
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries'
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # DAX запрос для поиска конкретного товара
    query = f"""
    EVALUATE
    FILTER(
        SUMMARIZECOLUMNS(
            'CompanyProducts'[ID],
            'CompanyProducts'[Description],
            'CompanyProducts'[Brand],
            'CompanyProducts'[Category],
            'CompanyProducts'[item_number],
            "Product Properties",
            VAR CurrentProduct = SELECTEDVALUE('УТ_Номенклатура'[Артикул], "No Product Selected")
            RETURN
            CONCATENATEX(
                TOPN(
                    1000,
                    FILTER(
                        'Char_table',
                        [Артикул] = CurrentProduct
                    ),
                    [SortOrder]
                ),
                [_description] & ": " & [Значение],
                " | ",
                [SortOrder]
            )
        ),
        'CompanyProducts'[item_number] = "{item_number}"
    )
    """
    
    body = {
        "queries": [
            {
                "query": query
            }
        ]
    }
    
    print(f"🔍 Ищем товар с item_number: {item_number}")
    print(f"📊 DAX Query: {query[:200]}...")
    
    try:
        resp = requests.post(url, headers=headers, json=body)
        
        if resp.status_code == 200:
            result = resp.json()
            print(f"✅ УСПЕХ! Статус: {resp.status_code}")
            
            if result.get('results') and result['results'][0].get('tables'):
                tables = result['results'][0]['tables']
                for i, table in enumerate(tables):
                    rows = table.get('rows', [])
                    print(f"📋 Таблица {i+1}: {len(rows)} строк")
                    
                    if rows:
                        for j, row in enumerate(rows):
                            print(f"\n📦 Товар {j+1}:")
                            for key, value in row.items():
                                if key == '[Product Properties]':
                                    print(f"   {key}: {value[:200]}...")
                                else:
                                    print(f"   {key}: {value}")
                    else:
                        print("❌ Товар не найден в Power BI")
            else:
                print("❌ Нет результатов")
                
        else:
            print(f"❌ ОШИБКА! Статус: {resp.status_code}")
            print(f"Ответ: {resp.text}")
            
    except Exception as e:
        print(f"❌ ИСКЛЮЧЕНИЕ: {e}")

def main():
    """Основная функция"""
    print("=== Поиск товара в Power BI ===")
    
    workspace_id = Variable.get('powerbi_workspace_id')
    dataset_id = '022e7796-b30f-44d4-b076-15331e612d47'  # ID датасета
    
    try:
        # Получаем токен
        access_token = get_access_token()
        print("✓ Токен получен успешно")
        
        # Ищем конкретный товар
        item_number = "20.235.A0615.305"
        find_specific_item(access_token, workspace_id, dataset_id, item_number)
        
    except Exception as e:
        print(f"❌ Ошибка в основной функции: {e}")

if __name__ == '__main__':
    main()

