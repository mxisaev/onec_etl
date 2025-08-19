#!/usr/bin/env python3
"""
Простой тест подключения к Power BI для поставщиков
"""

import requests
import sys
import json

# Добавляем путь к Airflow
sys.path.append('/opt/airflow')

from airflow.models import Variable

def test_powerbi_connection():
    """Тестируем подключение к Power BI"""
    print("=== Тест подключения к Power BI для поставщиков ===")
    
    try:
        # Получаем переменные из Airflow
        print("1. Получаем переменные из Airflow...")
        
        client_id = Variable.get('powerbi_client_id')
        print(f"   ✓ powerbi_client_id: {client_id[:10]}...")
        
        client_secret = Variable.get('powerbi_client_secret')
        print(f"   ✓ powerbi_client_secret: {client_secret[:10]}...")
        
        tenant_id = Variable.get('powerbi_tenant_id')
        print(f"   ✓ powerbi_tenant_id: {tenant_id}")
        
        workspace_id = Variable.get('powerbi_workspace_id')
        print(f"   ✓ powerbi_workspace_id: {workspace_id}")
        
        print("   ✅ Все переменные получены успешно!")
        
    except Exception as e:
        print(f"   ❌ Ошибка получения переменных: {e}")
        return False
    
    try:
        # Получаем access token
        print("\n2. Получаем access token...")
        
        token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'resource': 'https://analysis.windows.net/powerbi/api'
        }
        
        token_response = requests.post(token_url, data=token_data)
        
        if token_response.status_code != 200:
            print(f"   ❌ Ошибка получения токена: {token_response.status_code}")
            print(f"   Ответ: {token_response.text}")
            return False
        
        token_json = token_response.json()
        access_token = token_json.get('access_token')
        print("   ✅ Токен получен успешно!")
        
    except Exception as e:
        print(f"   ❌ Ошибка получения токена: {e}")
        return False
    
    try:
        # Тестируем подключение к workspace
        print("\n3. Тестируем подключение к workspace...")
        
        workspace_url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}'
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        workspace_response = requests.get(workspace_url, headers=headers)
        
        if workspace_response.status_code != 200:
            print(f"   ❌ Ошибка доступа к workspace: {workspace_response.status_code}")
            print(f"   Ответ: {workspace_response.text}")
            return False
        
        workspace_info = workspace_response.json()
        print(f"   ✅ Workspace доступен: {workspace_info.get('name', 'N/A')}")
        
    except Exception as e:
        print(f"   ❌ Ошибка доступа к workspace: {e}")
        return False
    
    try:
        # Тестируем доступ к датасету поставщиков
        print("\n4. Тестируем доступ к датасету поставщиков...")
        
        dataset_id = 'afb5ea40-5805-4b0b-a082-81ca7333be85'
        dataset_url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}'
        
        dataset_response = requests.get(dataset_url, headers=headers)
        
        if dataset_response.status_code != 200:
            print(f"   ❌ Ошибка доступа к датасету: {dataset_response.status_code}")
            print(f"   Ответ: {dataset_response.text}")
            return False
        
        dataset_info = dataset_response.json()
        print(f"   ✅ Датасет доступен: {dataset_info.get('name', 'N/A')}")
        
        # Показываем таблицы в датасете
        tables = dataset_info.get('tables', [])
        if tables:
            print(f"   📊 Найдено таблиц: {len(tables)}")
            for table in tables:
                print(f"      - {table.get('name', 'N/A')}")
        else:
            print("   ⚠️  Таблицы не найдены")
        
    except Exception as e:
        print(f"   ❌ Ошибка доступа к датасету: {e}")
        return False
    
    print("\n🎉 Тест подключения завершен успешно!")
    return True

if __name__ == '__main__':
    success = test_powerbi_connection()
    if not success:
        sys.exit(1)
