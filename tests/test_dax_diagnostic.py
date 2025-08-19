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

def test_dax_query(access_token, workspace_id, dataset_id, query, query_name):
    """Тестируем DAX запрос и возвращаем результат"""
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries'
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    body = {
        "queries": [
            {
                "query": query
            }
        ]
    }
    
    print(f"\n=== Тест: {query_name} ===")
    print(f"Запрос: {query}")
    
    try:
        resp = requests.post(url, headers=headers, json=body)
        
        if resp.status_code == 200:
            result = resp.json()
            print(f"✅ УСПЕХ! Статус: {resp.status_code}")
            print(f"Результат: {len(result.get('results', []))} записей")
            
            # Показываем первые несколько записей
            if result.get('results') and result['results'][0].get('tables'):
                tables = result['results'][0]['tables']
                for i, table in enumerate(tables):
                    print(f"Таблица {i+1}: {len(table.get('rows', []))} строк")
                    if table.get('rows'):
                        print(f"Первая строка: {table['rows'][0]}")
                        print(f"Колонки: {[col.get('name') for col in table.get('columns', [])]}")
        else:
            print(f"❌ ОШИБКА! Статус: {resp.status_code}")
            print(f"Ответ: {resp.text}")
            
            # Пытаемся получить больше деталей об ошибке
            try:
                error_details = resp.json()
                print(f"Детали ошибки: {json.dumps(error_details, indent=2, ensure_ascii=False)}")
            except:
                print(f"Текст ошибки: {resp.text}")
                
    except Exception as e:
        print(f"❌ ИСКЛЮЧЕНИЕ: {e}")

def main():
    """Основная функция диагностики"""
    print("=== Диагностика DAX запросов Power BI для поставщиков ===")
    
    workspace_id = Variable.get('powerbi_workspace_id')
    dataset_id = 'afb5ea40-5805-4b0b-a082-81ca7333be85'  # ID датасета поставщиков
    
    try:
        # Получаем токен
        access_token = get_access_token()
        print("✓ Токен получен успешно")
        
        # Тестируем различные варианты DAX запросов для поставщиков
        test_queries = [
            {
                "name": "Простейший запрос - только EVALUATE",
                "query": "EVALUATE 'Suppliers'"
            },
            {
                "name": "С ограничением TOPN 10",
                "query": "EVALUATE TOPN(10, 'Suppliers', 'Suppliers'[ID])"
            },
            {
                "name": "С выбором конкретных колонок",
                "query": "EVALUATE SELECTCOLUMNS('Suppliers', 'ID', 'Suppliers'[ID], 'Name', 'Suppliers'[Name])"
            },
            {
                "name": "С фильтрацией",
                "query": "EVALUATE FILTER('Suppliers', 'Suppliers'[Status] = \"active\")"
            },
            {
                "name": "С SUMMARIZECOLUMNS (простой)",
                "query": "EVALUATE SUMMARIZECOLUMNS('Suppliers'[ID], 'Suppliers'[Name], 'Suppliers'[Code])"
            },
            {
                "name": "С SUMMARIZECOLUMNS и TOPN",
                "query": "EVALUATE TOPN(10, SUMMARIZECOLUMNS('Suppliers'[ID], 'Suppliers'[Name], 'Suppliers'[Code]), 'Suppliers'[ID])"
            },
            {
                "name": "Полный запрос для поставщиков",
                "query": """
EVALUATE
SUMMARIZECOLUMNS(
    'Suppliers'[ID],
    'Suppliers'[Name],
    'Suppliers'[Code],
    'Suppliers'[INN],
    'Suppliers'[MainManager],
    'Suppliers'[Status],
    'Suppliers'[CreatedAt],
    'Suppliers'[UpdatedAt]
)
"""
            }
        ]
        
        for test_query in test_queries:
            test_dax_query(
                access_token, 
                workspace_id, 
                dataset_id, 
                test_query["query"], 
                test_query["name"]
            )
        
        # Новый тест: DAX из Airflow Variable
        dax_queries = json.loads(Variable.get('dax_queries'))
        query = dax_queries['suppliers']['query']
        test_dax_query(
            access_token,
            workspace_id,
            dataset_id,
            query,
            "suppliers из Airflow Variable (актуальный DAX)"
        )
        
    except Exception as e:
        print(f"❌ Ошибка в основной функции: {e}")

if __name__ == '__main__':
    main() 