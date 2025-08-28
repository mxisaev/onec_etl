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
                    rows_count = len(table.get('rows', []))
                    print(f"Таблица {i+1}: {rows_count} строк")
                    if table.get('rows'):
                        print(f"Первая строка: {table['rows'][0]}")
                        print(f"Колонки: {[col.get('name') for col in table.get('columns', [])]}")
                        
                        # Добавляем детальную информацию о структуре ответа
                        print(f"📊 Структура ответа:")
                        print(f"   • Результатов: {len(result.get('results', []))}")
                        print(f"   • Таблиц в результате 0: {len(tables)}")
                        print(f"   • Строк в таблице {i+1}: {rows_count}")
                        if 'columns' in table:
                            print(f"   • Колонок в таблице {i+1}: {len(table['columns'])}")
            else:
                print("⚠️ Структура ответа не содержит таблиц или строк")
                print(f"📊 Детали ответа: {json.dumps(result, indent=2, ensure_ascii=False)}")
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
    print("=== Диагностика DAX запросов Power BI для партнеров ===")
    
    workspace_id = Variable.get('powerbi_workspace_id')
    dataset_id = 'afb5ea40-5805-4b0b-a082-81ca7333be85'  # ID датасета партнеров
    
    try:
        # Получаем токен
        access_token = get_access_token()
        print("✓ Токен получен успешно")
        
        # Тестируем различные варианты DAX запросов для поставщиков
        test_queries = [
            {
                "name": "Простейший запрос - только EVALUATE",
                "query": "EVALUATE 'УТ_Партнеры'"
            },
            {
                "name": "С ограничением TOPN 10",
                "query": "EVALUATE TOPN(10, 'УТ_Партнеры', 'УТ_Партнеры'[id_1c])"
            },
            {
                "name": "С выбором конкретных колонок",
                "query": "EVALUATE SELECTCOLUMNS('УТ_Партнеры', 'id_1c', 'УТ_Партнеры'[id_1c], 'Партнер.УТ11', 'УТ_Партнеры'[Партнер.УТ11])"
            },
            {
                "name": "С фильтрацией по поставщикам",
                "query": "EVALUATE FILTER('УТ_Партнеры', 'УТ_Партнеры'[is_supplier] = TRUE)"
            },
            {
                "name": "С SUMMARIZECOLUMNS (простой)",
                "query": "EVALUATE SUMMARIZECOLUMNS('УТ_Партнеры'[id_1c], 'УТ_Партнеры'[Партнер.УТ11], 'УТ_Партнеры'[is_supplier])"
            },
            {
                "name": "С SUMMARIZECOLUMNS и TOPN",
                "query": "EVALUATE TOPN(10, SUMMARIZECOLUMNS('УТ_Партнеры'[id_1c], 'УТ_Партнеры'[Партнер.УТ11], 'УТ_Партнеры'[is_supplier]), 'УТ_Партнеры'[id_1c])"
            },
            {
                "name": "Полный запрос для партнеров",
                "query": """
EVALUATE
SUMMARIZECOLUMNS(
    'УТ_Партнеры'[id_1c],
    'УТ_Партнеры'[Партнер.УТ11],
    'УТ_Партнеры'[is_client],
    'УТ_Партнеры'[is_supplier]
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
        query = dax_queries['partners']['query']
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