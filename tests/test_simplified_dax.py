#!/usr/bin/env python3
"""
Тест упрощенного DAX запроса для партнеров
"""

import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_simplified_dax():
    try:
        from suppliers_etl.services.powerbi.client import PowerBIClient
        from suppliers_etl.dax_prepare.raw_queries import PARTNERS_QUERY
        
        print("🧪 Тестируем упрощенный DAX запрос...")
        print(f"📝 Запрос:\n{PARTNERS_QUERY}")
        
        # Создаем клиент PowerBI
        client = PowerBIClient()
        
        # Получаем токен
        token = client._get_access_token()
        if not token:
            print("❌ Не удалось получить токен")
            return None
        
        print("✅ Токен получен успешно")
        
        # Получаем dataset_id из Airflow Variables
        from airflow.models import Variable
        datasets = Variable.get('datasets', deserialize_json=True)
        partners_dataset = datasets['partners']
        dataset_id = partners_dataset['id']
        
        print(f"📊 Dataset ID: {dataset_id}")
        
        # Выполняем запрос
        print("🚀 Выполняем DAX запрос...")
        result = client.execute_query(dataset_id, PARTNERS_QUERY)
        
        print(f"✅ УСПЕХ! Результат: {len(result)} строк")
        if result:
            print(f"📋 Первая строка: {result[0]}")
            print(f"📊 Колонки: {list(result[0].keys())}")
        
        return result
        
    except Exception as e:
        print(f"❌ ОШИБКА: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    print("🧪 Запуск теста упрощенного DAX запроса...")
    result = test_simplified_dax()
    if result:
        print(f"🎉 Тест прошел успешно! Получено {len(result)} строк")
    else:
        print("�� Тест не прошел")
