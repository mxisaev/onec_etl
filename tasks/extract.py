#!/usr/bin/env python3
"""
Модуль для извлечения данных из Power BI через DAX запросы
"""

import sys
import os
import json
from typing import Dict, List, Any
from datetime import datetime

# 🎯 КРИТИЧЕСКИ ВАЖНО: Настройка путей для utils.logger
# Для модулей tasks/services (уровень 2) нужен путь к docker/dags/
dags_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if dags_root not in sys.path:
    sys.path.insert(0, dags_root)

from utils.logger import get_logger

# Пишем в основной лог DAG, без отдельного файла для модуля
logger = get_logger("suppliers_etl", "suppliers_etl")



def extract_powerbi_data(task_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Извлекает данные из Power BI через DAX запрос
    
    Args:
        task_config: Конфигурация задачи с dataset_id, dax_query и columns
        
    Returns:
        Список словарей с данными
    """
    try:
        # Получаем параметры из конфигурации
        dataset_id = task_config.get('dataset_id')
        dax_query_key = task_config.get('dax_query_key')  # Ключ для получения DAX из переменных
        columns_mapping = task_config.get('columns', {})
        
        if not dataset_id or not dax_query_key:
            raise ValueError("Не указаны dataset_id или dax_query_key в конфигурации")
        
        # Получаем DAX запрос из переменных Airflow
        from airflow.models import Variable
        dax_queries = Variable.get('dax_queries')
        dax_queries_dict = json.loads(dax_queries) if isinstance(dax_queries, str) else dax_queries
        
        if dax_query_key not in dax_queries_dict:
            raise ValueError(f"DAX запрос '{dax_query_key}' не найден в переменной dax_queries")
        
        actual_dax_query = dax_queries_dict[dax_query_key]['query']
        
        # Используем PowerBI клиент из suppliers_etl
        from suppliers_etl.services.powerbi.client import PowerBIClient
        
        # Инициализируем клиент (он автоматически получит все переменные)
        client = PowerBIClient()
        
        # Выполняем DAX запрос
        raw_data = client.execute_query(dataset_id, actual_dax_query)
        
        if not raw_data:
            logger.warning("⚠️ Данные не получены из Power BI")
            return []
        
        # Трансформируем данные согласно маппингу колонок
        transformed_data = []
        for row in raw_data:
            transformed_row = {}
            
            for powerbi_column, target_column in columns_mapping.items():
                if powerbi_column in row:
                    transformed_row[target_column] = row[powerbi_column]
                else:
                    transformed_row[target_column] = None
            
            # Добавляем timestamp
            transformed_row['extracted_at'] = datetime.utcnow().isoformat()
            transformed_data.append(transformed_row)
        
        return transformed_data
        
    except Exception as e:
        logger.exception(f"❌ Ошибка извлечения данных из Power BI: {str(e)}")
        raise

if __name__ == "__main__":
    # Тестирование модуля
    test_config = {
        'dataset_id': 'test-dataset-id',
        'dax_query': 'EVALUATE CompanyProducts',
        'columns': {
            'CompanyProducts[ID]': 'id',
            'CompanyProducts[Description]': 'description'
        }
    }
    
    try:
        result = extract_powerbi_data(test_config)
        print(f"Тест успешен: получено {len(result)} строк")
    except Exception as e:
        print(f"Тест не прошел: {e}")
        print("Примечание: для тестирования нужны переменные Airflow (powerbi_tenant_id, powerbi_client_id, powerbi_client_secret, powerbi_workspace_id)")
