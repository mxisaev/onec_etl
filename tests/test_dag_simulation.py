#!/usr/bin/env python3
"""
Test script that EXACTLY simulates what the main DAG does
This test calls extract_powerbi_data with the same task_config as the DAG
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_dag_simulation():
    """Test that EXACTLY simulates the main DAG execution"""
    try:
        from airflow.models import Variable
        from suppliers_etl.tasks.extract import extract_powerbi_data
        
        print("🧪 Testing DAG simulation - EXACT same code as main DAG...")
        
        # Получаем конфигурацию dataset из Airflow Variables (точно как в DAG)
        datasets = Variable.get('datasets', deserialize_json=True)
        partners_dataset = datasets['partners']
        
        print(f"📊 Dataset config: {partners_dataset}")
        
        # Конфигурация задачи согласно документации (точно как в DAG)
        task_config = {
            'dataset_id': partners_dataset['id'],  # Получаем из переменной datasets
            'dax_query_key': 'partners',  # Используем ключ 'partners' из Airflow Variables
            'columns': {
                'УТ_Партнеры[Партнер.УТ11]': 'partner',
                'УТ_Контактные лица партнеров[Контактное лицо]': 'contact',
                'УТ_Контактные лица партнеров[email]': 'contact_email',
                'УТ_Пользователи[_description]': 'responsible_manager',
                'УТ_Партнеры[id_1c]': 'id_1c_partner',
                'УТ_Партнеры[is_client]': 'is_client',
                'УТ_Партнеры[is_supplier]': 'is_supplier',
                'УТ_Контактные лица партнеров[Роль]': 'role',
                'УТ_Контактные лица партнеров[id_1c]': 'id_1c_contact'
            }
        }
        
        print(f"🔧 Task config: {task_config}")
        print(f"🚀 Calling extract_powerbi_data with EXACT same parameters as DAG...")
        
        # Вызываем точно ту же функцию, что и DAG
        result = extract_powerbi_data(task_config)
        
        print(f"✅ SUCCESS! DAG simulation completed: {len(result)} rows")
        if result:
            print(f"📋 Sample data: {result[0]}")
        
        return result
        
    except Exception as e:
        print(f"❌ DAG simulation FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    print("🧪 Starting DAG simulation test...")
    result = test_dag_simulation()


