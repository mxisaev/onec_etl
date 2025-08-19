#!/usr/bin/env python3
"""
PowerBI to PostgreSQL ETL DAG
ETL процесс для загрузки данных из PowerBI в PostgreSQL
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# 🎯 КРИТИЧЕСКИ ВАЖНО: Настройка путей для utils.logger
dags_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dags_root not in sys.path:
    sys.path.insert(0, dags_root)

# 🎯 Импортируем utils.logger
from utils.logger import logger

# Настройка DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'PowerBI2PostgresETL',
    default_args=default_args,
    description='ETL процесс для загрузки данных из PowerBI в PostgreSQL',
    schedule_interval='0 10 * * 1-5',  # Пн-Пт в 10:00 UTC
    catchup=False,
    tags=['etl', 'powerbi', 'postgres', 'data_warehouse']
)

def extract_powerbi_data_task(**context):
    """Извлечение данных из PowerBI"""
    try:
        logger.info("🔄 Начинаем извлечение данных из PowerBI...")
        
        from oneC_etl.tasks.extract import extract_powerbi_data
        
        # Конфигурация задачи
        task_config = {
            'dataset_id': 'afb5ea40-5805-4b0b-a082-81ca7333be85',
            'dax_query': 'company_products',  # Будет загружен из Airflow Variables
            'columns': {
                'CompanyProducts[ID]': 'id',
                'CompanyProducts[Description]': 'description',
                'CompanyProducts[Brand]': 'brand',
                'CompanyProducts[Category]': 'category',
                'CompanyProducts[Withdrawn_from_range]': 'withdrawn_from_range',
                'CompanyProducts[item_number]': 'item_number'
            }
        }
        
        result = extract_powerbi_data(task_config)
        
        logger.info(f"✅ Извлечение завершено: {len(result)} строк")
        return result
        
    except Exception as e:
        logger.exception(f"❌ Ошибка извлечения данных: {str(e)}")
        raise

def load_to_postgres_task(**context):
    """Загрузка данных в PostgreSQL"""
    try:
        logger.info("🔄 Начинаем загрузку данных в PostgreSQL...")
        
        from oneC_etl.tasks.load import execute_etl_task
        
        # Получаем данные из предыдущей задачи
        ti = context['ti']
        data = ti.xcom_pull(task_ids='extract_powerbi_data')
        
        if data is None:
            raise ValueError("Нет данных для загрузки")
        
        # Конфигурация загрузки
        task_config = {
            'source_table': 'powerbi_data',
            'target_table': 'postgres_data',
            'mapping_name': 'company_products'
        }
        
        result = execute_etl_task(data, task_config)
        
        logger.info(f"✅ Загрузка завершена: {result}")
        return result
        
    except Exception as e:
        logger.exception(f"❌ Ошибка загрузки данных: {str(e)}")
        raise

def validate_data_task(**context):
    """Валидация загруженных данных"""
    try:
        logger.info("🔄 Начинаем валидацию данных...")
        
        from oneC_etl.database.client import DatabaseClient
        
        db_client = DatabaseClient()
        
        # Проверяем количество загруженных записей
        validation_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT id) as unique_ids,
            COUNT(CASE WHEN description IS NOT NULL THEN 1 END) as records_with_description
        FROM postgres_data
        """
        
        result = db_client.execute_query(validation_query)
        
        if result and len(result) > 0:
            stats = result[0]
            logger.info(f"📊 Результаты валидации: {stats}")
            
            # Проверяем качество данных
            if stats['total_records'] > 0 and stats['unique_ids'] == stats['total_records']:
                logger.info("✅ Валидация прошла успешно")
                return {"status": "success", "stats": stats}
            else:
                logger.warning("⚠️ Обнаружены проблемы с данными")
                return {"status": "warning", "stats": stats}
        else:
            logger.error("❌ Не удалось получить результаты валидации")
            return {"status": "error", "message": "Validation failed"}
            
    except Exception as e:
        logger.exception(f"❌ Ошибка валидации: {str(e)}")
        raise

# Создание задач
extract_operator = PythonOperator(
    task_id='extract_powerbi_data',
    python_callable=extract_powerbi_data_task,
    dag=dag
)

load_operator = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres_task,
    dag=dag
)

validate_operator = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data_task,
    dag=dag
)

# Настройка зависимостей
extract_operator >> load_operator >> validate_operator

if __name__ == "__main__":
    dag.cli()
