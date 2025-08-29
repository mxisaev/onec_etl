#!/usr/bin/env python3
"""
Company Products ETL DAG
ETL процесс для извлечения данных о товарах компании из Power BI и загрузки в PostgreSQL
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
from utils.logger import get_logger

# Создаем logger для конкретного DAG'а
logger = get_logger("company_products_etl", "oneC_etl")

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
    'CompanyProductsETL',
    default_args=default_args,
    description='ETL процесс для извлечения данных о товарах компании из Power BI и загрузки в PostgreSQL',
    schedule_interval='35 4-13 * * 1-5',  # Пн-Пт каждые 35 минут с 4:00 до 13:00 UTC (9:00-18:00 UTC+5)
    catchup=False,
    tags=['etl', 'powerbi', 'postgres', 'company_products', 'product_properties']
)

def extract_powerbi_data_task(**context):
    """Извлечение данных о товарах из Power BI через DAX"""
    try:
        logger.info("🔄 Начинаем извлечение данных о товарах из Power BI...")
        
        from oneC_etl.tasks.extract import extract_powerbi_data
        
        # Конфигурация задачи согласно документации
        task_config = {
            'dataset_id': '022e7796-b30f-44d4-b076-15331e612d47',  # 1cExportDataset (правильный датасет)
            'dax_query': 'company_products',  # Загружается из Airflow Variables
            'columns': {
                'CompanyProducts[ID]': 'id',
                'CompanyProducts[Description]': 'description',
                'CompanyProducts[Brand]': 'brand',
                'CompanyProducts[Category]': 'category',
                'CompanyProducts[Withdrawn_from_range]': 'withdrawn_from_range',
                'CompanyProducts[item_number]': 'item_number',
                '[Product_Properties]': 'product_properties',
                'УТ_Товарные категории[_description]': 'product_category',
                'УТ_РСвДополнительныеСведения2_0[Под заказ]': 'on_order',
                'Выводится_без_остатков': 'is_vector',
                'CountRowsУТ_РСвДополнительныеСведения2_0': 'count_rows'
            }
        }
        
        result = extract_powerbi_data(task_config)
        return result
        
    except Exception as e:
        logger.exception(f"❌ Ошибка извлечения данных: {str(e)}")
        raise

def load_to_postgres_task(**context):
    """Загрузка данных о товарах в PostgreSQL"""
    try:
        logger.info("🔄 Начинаем загрузку данных в PostgreSQL...")
        
        from oneC_etl.tasks.load import execute_etl_task
        
        # Получаем данные из предыдущей задачи
        ti = context['ti']
        data = ti.xcom_pull(task_ids='extract_powerbi_data')
        
        if data is None:
            raise ValueError("Нет данных для загрузки")
        
        # Преобразуем список словарей в pandas DataFrame
        import pandas as pd
        df = pd.DataFrame(data)
        
        # Конфигурация загрузки согласно документации
        task_config = {
            'source_table': 'powerbi_company_products',
            'target_table': 'companyproducts',
            'mapping_name': 'company_products'
        }
        
        result = execute_etl_task(df, task_config)
        return result
        
    except Exception as e:
        logger.exception(f"❌ Ошибка загрузки данных: {str(e)}")
        raise



def validate_data_task(**context):
    """Валидация загруженных данных о товарах"""
    try:
        logger.info("🔄 Начинаем валидацию данных о товарах...")
        
        from oneC_etl.services.postgres.client import PostgresClient
        
        db_client = PostgresClient()
        
        # Проверяем количество загруженных товаров
        products_query = """
        SELECT 
            COUNT(*) as total_products,
            COUNT(CASE WHEN description IS NOT NULL THEN 1 END) as products_with_description,
            COUNT(CASE WHEN brand IS NOT NULL THEN 1 END) as products_with_brand,
            COUNT(CASE WHEN category IS NOT NULL THEN 1 END) as products_with_category,
            COUNT(CASE WHEN item_number IS NOT NULL THEN 1 END) as products_with_item_number
        FROM companyproducts
        """
        
        products_result = db_client.execute_query(products_query)
        
        if products_result:
            products_stats = products_result[0]
            
            # Проверяем качество данных
            validation_status = "success"
            if products_stats['total_products'] == 0:
                validation_status = "error"
                logger.error("❌ В таблице нет товаров")
            elif products_stats['products_with_description'] == 0:
                validation_status = "warning"
                logger.warning("⚠️ У товаров нет описаний")
            elif products_stats['products_with_brand'] == 0:
                logger.warning("⚠️ У товаров нет брендов")
            else:
                logger.info("✅ Данные о товарах загружены корректно")
            
            # Получаем результаты очистки из предыдущей задачи
            ti = context['ti']
            cleanup_result = ti.xcom_pull(task_ids='cleanup_orphaned_records')
            
            if cleanup_result and cleanup_result.get('status') == 'success':
                logger.info(f"🗑️ Очистка: удалено {cleanup_result.get('deleted_records', 0)} устаревших записей")
            
            result = {
                "status": validation_status,
                "products": products_stats,
                "cleanup": cleanup_result,
                "message": f"Проверено {products_stats['total_products']} товаров"
            }
            
            # Добавляем итоговую строку
            logger.info("🎯 ========================================")
            logger.info(f"📊 Итоговая статистика:")
            logger.info(f"   - Всего товаров: {products_stats['total_products']}")
            if cleanup_result and cleanup_result.get('status') == 'success':
                logger.info(f"   - Удалено устаревших: {cleanup_result.get('deleted_records', 0)}")
            logger.info("🎯 ========================================")
            
            return result
        else:
            logger.warning("⚠️ Не удалось получить результаты валидации")
            return {"status": "error", "message": "Validation failed"}
        
    except Exception as e:
        logger.exception(f"❌ Ошибка валидации: {str(e)}")
        raise

def cleanup_orphaned_records_task(**context):
    """Удаление записей, которых нет в новой выгрузке PowerBI"""
    try:
        logger.info("🔄 Начинаем очистку устаревших записей...")
        
        from oneC_etl.tasks.cleanup import cleanup_orphaned_records
        
        # Получаем данные из предыдущей задачи
        ti = context['ti']
        data = ti.xcom_pull(task_ids='extract_powerbi_data')
        
        if data is None:
            raise ValueError("Нет данных для очистки")
        
        # Конфигурация очистки
        cleanup_config = {
            'source_table': 'powerbi_company_products',
            'target_table': 'companyproducts',
            'key_column': 'id'  # Используем 'id' как в PowerBI данных
        }
        
        result = cleanup_orphaned_records(data, cleanup_config)
        return result
        
    except Exception as e:
        logger.exception(f"❌ Ошибка очистки устаревших записей: {str(e)}")
        raise

# Создание задач согласно архитектуре из документации
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

cleanup_operator = PythonOperator(
    task_id='cleanup_orphaned_records',
    python_callable=cleanup_orphaned_records_task,
    dag=dag
)

# Настройка зависимостей согласно потоку данных:
# 1. Извлечение данных из Power BI через DAX
# 2. Загрузка в таблицу companyproducts
# 3. Валидация загруженных данных о товарах
# 4. Очистка устаревших записей
extract_operator >> load_operator >> validate_operator >> cleanup_operator

if __name__ == "__main__":
    dag.cli()
