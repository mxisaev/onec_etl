"""
Company Products ETL DAG
-----------------------
ETL DAG for syncing company products from Power BI to PostgreSQL.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from loguru import logger
import re
import sys
import os

# Добавляем путь к проекту color_processing для импорта миграции
sys.path.append('/var/www/vhosts/itland.uk/docker/dags/color_processing')

from oneC_etl.tasks.extract import extract_powerbi_data
from oneC_etl.tasks.load import execute_etl_task
from oneC_etl.config.variables import get_dataset_config
from oneC_etl.utils.dax_utils import get_business_columns_from_dax, normalize_column_name

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

def migrate_product_properties_task(**context):
    """Migrate product_properties from companyproducts to product_properties table"""
    try:
        logger.info("🔄 Начинаем миграцию product_properties...")
        
        # Импортируем функцию миграции из color_processing
        from database.scripts.migrate_product_properties import migrate_product_properties, create_product_properties_table
        
        # Создаем таблицу если её нет
        logger.info("📋 Создаем таблицу product_properties если её нет...")
        if not create_product_properties_table():
            raise Exception("Не удалось создать таблицу product_properties")
        
        # Выполняем миграцию
        logger.info("🔄 Мигрируем данные...")
        if not migrate_product_properties():
            raise Exception("Не удалось мигрировать данные product_properties")
        
        logger.success("✅ Миграция product_properties завершена успешно!")
        
        # Сохраняем результат в XCom
        context['ti'].xcom_push(key='migration_result', value='success')
        return 'success'
        
    except Exception as e:
        logger.error(f"❌ Ошибка миграции product_properties: {e}")
        context['ti'].xcom_push(key='migration_result', value='failed')
        raise

def process_datasets_task(**context):
    """Process Power BI datasets and prepare ETL tasks"""
    try:
        logger.info("=== Starting process_datasets_task ===")
        
        # Get dataset configuration
        dataset_name = 'company_products'
        dataset_config = get_dataset_config(dataset_name)
        
        # Get DAX query from Airflow Variables
        dax_queries = Variable.get("dax_queries", deserialize_json=True)
        query_config = dax_queries[dataset_name]
        dax_query = query_config["query"]

        # Формируем маппинг колонок из DAX (используем исправленную функцию)
        dax_columns = get_business_columns_from_dax(dax_query)
        
        # Создаем маппинг: оригинальные имена -> нормализованные имена
        # Оригинальные имена из DAX: ID, Description, Brand, Category, Withdrawn_from_range, item_number, Product Properties
        # Нормализованные имена: id, description, brand, category, withdrawn_from_range, item_number, product_properties
        original_names = ['ID', 'Description', 'Brand', 'Category', 'Withdrawn_from_range', 'item_number', 'Product Properties']
        
        columns_mapping = {}
        for i, original_name in enumerate(original_names):
            if i < len(dax_columns):
                columns_mapping[original_name] = dax_columns[i]

        logger.info(f"Автоматически сгенерированный маппинг колонок: {columns_mapping}")

        tasks = [{
            "dataset_id": dataset_config["id"],
            "source_table": dataset_config["source_table"],
            "target_table": dataset_config["target_table"],
            "dax_query": dax_query,
            "mapping_name": "company_products",
            "columns": columns_mapping
        }]
        
        logger.info(f"Created ETL task for dataset '{dataset_name}'")
        
        # Push to XCom
        context['ti'].xcom_push(key='etl_tasks', value=tasks)
        return tasks
        
    except Exception as e:
        logger.exception("Error in process_datasets_task")
        raise

def execute_etl_task_wrapper(**context):
    """Wrapper function to handle data extraction and loading"""
    try:
        # Get tasks from XCom
        tasks = context['ti'].xcom_pull(task_ids='etl_tasks.process_datasets', key='etl_tasks')
        
        if not tasks:
            raise ValueError("No ETL tasks found in XCom")
            
        operations = []
        
        for task in tasks:
            try:
                # Extract data
                data = extract_powerbi_data(task)
                
                # Load data
                result = execute_etl_task(data, task)
                operations.append(result)
                
            except Exception as e:
                logger.exception(f"Error processing task {task['source_table']}")
                operations.append({
                    'source': task['source_table'],
                    'target': task['target_table'],
                    'error': str(e),
                    'status': 'failed'
                })
        
        # Push operations to XCom
        context['ti'].xcom_push(key='etl_operations', value=operations)
        return operations
        
    except Exception as e:
        logger.exception("Error in execute_etl_task_wrapper")
        raise

def check_results(**context):
    """Generate summary report of ETL operations"""
    try:
        operations = context['ti'].xcom_pull(
            task_ids='etl_tasks.execute_etl',
            key='etl_operations'
        )
        
        migration_result = context['ti'].xcom_pull(
            task_ids='etl_tasks.migrate_product_properties',
            key='migration_result'
        )
        
        if not operations:
            logger.warning("No operations found in XCom")
            return
        
        # Calculate statistics
        total_ops = len(operations)
        successful_ops = len([op for op in operations if op['status'] == 'success'])
        failed_ops = len([op for op in operations if op['status'] == 'failed'])
        total_records = sum(op.get('records', 0) for op in operations if op['status'] == 'success')
        total_updated = sum(op.get('updated', 0) for op in operations if op['status'] == 'success')
        
        # Generate report
        report = f"""
ETL Operations Summary:
----------------------
Total Operations: {total_ops}
Successful: {successful_ops}
Failed: {failed_ops}
Total Records Processed: {total_records}
Total Records Updated: {total_updated}

Product Properties Migration:
---------------------------
Status: {'✅ SUCCESS' if migration_result == 'success' else '❌ FAILED'}

Detailed Results:
----------------"""
        
        for op in operations:
            if op['status'] == 'success':
                report += f"\n✓ {op['source']} → {op['target']}: {op.get('records', 0)} records processed, {op.get('updated', 0)} updated"
            else:
                report += f"\n✗ {op['source']} → {op['target']}: FAILED - {op.get('error', 'Unknown error')}"
        
        logger.info(report)
        context['ti'].xcom_push(key='etl_report', value=report)
        
    except Exception as e:
        logger.exception("Error in check_results")
        raise

# Create the DAG
with DAG(
    'CompanyProductsETL',  # Specific name for company products
    default_args=default_args,
    description='ETL for company products from Power BI to PostgreSQL',
    schedule_interval='0 8 * * *',  # Run at 8 AM local time (UTC+5)
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    with TaskGroup('etl_tasks') as etl_group:
        process_datasets = PythonOperator(
            task_id='process_datasets',
            python_callable=process_datasets_task,
            provide_context=True
        )
        
        execute_etl = PythonOperator(
            task_id='execute_etl',
            python_callable=execute_etl_task_wrapper,
            provide_context=True
        )
        
        migrate_product_properties = PythonOperator(
            task_id='migrate_product_properties',
            python_callable=migrate_product_properties_task,
            provide_context=True
        )
        
        # Set task dependencies within the group
        process_datasets >> execute_etl >> migrate_product_properties
    
    check_results = PythonOperator(
        task_id='check_results',
        python_callable=check_results,
        provide_context=True
    )
    
    # Set task group dependency
    etl_group >> check_results 