#!/usr/bin/env python3
"""
Suppliers ETL DAG
ETL процесс для загрузки данных о партнерах (поставщиках и клиентах) из Power BI в PostgreSQL
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

# Создаем logger для этого DAG
logger = get_logger("suppliers_etl", "suppliers_etl")

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
    'SuppliersETL',
    default_args=default_args,
    description='ETL процесс для загрузки данных о партнерах (поставщиках и клиентах) из Power BI в PostgreSQL',
    schedule_interval='50 6,10 * * 1-5',  # Пн-Пт в 6:50 и 10:50 UTC (11:50 и 15:50 UTC+5)
    catchup=False,
    tags=['etl', 'powerbi', 'postgres', 'suppliers', 'partners']
)

def extract_suppliers_data_task(**context):
    """Извлечение данных о партнерах из Power BI через DAX"""
    try:
        logger.info("🔄 Начинаем извлечение данных о партнерах из Power BI...")
        
        from suppliers_etl.tasks.extract import extract_powerbi_data
        
        # Получаем конфигурацию dataset из Airflow Variables
        from airflow.models import Variable
        datasets = Variable.get('datasets', deserialize_json=True)
        partners_dataset = datasets['partners']
        
        # Конфигурация задачи согласно документации
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
        
        result = extract_powerbi_data(task_config)
        return result
        
    except Exception as e:
        logger.exception(f"❌ Ошибка извлечения данных о партнерах: {str(e)}")
        raise

def load_partners_to_postgres_task(**context):
    """Загрузка данных о партнерах в PostgreSQL"""
    try:
        logger.info("🔄 Начинаем загрузку данных в PostgreSQL...")
        
        # Горячая перезагрузка модулей, чтобы воркер подцепил обновленный PostgresClient/merge
        import importlib
        import suppliers_etl.services.postgres.client as pg_client
        import suppliers_etl.tasks.load as load_module
        importlib.reload(pg_client)
        importlib.reload(load_module)
        execute_etl_task = load_module.execute_etl_task
        
        # Получаем данные из предыдущей задачи
        ti = context['ti']
        data = ti.xcom_pull(task_ids='extract_suppliers_data')
        
        if data is None:
            raise ValueError("Нет данных о партнерах для загрузки")
        
        # Конвертируем данные в DataFrame
        import pandas as pd
        df = pd.DataFrame(data)
        
        logger.info(f"📊 DataFrame создан: {len(df)} строк, {len(df.columns)} колонок")
        
        # Получаем только названия таблиц для PostgreSQL
        from airflow.models import Variable
        datasets = Variable.get('datasets', deserialize_json=True)
        
        # Конфигурация загрузки согласно документации
        task_config = {
            'source_table': datasets['partners']['source_table'],
            'target_table': datasets['partners']['target_table'],
            'mapping_name': 'partners'
        }
        
        # Проводим загрузку напрямую через PostgresClient, чтобы гарантировать использование актуального MERGE
        from suppliers_etl.config.dax_mappings import get_dax_mapping
        mapping = get_dax_mapping(task_config['mapping_name'])
        
        # Подготовка данных
        df['is_vector'] = False
        df = df.rename(columns=mapping['columns'])
        # Фильтруем записи без обязательного бизнес-ключа id_1c_partner
        before = len(df)
        df = df.dropna(subset=['id_1c_partner'])
        df = df[df['id_1c_partner'].astype(str).str.strip() != '']
        dropped = before - len(df)
        if dropped > 0:
            logger.info(f"🧹 Отфильтровано записей без id_1c_partner: {dropped}")
        key_columns = ['id_1c_partner', 'id_1c_contact']
        
        # Колонки для анализа изменений
        technical_fields = {'partner_uid', 'id_1c_partner', 'id_1c_contact', 'is_vector', 'created_at', 'updated_at', 'vector', 'extracted_at'}
        columns_for_change_analysis = [col for col in df.columns if col not in technical_fields]
        
        # Типы колонок
        def get_column_type(col_name):
            if col_name in ['partner_uid']:
                return 'UUID'
            elif col_name in ['id_1c_partner', 'id_1c_contact']:
                return 'VARCHAR'
            elif col_name in ['is_client', 'is_supplier', 'is_vector']:
                return 'BOOLEAN'
            else:
                return 'TEXT'
        
        PostgresClient = pg_client.PostgresClient
        client = PostgresClient()
        result = client.merge_data(
            table_name=task_config['target_table'],
            data=df,
            key_columns=key_columns,
            columns=[{'name': col, 'dataType': get_column_type(col)} for col in df.columns],
            template_name=mapping['table_template'],
            columns_for_change_analysis=columns_for_change_analysis
        )
        
        return result
        
    except Exception as e:
        logger.exception(f"❌ Ошибка загрузки данных о партнерах: {str(e)}")
        raise

def validate_partners_data_task(**context):
    """Валидация загруженных данных о партнерах"""
    try:
        from suppliers_etl.services.postgres.client import PostgresClient
        
        db_client = PostgresClient()
        
        # Проверяем количество загруженных партнеров
        partners_query = """
        SELECT 
            COUNT(*) as total_partners,
            COUNT(CASE WHEN partner IS NOT NULL THEN 1 END) as partners_with_name,
            COUNT(CASE WHEN contact IS NOT NULL THEN 1 END) as partners_with_contact,
            COUNT(CASE WHEN contact_email IS NOT NULL THEN 1 END) as partners_with_email,
            COUNT(CASE WHEN responsible_manager IS NOT NULL THEN 1 END) as partners_with_manager,
            COUNT(CASE WHEN is_client = TRUE THEN 1 END) as total_clients,
            COUNT(CASE WHEN is_supplier = TRUE THEN 1 END) as total_suppliers
        FROM partners
        """
        
        result = db_client.execute_query(partners_query)
        
        if result and len(result) > 0:
            stats = result[0]
            
            # Проверяем качество данных
            validation_status = "success"
            if stats['total_partners'] == 0:
                validation_status = "error"
            elif stats['partners_with_name'] == 0:
                validation_status = "warning"
            
            # Проверяем композитный ключ
            composite_key_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT CONCAT(id_1c_partner, '|', id_1c_contact)) as unique_composite_keys
            FROM partners
            """
            
            composite_result = db_client.execute_query(composite_key_query)
            if composite_result and len(composite_result) > 0:
                composite_stats = composite_result[0]
                
                if composite_stats['total_records'] != composite_stats['unique_composite_keys']:
                    logger.warning("⚠️ Обнаружены дублирующиеся композитные ключи")
                    validation_status = "warning"
                
                stats.update(composite_stats)
            
            return {
                "status": validation_status,
                "partners": stats
            }
        else:
            logger.warning("⚠️ Не удалось получить результаты валидации")
            return {"status": "error", "message": "Validation failed"}
        
    except Exception as e:
        logger.exception(f"❌ Ошибка валидации: {str(e)}")
        raise

def generate_supplier_report_task(**context):
    """Генерация отчета по поставщикам"""
    try:
        from suppliers_etl.services.postgres.client import PostgresClient
        
        db_client = PostgresClient()
        
        # Генерируем отчет согласно документации
        report_query = """
        SELECT 
            'Общая статистика' as report_section,
            COUNT(*) as total_partners,
            COUNT(CASE WHEN COALESCE(is_client,false) = TRUE THEN 1 END) as total_clients,
            COUNT(CASE WHEN COALESCE(is_client,false) = TRUE THEN 1 END) as total_suppliers,
            COUNT(CASE WHEN COALESCE(is_client,false) = TRUE AND COALESCE(is_supplier,false) = TRUE THEN 1 END) as client_suppliers,
            NULL::integer as unique_roles,
            NULL::integer as managers,
            NULL::integer as directors,
            NULL::integer as other_roles
        FROM partners
        
        UNION ALL
        
        SELECT 
            'По ролям контактов' as report_section,
            NULL::integer as total_partners,
            NULL::integer as total_clients,
            NULL::integer as total_suppliers,
            NULL::integer as client_suppliers,
            COUNT(DISTINCT role) as unique_roles,
            COUNT(CASE WHEN role = 'Менеджер' THEN 1 END) as managers,
            COUNT(CASE WHEN role = 'Директор' THEN 1 END) as directors,
            COUNT(CASE WHEN role NOT IN ('Менеджер', 'Директор') THEN 1 END) as other_roles
        FROM partners
        """
        
        result = db_client.execute_query(report_query)
        
        if result and len(result) > 0:
            return {"status": "success", "report_rows": len(result)}
        else:
            logger.warning("⚠️ Не удалось сгенерировать отчет")
            return {"status": "warning", "message": "Report generation failed"}
        
    except Exception as e:
        logger.exception(f"❌ Ошибка генерации отчета: {str(e)}")
        raise

def final_summary_task(**context):
    """Финальная сводка выполнения DAG"""
    try:
        logger.info("🎯 ========================================")
        logger.info("🎯 ФИНАЛЬНАЯ СВОДКА ВЫПОЛНЕНИЯ DAG")
        logger.info("🎯 ========================================")
        
        # Получаем результаты всех задач
        ti = context['ti']
        extract_result = ti.xcom_pull(task_ids='extract_suppliers_data')
        load_result = ti.xcom_pull(task_ids='load_partners_to_postgres')
        validate_result = ti.xcom_pull(task_ids='validate_partners_data')
        report_result = ti.xcom_pull(task_ids='generate_supplier_report')
        
        logger.info("📋 Результаты выполнения задач:")
        logger.info(f"   ✅ Extract: {len(extract_result) if extract_result else 0} записей извлечено")
        logger.info(f"   ✅ Load: {load_result} записей загружено в PostgreSQL")
        logger.info(f"   ✅ Validate: Статус валидации - {validate_result.get('status', 'unknown')}")
        logger.info(f"   ✅ Report: Отчет сгенерирован - {report_result.get('status', 'unknown')}")
        
        # Проверяем общий статус
        all_success = all([
            extract_result is not None,
            load_result is not None,
            validate_result and validate_result.get('status') == 'success',
            report_result and report_result.get('status') == 'success'
        ])
        
        if all_success:
            logger.info("🎉 ВСЕ ЗАДАЧИ ВЫПОЛНЕНЫ УСПЕШНО!")
            logger.info("🎯 DAG SuppliersETL завершен успешно")
        else:
            logger.warning("⚠️ Некоторые задачи завершились с предупреждениями")
        
        # Добавляем финальную строку
        logger.info("🎯 ========================================")
        
        return {
            "status": "success" if all_success else "warning",
            "summary": {
                "extract_count": len(extract_result) if extract_result else 0,
                "load_status": load_result,
                "validation_status": validate_result.get('status') if validate_result else 'unknown',
                "report_status": report_result.get('status') if report_result else 'unknown'
            }
        }
        
    except Exception as e:
        logger.exception(f"❌ Ошибка финальной сводки: {str(e)}")
        raise

# Создание задач согласно архитектуре из документации
extract_operator = PythonOperator(
    task_id='extract_suppliers_data',
    python_callable=extract_suppliers_data_task,
    dag=dag
)

load_operator = PythonOperator(
    task_id='load_partners_to_postgres',
    python_callable=load_partners_to_postgres_task,
    dag=dag
)

validate_operator = PythonOperator(
    task_id='validate_partners_data',
    python_callable=validate_partners_data_task,
    dag=dag
)

report_operator = PythonOperator(
    task_id='generate_supplier_report',
    python_callable=generate_supplier_report_task,
    dag=dag
)

summary_operator = PythonOperator(
    task_id='final_summary',
    python_callable=final_summary_task,
    dag=dag
)

# Настройка зависимостей согласно потоку данных из документации:
# 1. Извлечение данных о партнерах из Power BI через DAX
# 2. Загрузка в таблицу partners с композитным ключом
# 3. Валидация загруженных данных
# 4. Генерация отчета по поставщикам
# 5. Финальная сводка выполнения DAG
extract_operator >> load_operator >> validate_operator >> report_operator >> summary_operator

if __name__ == "__main__":
    dag.cli()
