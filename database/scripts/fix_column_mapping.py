#!/usr/bin/env python3
"""
Скрипт для исправления проблем с маппированием колонок
- Обновляет маппирование в DAG
- Добавляет недостающие колонки в PostgreSQL
- Исправляет маппирование в коде
"""

import sys
from pathlib import Path
import pandas as pd
from loguru import logger
import psycopg2
from dotenv import load_dotenv
import os
import json

# Добавляем путь к модулям проекта
sys.path.append(str(Path(__file__).parent.parent.parent))

# Загружаем переменные окружения
env_path = Path('/var/www/vhosts/itland.uk/docker/dags/.env')
if env_path.exists():
    load_dotenv(env_path)

def get_database_connection():
    """Получить соединение с базой данных"""
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=os.getenv('POSTGRES_PORT', '5432'),
            dbname=os.getenv('POSTGRES_DB', 'postgres'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres')
        )
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        raise

def get_table_structure(table_name):
    """Получить структуру таблицы"""
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position;
            """, (table_name,))
            columns = cur.fetchall()
            return {col[0]: {'type': col[1], 'nullable': col[2]} for col in columns}
    finally:
        conn.close()

def add_missing_columns(table_name, required_columns):
    """Добавить недостающие колонки в таблицу"""
    conn = get_database_connection()
    try:
        existing_columns = get_table_structure(table_name)
        
        with conn.cursor() as cur:
            for column_name, column_info in required_columns.items():
                if column_name not in existing_columns:
                    sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_info['type']};"
                    logger.info(f"Добавляем колонку: {sql}")
                    cur.execute(sql)
                    
        conn.commit()
        logger.success(f"Колонки добавлены в таблицу {table_name}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка добавления колонок: {e}")
        raise
    finally:
        conn.close()

def update_dag_column_mapping():
    """Обновить маппирование колонок в DAG"""
    dag_file = Path('/var/www/vhosts/itland.uk/docker/dags/oneC_etl/company_products_etl.py')
    
    # Новое маппирование с правильными именами колонок
    new_mapping = {
        "CompanyProducts[ID]": "id",
        "CompanyProducts[Description]": "description", 
        "CompanyProducts[Brand]": "brand",
        "CompanyProducts[Category]": "category",
        "CompanyProducts[Withdrawn_from_range]": "withdrawn_from_range",
        "CompanyProducts[item_number]": "item_number",
        "УТ_Товарные категории[_description]": "product_category",
        "УТ_РСвДополнительныеСведения2_0[Под заказ]": "on_order",
        "Выводится_без_остатков": "is_vector",
        "CountRowsУТ_РСвДополнительныеСведения2_0": "count_rows",
        "Product Properties": "product_properties"  # Добавляем недостающую колонку
    }
    
    # Читаем файл DAG
    with open(dag_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим и заменяем маппирование колонок
    import re
    
    # Паттерн для поиска блока columns
    pattern = r'("columns":\s*\{[^}]*\})'
    
    # Создаем новое содержимое для columns
    new_columns_content = '"columns": {\n'
    for i, (key, value) in enumerate(new_mapping.items()):
        new_columns_content += f'                "{key}": "{value}"'
        if i < len(new_mapping) - 1:
            new_columns_content += ','
        new_columns_content += '\n'
    new_columns_content += '            }'
    
    # Заменяем старый блок на новый
    new_content = re.sub(pattern, new_columns_content, content, flags=re.DOTALL)
    
    # Записываем обновленный файл
    with open(dag_file, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    logger.success(f"Маппирование колонок обновлено в {dag_file}")

def update_dax_mappings():
    """Обновить маппирование в файле dax_mappings.py"""
    mappings_file = Path('/var/www/vhosts/itland.uk/docker/dags/oneC_etl/config/dax_mappings.py')
    
    # Новое маппирование
    new_mapping = {
        'CompanyProducts[ID]': 'id',
        'CompanyProducts[Description]': 'description',
        'CompanyProducts[Brand]': 'brand', 
        'CompanyProducts[Category]': 'category',
        'CompanyProducts[Withdrawn_from_range]': 'withdrawn_from_range',
        'CompanyProducts[item_number]': 'item_number',
        'УТ_Товарные категории[_description]': 'product_category',
        'УТ_РСвДополнительныеСведения2_0[Под заказ]': 'on_order',
        'Выводится_без_остатков': 'is_vector',
        'CountRowsУТ_РСвДополнительныеСведения2_0': 'count_rows',
        'Product Properties': 'product_properties'
    }
    
    # Читаем файл
    with open(mappings_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Находим и заменяем маппирование в DAX_MAPPINGS
    import re
    
    # Паттерн для поиска блока columns в DAX_MAPPINGS
    pattern = r"'columns':\s*\{[^}]*\}"
    
    # Создаем новое содержимое
    new_columns_content = "'columns': {\n"
    for i, (key, value) in enumerate(new_mapping.items()):
        new_columns_content += f"            '{key}': '{value}'"
        if i < len(new_mapping) - 1:
            new_columns_content += ','
        new_columns_content += '\n'
    new_columns_content += '        }'
    
    # Заменяем
    new_content = re.sub(pattern, new_columns_content, content, flags=re.DOTALL)
    
    # Записываем обновленный файл
    with open(mappings_file, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    logger.success(f"Маппирование обновлено в {mappings_file}")

def create_migration_sql():
    """Создать SQL миграцию для добавления недостающих колонок"""
    migration_sql = """
-- Миграция для добавления недостающих колонок в таблицу companyproducts
-- Дата: $(date)

-- Добавляем колонку product_properties если её нет
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'companyproducts' 
        AND column_name = 'product_properties'
    ) THEN
        ALTER TABLE public.companyproducts ADD COLUMN product_properties TEXT;
        RAISE NOTICE 'Колонка product_properties добавлена';
    ELSE
        RAISE NOTICE 'Колонка product_properties уже существует';
    END IF;
END $$;

-- Проверяем структуру таблицы
SELECT column_name, data_type, is_nullable
FROM information_schema.columns 
WHERE table_schema = 'public' 
AND table_name = 'companyproducts'
ORDER BY ordinal_position;
"""
    
    migration_file = Path('/var/www/vhosts/itland.uk/docker/dags/oneC_etl/database/migrations/add_product_properties_column.sql')
    with open(migration_file, 'w', encoding='utf-8') as f:
        f.write(migration_sql)
    
    logger.success(f"SQL миграция создана: {migration_file}")
    return migration_file

def execute_migration(migration_file):
    """Выполнить SQL миграцию"""
    conn = get_database_connection()
    try:
        with open(migration_file, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
        
        logger.success("Миграция выполнена успешно")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка выполнения миграции: {e}")
        raise
    finally:
        conn.close()

def main():
    """Основная функция"""
    logger.info("Начинаем исправление проблем с маппированием колонок...")
    
    try:
        # 1. Обновляем маппирование в DAG
        logger.info("1. Обновляем маппирование в DAG...")
        update_dag_column_mapping()
        
        # 2. Обновляем маппирование в dax_mappings.py
        logger.info("2. Обновляем маппирование в dax_mappings.py...")
        update_dax_mappings()
        
        # 3. Создаем SQL миграцию
        logger.info("3. Создаем SQL миграцию...")
        migration_file = create_migration_sql()
        
        # 4. Выполняем миграцию
        logger.info("4. Выполняем миграцию...")
        execute_migration(migration_file)
        
        # 5. Проверяем структуру таблицы
        logger.info("5. Проверяем структуру таблицы...")
        structure = get_table_structure('companyproducts')
        logger.info("Текущая структура таблицы companyproducts:")
        for col, info in structure.items():
            logger.info(f"  {col}: {info['type']} (nullable: {info['nullable']})")
        
        logger.success("Все проблемы с маппированием колонок исправлены!")
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 