#!/usr/bin/env python3
"""
Скрипт для анализа структуры данных из Power BI и исправления PostgreSQL таблицы
"""

import requests
import sys
import os
import subprocess
import json
import psycopg2
from psycopg2.extras import RealDictCursor

# Добавляем путь к Airflow
sys.path.append('/opt/airflow')

from airflow.models import Variable
from oneC_etl.utils.dax_utils import get_business_columns_from_dax

def get_access_token():
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

def get_powerbi_columns():
    """Получаем колонки из Power BI через DAX-запрос из Airflow Variable"""
    print("=== Получение колонок из Power BI (через DAX из Airflow Variable) ===")
    try:
        access_token = get_access_token()
        workspace_id = Variable.get('powerbi_workspace_id')
        dataset_id = '022e7796-b30f-44d4-b076-15331e612d47'  # ID датасета
        dax_queries = json.loads(Variable.get('dax_queries'))
        query = dax_queries['company_products']['query']
        print(f"DAX-запрос из Airflow Variable:\n{query}\n")
        # Используем новую функцию для получения бизнес-колонок
        powerbi_columns = get_business_columns_from_dax(query)
        print(f"📊 Найдено {len(powerbi_columns)} бизнес-колонок в Power BI:")
        for i, col in enumerate(powerbi_columns, 1):
            print(f"  {i}. {col}")
        return powerbi_columns
    except Exception as e:
        print(f"❌ Ошибка при получении колонок из Power BI: {e}")
        return []

def get_postgres_connection():
    """Получаем подключение к PostgreSQL"""
    try:
        # Получаем параметры подключения из Airflow Variable
        postgres_config = json.loads(Variable.get('postgres_connection'))
        
        conn = psycopg2.connect(
            host=postgres_config['host'],
            port=postgres_config['port'],
            database=postgres_config['database'],
            user=postgres_config['user'],
            password=postgres_config['password']
        )
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        return None

def get_table_structure(conn, table_name='companyproducts'):
    """Получаем структуру таблицы PostgreSQL"""
    print(f"\n=== Анализ структуры таблицы {table_name} в PostgreSQL ===")
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Проверяем существование таблицы
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (table_name,))
            
            table_exists = cursor.fetchone()['exists']
            
            if not table_exists:
                print(f"❌ Таблица {table_name} не существует")
                return []
            
            # Получаем структуру таблицы
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position;
            """, (table_name,))
            
            columns = cursor.fetchall()
            
            print(f"📊 Найдено {len(columns)} колонок в PostgreSQL:")
            for i, col in enumerate(columns, 1):
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                print(f"  {i}. {col['column_name']} ({col['data_type']}) {nullable}{default}")
            
            return columns
            
    except Exception as e:
        print(f"❌ Ошибка при получении структуры таблицы: {e}")
        return []

def normalize_column_name(powerbi_col):
    """Нормализуем имя колонки из Power BI для PostgreSQL"""
    # Убираем префикс таблицы и квадратные скобки
    if '[' in powerbi_col and ']' in powerbi_col:
        # Извлекаем имя колонки из формата 'Table[Column]'
        col_name = powerbi_col.split('[')[1].split(']')[0]
    else:
        col_name = powerbi_col
    
    # Приводим к нижнему регистру и заменяем пробелы на подчеркивания
    normalized = col_name.lower().replace(' ', '_').replace('-', '_')
    
    # Убираем специальные символы
    normalized = ''.join(c for c in normalized if c.isalnum() or c == '_')
    
    return normalized

SPECIAL_COLUMNS = {
    'upload_timestamp': 'TIMESTAMP',
    'is_vector': 'BOOLEAN',
    'updated_at': 'TIMESTAMP',
    'vector': 'BYTEA',
}

def analyze_differences(powerbi_columns, postgres_columns):
    """Анализируем различия между колонками Power BI и PostgreSQL"""
    print(f"\n=== Анализ различий ===")
    
    # Нормализуем имена колонок Power BI
    normalized_powerbi = {normalize_column_name(col): col for col in powerbi_columns}
    postgres_col_names = [col['column_name'] for col in postgres_columns]
    
    # Колонки, которые нужно добавить (Power BI + спец-колонки)
    columns_to_add = []
    for norm_name, original_name in normalized_powerbi.items():
        if norm_name not in postgres_col_names:
            columns_to_add.append((norm_name, original_name, 'TEXT'))
    for special_col, special_type in SPECIAL_COLUMNS.items():
        if special_col not in postgres_col_names:
            columns_to_add.append((special_col, special_col, special_type))
    
    # Колонки, которые можно удалить (только если не спец-колонка и не из Power BI)
    columns_to_remove = []
    for col in postgres_columns:
        if col['column_name'] not in normalized_powerbi.keys() and col['column_name'] not in SPECIAL_COLUMNS:
            columns_to_remove.append(col['column_name'])
    
    print(f"📈 Колонки для добавления: {len(columns_to_add)}")
    for norm_name, original_name, col_type in columns_to_add:
        print(f"  + {norm_name} (из {original_name}) тип {col_type}")
    print(f"📉 Колонки для удаления: {len(columns_to_remove)}")
    for col_name in columns_to_remove:
        print(f"  - {col_name}")
    return columns_to_add, columns_to_remove

def apply_fixes(conn, table_name, columns_to_add, columns_to_remove):
    """Применяем исправления к таблице"""
    print(f"\n=== Применение исправлений ===")
    
    try:
        with conn.cursor() as cursor:
            # Удаляем лишние колонки
            for col_name in columns_to_remove:
                print(f"🗑️  Удаляем колонку: {col_name}")
                cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {col_name};")
            
            # Добавляем недостающие колонки
            for norm_name, original_name, col_type in columns_to_add:
                print(f"➕ Добавляем колонку: {norm_name} (из {original_name}) тип {col_type}")
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {norm_name} {col_type};")
            
            conn.commit()
            print("✅ Исправления применены успешно!")
            
    except Exception as e:
        print(f"❌ Ошибка при применении исправлений: {e}")
        conn.rollback()

def main():
    """Основная функция"""
    print("🔍 Анализатор структуры данных Power BI → PostgreSQL")
    print("=" * 60)
    
    # 1. Получаем колонки из Power BI
    powerbi_columns = get_powerbi_columns()
    if not powerbi_columns:
        print("❌ Не удалось получить колонки из Power BI")
        return
    
    # 2. Подключаемся к PostgreSQL
    conn = get_postgres_connection()
    if not conn:
        return
    
    try:
        # 3. Получаем структуру PostgreSQL таблицы
        postgres_columns = get_table_structure(conn, table_name='companyproducts')
        if not postgres_columns:
            print("❌ Не удалось получить структуру таблицы PostgreSQL")
            return
        
        # 4. Анализируем различия
        columns_to_add, columns_to_remove = analyze_differences(powerbi_columns, postgres_columns)
        
        if not columns_to_add and not columns_to_remove:
            print("✅ Структура таблицы соответствует данным Power BI и спец-колонкам!")
            return
        
        # 5. Запрашиваем подтверждение
        print(f"\n🤔 Найдены различия в структуре таблицы.")
        response = input("Применить исправления? (y/N): ").strip().lower()
        
        if response in ['y', 'yes', 'да']:
            apply_fixes(conn, 'companyproducts', columns_to_add, columns_to_remove)
        else:
            print("❌ Исправления отменены пользователем")
    
    finally:
        conn.close()

if __name__ == '__main__':
    main() 