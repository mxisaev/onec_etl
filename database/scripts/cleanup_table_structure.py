#!/usr/bin/env python3
"""
Скрипт для очистки структуры таблицы companyproducts
- Удаляет лишние колонки с неправильными именами
- Оставляет только нужные колонки
- Создает резервную копию перед изменениями
- Интерактивный режим с подтверждением
"""

import sys
from pathlib import Path
import pandas as pd
from loguru import logger
import psycopg2
from dotenv import load_dotenv
import os
import json
from datetime import datetime

# Определяем пути относительно текущего скрипта
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # oneC_etl/
DOCKER_ROOT = PROJECT_ROOT.parent.parent  # docker/

# Добавляем путь к модулям проекта
sys.path.append(str(PROJECT_ROOT))

# Ищем .env файл в разных местах
env_paths = [
    PROJECT_ROOT / '.env',
    DOCKER_ROOT / 'dags' / '.env',
    DOCKER_ROOT / '.env',
    Path('/var/www/vhosts/itland.uk/docker/dags/.env')
]

env_loaded = False
for env_path in env_paths:
    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f"Загружены переменные окружения из: {env_path}")
        env_loaded = True
        break

if not env_loaded:
    logger.warning("Файл .env не найден, используем значения по умолчанию")

def get_database_connection():
    """Получить соединение с базой данных"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            dbname=os.getenv('POSTGRES_DB', 'data'),
            user=os.getenv('POSTGRES_USER', 'postgresadmin'),
            password=os.getenv('POSTGRES_PASSWORD', 'J5-unaxda3SK')
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

def get_required_columns():
    """Получить список нужных колонок"""
    return {
        'id': 'uuid',
        'description': 'text', 
        'brand': 'character varying',
        'category': 'character varying',
        'withdrawn_from_range': 'boolean',
        'item_number': 'character varying',
        'product_category': 'character varying',
        'on_order': 'boolean',
        'is_vector': 'boolean',
        'count_rows': 'integer',
        'merged': 'text',  # ✅ Колонка для эмбеддинга
        'upload_timestamp': 'timestamp with time zone',
        'updated_at': 'timestamp with time zone',
        'vector': 'USER-DEFINED'  # pgvector тип
    }

def confirm_cleanup(columns_to_drop, current_structure):
    """Спросить подтверждение у пользователя"""
    required_columns = get_required_columns()
    
    print("\n" + "="*80)
    print("🎯 ПЛАН ОЧИСТКИ ТАБЛИЦЫ COMPANYPRODUCTS")
    print("="*80)
    print(f"📊 Всего колонок для удаления: {len(columns_to_drop)}")
    print(f"📊 Всего колонок останется: {len(required_columns)}")
    
    print("\n❌ КОЛОНКИ КОТОРЫЕ БУДУТ УДАЛЕНЫ:")
    for i, col in enumerate(columns_to_drop, 1):
        col_type = current_structure[col]['type']
        print(f"   {i:2d}. {col:<30} ({col_type})")
    
    print("\n✅ КОЛОНКИ КОТОРЫЕ ОСТАНУТСЯ:")
    for i, (col, col_type) in enumerate(required_columns.items(), 1):
        print(f"   {i:2d}. {col:<30} ({col_type})")
    
    print("\n" + "="*80)
    print("⚠️  ВНИМАНИЕ: Будет создана резервная копия перед удалением")
    print("="*80)
    
    while True:
        response = input("\n🤔 Продолжить очистку? (y/N/q для выхода): ").lower().strip()
        if response == 'q':
            print("❌ Операция отменена пользователем")
            return False
        elif response == 'y':
            return True
        else:
            print("❓ Пожалуйста, введите 'y' для продолжения или 'q' для выхода")

def create_backup(table_name):
    """Создать резервную копию таблицы"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_table = f"{table_name}_backup_{timestamp}"
    
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            # Создаем резервную копию
            cur.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {table_name}")
            conn.commit()
            logger.success(f"💾 Резервная копия создана: {backup_table}")
            return backup_table
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Ошибка создания резервной копии: {e}")
        raise
    finally:
        conn.close()

def cleanup_table_structure(table_name):
    """Очистить структуру таблицы"""
    conn = get_database_connection()
    try:
        # Получаем текущую структуру
        current_structure = get_table_structure(table_name)
        required_columns = get_required_columns()
        
        # Находим колонки для удаления
        columns_to_drop = []
        for col in current_structure.keys():
            if col not in required_columns:
                columns_to_drop.append(col)
        
        if not columns_to_drop:
            logger.info("✅ Все колонки уже правильные, ничего удалять не нужно")
            return None
        
        # Показываем план и спрашиваем подтверждение
        if not confirm_cleanup(columns_to_drop, current_structure):
            return None
        
        # Создаем резервную копию
        backup_table = create_backup(table_name)
        
        # Удаляем лишние колонки
        with conn.cursor() as cur:
            for col in columns_to_drop:
                # Экранируем имя колонки если нужно
                if col.startswith('[') and col.endswith(']'):
                    col_name = f'"{col}"'
                else:
                    col_name = col
                
                sql = f"ALTER TABLE {table_name} DROP COLUMN {col_name} CASCADE;"
                logger.info(f"🗑️  Удаляем колонку: {col}")
                cur.execute(sql)
            
            conn.commit()
        
        logger.success(f"✅ Удалено {len(columns_to_drop)} лишних колонок")
        logger.info(f"💾 Резервная копия сохранена в таблице: {backup_table}")
        return backup_table
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Ошибка очистки таблицы: {e}")
        raise
    finally:
        conn.close()

def verify_table_structure(table_name):
    """Проверить структуру таблицы после очистки"""
    structure = get_table_structure(table_name)
    required_columns = get_required_columns()
    
    print("\n" + "="*60)
    print("🔍 ПРОВЕРКА СТРУКТУРЫ ТАБЛИЦЫ ПОСЛЕ ОЧИСТКИ")
    print("="*60)
    
    # Проверяем наличие всех нужных колонок
    missing_columns = []
    for col, col_type in required_columns.items():
        if col in structure:
            print(f"  ✅ {col:<25} ({structure[col]['type']})")
        else:
            print(f"  ❌ {col:<25} ОТСУТСТВУЕТ")
            missing_columns.append(col)
    
    # Проверяем лишние колонки
    extra_columns = []
    for col in structure.keys():
        if col not in required_columns:
            print(f"  ⚠️  {col:<25} ЛИШНЯЯ КОЛОНКА")
            extra_columns.append(col)
    
    print("="*60)
    
    if missing_columns:
        logger.error(f"❌ Отсутствуют колонки: {missing_columns}")
        return False
    
    if extra_columns:
        logger.warning(f"⚠️  Лишние колонки: {extra_columns}")
        return False
    
    logger.success("✅ Структура таблицы корректна!")
    return True

def find_dag_file():
    """Найти файл DAG"""
    dag_paths = [
        PROJECT_ROOT / 'company_products_etl.py',
        PROJECT_ROOT / 'dags' / 'company_products_etl.py',
        DOCKER_ROOT / 'dags' / 'oneC_etl' / 'company_products_etl.py'
    ]
    
    for dag_path in dag_paths:
        if dag_path.exists():
            return dag_path
    
    raise FileNotFoundError("Файл company_products_etl.py не найден")

def find_mappings_file():
    """Найти файл dax_mappings.py"""
    mappings_paths = [
        PROJECT_ROOT / 'config' / 'dax_mappings.py',
        PROJECT_ROOT / 'dax_mappings.py',
        DOCKER_ROOT / 'dags' / 'oneC_etl' / 'config' / 'dax_mappings.py'
    ]
    
    for mappings_path in mappings_paths:
        if mappings_path.exists():
            return mappings_path
    
    raise FileNotFoundError("Файл dax_mappings.py не найден")

def update_dag_column_mapping():
    """Обновить маппирование колонок в DAG"""
    dag_file = find_dag_file()
    
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
        "Product Properties": "merged"  # ✅ Маппим в merged вместо product_properties
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
    
    logger.success(f"✅ Маппирование колонок обновлено в {dag_file}")

def update_dax_mappings():
    """Обновить маппирование в файле dax_mappings.py"""
    mappings_file = find_mappings_file()
    
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
        'Product Properties': 'merged'  # ✅ Маппим в merged
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
    
    logger.success(f"✅ Маппирование обновлено в {mappings_file}")

def restore_from_backup(backup_table, target_table):
    """Восстановить таблицу из резервной копии"""
    if not backup_table:
        logger.error("❌ Нет резервной копии для восстановления")
        return False
    
    print(f"\n🔄 Восстанавливаем из резервной копии: {backup_table}")
    
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            # Удаляем текущую таблицу
            cur.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE")
            
            # Восстанавливаем из резервной копии
            cur.execute(f"CREATE TABLE {target_table} AS SELECT * FROM {backup_table}")
            
            conn.commit()
            logger.success(f"✅ Таблица {target_table} восстановлена из {backup_table}")
            return True
            
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Ошибка восстановления: {e}")
        return False
    finally:
        conn.close()

def create_cleanup_migration():
    """Создать SQL миграцию для очистки таблицы"""
    migration_sql = f"""
-- Миграция для очистки структуры таблицы companyproducts
-- Дата: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

-- Создаем резервную копию
CREATE TABLE companyproducts_backup_$(date +%Y%m%d_%H%M%S) AS SELECT * FROM companyproducts;

-- Удаляем лишние колонки
ALTER TABLE companyproducts DROP COLUMN IF EXISTS "companyproducts" CASCADE;
ALTER TABLE companyproducts DROP COLUMN IF EXISTS "isgrandtotalrowtotal" CASCADE;
ALTER TABLE companyproducts DROP COLUMN IF EXISTS "companyproducts_1" CASCADE;
ALTER TABLE companyproducts DROP COLUMN IF EXISTS "companyproducts_2" CASCADE;
ALTER TABLE companyproducts DROP COLUMN IF EXISTS "companyproducts_3" CASCADE;
ALTER TABLE companyproducts DROP COLUMN IF EXISTS "companyproducts_4" CASCADE;
ALTER TABLE companyproducts DROP COLUMN IF EXISTS "companyproducts_5" CASCADE;
ALTER TABLE companyproducts DROP COLUMN IF EXISTS "[выводится_без_остатков]" CASCADE;
ALTER TABLE companyproducts DROP COLUMN IF EXISTS "companyproducts[merged]" CASCADE;
ALTER TABLE companyproducts DROP COLUMN IF EXISTS "[product properties]" CASCADE;
ALTER TABLE companyproducts DROP COLUMN IF EXISTS "product_properties" CASCADE;

-- Проверяем структуру
SELECT column_name, data_type, is_nullable
FROM information_schema.columns 
WHERE table_schema = 'public' 
AND table_name = 'companyproducts'
ORDER BY ordinal_position;
"""
    
    # Создаем папку migrations если её нет
    migrations_dir = SCRIPT_DIR.parent / 'migrations'
    migrations_dir.mkdir(exist_ok=True)
    
    migration_file = migrations_dir / 'cleanup_companyproducts_structure.sql'
    with open(migration_file, 'w', encoding='utf-8') as f:
        f.write(migration_sql)
    
    logger.success(f"📄 SQL миграция создана: {migration_file}")
    return migration_file

def main():
    """Основная функция"""
    print("\n" + "🚀" + "="*60 + "🚀")
    print("🎯 ОЧИСТКА СТРУКТУРЫ ТАБЛИЦЫ COMPANYPRODUCTS")
    print("🚀" + "="*60 + "🚀")
    
    try:
        table_name = 'companyproducts'
        
        # 1. Создаем SQL миграцию
        logger.info("1. 📄 Создаем SQL миграцию...")
        migration_file = create_cleanup_migration()
        
        # 2. Очищаем структуру таблицы
        logger.info("2. 🗑️  Очищаем структуру таблицы...")
        backup_table = cleanup_table_structure(table_name)
        
        if backup_table is None:
            logger.info("❌ Операция отменена")
            return True
        
        # 3. Проверяем результат
        logger.info("3. 🔍 Проверяем структуру таблицы...")
        success = verify_table_structure(table_name)
        
        if not success:
            logger.error("❌ Очистка завершена с ошибками!")
            
            # Предлагаем восстановление
            response = input("\n🔄 Восстановить таблицу из резервной копии? (y/N): ").lower().strip()
            if response == 'y':
                restore_from_backup(backup_table, table_name)
            
            return False
        
        # 4. Обновляем маппирование в DAG
        logger.info("4. 🔄 Обновляем маппирование в DAG...")
        update_dag_column_mapping()
        
        # 5. Обновляем маппирование в dax_mappings.py
        logger.info("5. 🔄 Обновляем маппирование в dax_mappings.py...")
        update_dax_mappings()
        
        print("\n" + "🎉" + "="*60 + "🎉")
        print("✅ ОЧИСТКА ЗАВЕРШЕНА УСПЕШНО!")
        print("🎉" + "="*60 + "🎉")
        print(f"💾 Резервная копия: {backup_table}")
        print(f"📄 SQL миграция: {migration_file}")
        print("🔄 Маппирование обновлено в DAG и конфигурации")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 