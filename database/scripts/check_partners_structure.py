#!/usr/bin/env python3
"""
Скрипт для проверки структуры таблицы partners и тестирования ETL процесса
"""

import sys
import json
from pathlib import Path
from loguru import logger
import psycopg2
from psycopg2.extras import RealDictCursor

# Добавляем путь к модулям проекта
sys.path.append(str(Path(__file__).parent.parent.parent))

def get_postgres_connection():
    """Получить соединение с PostgreSQL через Airflow Variables"""
    try:
        from airflow.models import Variable
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
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        raise

def check_partners_table():
    """Проверить структуру таблицы partners"""
    logger.info("🔍 Проверяем структуру таблицы partners...")
    
    conn = get_postgres_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Проверяем существование таблицы
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = 'partners'
                )
            """)
            table_exists = cursor.fetchone()['exists']
            
            if not table_exists:
                logger.error("❌ Таблица partners не существует!")
                return False
            
            logger.info("✅ Таблица partners существует")
            
            # Получаем структуру таблицы
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = 'partners'
                ORDER BY ordinal_position
            """)
            
            columns = cursor.fetchall()
            logger.info(f"📊 Структура таблицы partners ({len(columns)} колонок):")
            
            for col in columns:
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                logger.info(f"  - {col['column_name']}: {col['data_type']} {nullable}{default}")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке структуры: {e}")
        return False
    finally:
        conn.close()

def check_suppliers_view():
    """Проверить VIEW suppliers_view"""
    logger.info("🔍 Проверяем VIEW suppliers_view...")
    
    conn = get_postgres_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Проверяем существование VIEW
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.views 
                    WHERE table_schema = 'public' AND table_name = 'suppliers_view'
                )
            """)
            view_exists = cursor.fetchone()['exists']
            
            if not view_exists:
                logger.error("❌ VIEW suppliers_view не существует!")
                return False
            
            logger.info("✅ VIEW suppliers_view существует")
            
            # Проверяем содержимое VIEW
            cursor.execute("SELECT COUNT(*) as count FROM suppliers_view")
            count = cursor.fetchone()['count']
            logger.info(f"📊 В suppliers_view записей: {count}")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке VIEW: {e}")
        return False
    finally:
        conn.close()

def test_dax_query():
    """Тестировать DAX запрос из Airflow Variables"""
    logger.info("🔍 Тестируем DAX запрос...")
    
    try:
        from airflow.models import Variable
        
        # Получаем DAX запрос
        dax_queries = Variable.get("dax_queries", deserialize_json=True)
        if 'partners' not in dax_queries:
            logger.error("❌ DAX запрос для partners не найден в Airflow Variables")
            return False
        
        query_config = dax_queries['partners']
        logger.info(f"✅ DAX запрос найден: {query_config['description']}")
        
        # Получаем конфигурацию датасета
        datasets = Variable.get("datasets", deserialize_json=True)
        if 'partners' not in datasets:
            logger.error("❌ Конфигурация датасета partners не найдена")
            return False
        
        dataset_config = datasets['partners']
        logger.info(f"✅ Конфигурация датасета: {dataset_config}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при тестировании DAX: {e}")
        return False

def main():
    """Основная функция"""
    logger.info("🚀 Начинаем проверку структуры partners...")
    
    # Проверяем таблицу
    if not check_partners_table():
        logger.error("❌ Проверка таблицы partners не прошла")
        return
    
    # Проверяем VIEW
    if not check_suppliers_view():
        logger.error("❌ Проверка VIEW suppliers_view не прошла")
        return
    
    # Тестируем DAX
    if not test_dax_query():
        logger.error("❌ Тестирование DAX не прошло")
        return
    
    logger.success("🎉 Все проверки прошли успешно!")
    logger.info("✅ Таблица partners готова к ETL процессу")

if __name__ == "__main__":
    main()
