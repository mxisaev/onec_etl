#!/usr/bin/env python3
"""
Скрипт для исправления символов | в prop_key таблицы product_properties
"""

import sys
import os
from pathlib import Path

# Добавляем путь к проекту
current_dir = Path(__file__).parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

from database.client import DatabaseClient
from utils.logger import setup_logger

logger = setup_logger()

def fix_pipe_symbols():
    """Исправляет символы | в prop_key таблицы product_properties"""
    try:
        logger.info("🔧 Начинаем исправление символов | в prop_key...")
        
        db_client = DatabaseClient()
        
        # Получаем все записи с символом | в prop_key
        select_query = """
        SELECT id, product_id, prop_key, prop_value
        FROM product_properties 
        WHERE prop_key LIKE '|%'
        """
        
        records_with_pipe = db_client.execute_query(select_query, fetch_all=True)
        
        if not records_with_pipe:
            logger.info("✅ Символы | в prop_key не найдены")
            return True
        
        logger.info(f"📊 Найдено {len(records_with_pipe)} записей с символом | в prop_key")
        
        # Показываем примеры
        for i, record in enumerate(records_with_pipe[:5]):
            logger.info(f"   Пример {i+1}: '{record['prop_key']}' -> '{record['prop_key'].replace('|', '').strip()}'")
        
        if len(records_with_pipe) > 5:
            logger.info(f"   ... и еще {len(records_with_pipe) - 5} записей")
        
        # Спрашиваем подтверждение
        response = input(f"\nИсправить {len(records_with_pipe)} записей? (y/N): ")
        if response.lower() != 'y':
            logger.info("❌ Операция отменена пользователем")
            return False
        
        # Исправляем записи
        fixed_count = 0
        for record in records_with_pipe:
            old_key = record['prop_key']
            new_key = old_key.replace('|', '').strip()
            
            if old_key != new_key:
                # Обновляем prop_key
                update_query = """
                UPDATE product_properties 
                SET prop_key = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """
                
                rows_updated = db_client.execute_query(update_query, (new_key, record['id']))
                if rows_updated > 0:
                    fixed_count += 1
                    logger.debug(f"✅ Исправлено: '{old_key}' -> '{new_key}'")
        
        db_client.commit()
        
        logger.success(f"🎉 Исправление завершено! Обработано {fixed_count} записей")
        
        # Проверяем результат
        remaining_query = """
        SELECT COUNT(*) as count
        FROM product_properties 
        WHERE prop_key LIKE '|%'
        """
        
        remaining = db_client.execute_query(remaining_query, fetch_one=True)['count']
        logger.info(f"📊 Осталось записей с символом |: {remaining}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка исправления символов |: {e}")
        return False

def main():
    """Основная функция"""
    try:
        success = fix_pipe_symbols()
        if success:
            print("✅ Исправление символов | завершено успешно")
        else:
            print("❌ Исправление символов | не удалось")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Ошибка выполнения: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
