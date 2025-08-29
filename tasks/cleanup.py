"""
Data cleanup module for removing orphaned records
"""

import pandas as pd
from loguru import logger
from oneC_etl.services.postgres.client import PostgresClient
from oneC_etl.config.settings import get_config

def cleanup_orphaned_records(data, task_config):
    """
    Remove records from target table that are not present in the new PowerBI export
    
    Args:
        data (list): List of dictionaries from PowerBI export
        task_config (dict): Task configuration containing:
            - target_table: Target table name to clean
            - key_column: Primary key column name (usually 'id')
    
    Returns:
        dict: Cleanup statistics
    """
    try:
        # Get configuration
        config = get_config()
        
        # Initialize PostgreSQL client
        client = PostgresClient()
        
        # Convert data to DataFrame if it's a list
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data
        
        # Get the key column values from new data
        key_column = task_config['key_column']
        target_table = task_config['target_table']
        
        if key_column not in df.columns:
            raise ValueError(f"Required key column '{key_column}' not found in data")
        
        logger.info(f"🔍 Анализируем данные:")
        logger.info(f"   - Колонка ключа: {key_column}")
        logger.info(f"   - Целевая таблица: {target_table}")
        logger.info(f"   - Новых записей из PowerBI: {len(df)}")
        
        # Get all existing IDs from the target table
        existing_ids_query = f"""
        SELECT {key_column} 
        FROM {target_table}
        """
        
        existing_records = client.execute_query(existing_ids_query)
        existing_ids = set(record[key_column] for record in existing_records)
        
        logger.info(f"   - Существующих записей в БД: {len(existing_ids)}")
        
        # Get IDs from new PowerBI export
        new_ids = set(df[key_column].dropna().astype(str))
        
        logger.info(f"   - Уникальных ID в новой выгрузке: {len(new_ids)}")
        
        # Debug: показываем несколько примеров ID
        if existing_ids:
            sample_existing = list(existing_ids)[:3]
            logger.info(f"   - Примеры существующих ID: {sample_existing}")
        
        if new_ids:
            sample_new = list(new_ids)[:3]
            logger.info(f"   - Примеры новых ID: {sample_new}")
        
        # Приводим ID к одинаковому формату для корректного сравнения
        # Если в БД UUID объекты, а из PowerBI строки - приводим к строкам
        existing_ids_normalized = set(str(uid) for uid in existing_ids)
        new_ids_normalized = set(str(uid) for uid in new_ids)
        
        logger.info(f"   - После нормализации:")
        logger.info(f"     - Существующих ID: {len(existing_ids_normalized)}")
        logger.info(f"     - Новых ID: {len(new_ids_normalized)}")
        
        # Find orphaned records (exist in DB but not in new export)
        orphaned_ids = existing_ids_normalized - new_ids_normalized
        
        logger.info(f"   - Найдено устаревших записей: {len(orphaned_ids)}")
        
        # Дополнительная проверка безопасности
        if len(orphaned_ids) > len(existing_ids_normalized) * 0.9:  # Если удаляем больше 90% записей
            logger.warning(f"⚠️ ВНИМАНИЕ: Попытка удалить {len(orphaned_ids)} из {len(existing_ids_normalized)} записей (>90%)!")
            logger.warning(f"⚠️ Это может быть ошибкой. Проверяем данные...")
            
            # Показываем больше деталей
            if existing_ids_normalized:
                sample_existing = list(existing_ids_normalized)[:5]
                logger.warning(f"⚠️ Примеры существующих ID (нормализованные): {sample_existing}")
            
            if new_ids_normalized:
                sample_new = list(new_ids_normalized)[:5]
                logger.warning(f"⚠️ Примеры новых ID (нормализованные): {sample_new}")
            
            if orphaned_ids:
                sample_orphaned = list(orphaned_ids)[:5]
                logger.warning(f"⚠️ Примеры устаревших ID: {sample_orphaned}")
            
            # Проверяем пересечение
            intersection = existing_ids_normalized & new_ids_normalized
            logger.warning(f"⚠️ Пересечение (общие ID): {len(intersection)}")
            
            if len(intersection) < len(existing_ids_normalized) * 0.1:  # Если общих ID меньше 10%
                logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Общих ID только {len(intersection)} из {len(existing_ids_normalized)}!")
                logger.error(f"❌ Возможно, колонка ключа '{key_column}' не совпадает между таблицами!")
                logger.error(f"❌ Очистка ОТМЕНЕНА для безопасности!")
                
                return {
                    'target_table': target_table,
                    'total_existing': len(existing_ids_normalized),
                    'total_new': len(new_ids_normalized),
                    'deleted_records': 0,
                    'status': 'error',
                    'message': f'Cleanup cancelled: only {len(intersection)} common IDs found, possible key column mismatch'
                }
        
        if orphaned_ids:
            sample_orphaned = list(orphaned_ids)[:3]
            logger.info(f"   - Примеры устаревших ID: {sample_orphaned}")
        
        if not orphaned_ids:
            logger.info("✅ Нет устаревших записей для удаления")
            return {
                'target_table': target_table,
                'total_existing': len(existing_ids),
                'total_new': len(new_ids),
                'deleted_records': 0,
                'status': 'success',
                'message': 'No orphaned records found'
            }
        
        # Delete orphaned records in batches
        batch_size = config.get('batch_size', 1000)
        total_orphaned = len(orphaned_ids)
        deleted_count = 0
        
        logger.info(f"🗑️ Начинаем удаление {total_orphaned} устаревших записей пакетами по {batch_size}")
        
        # Convert set to list for batching
        orphaned_ids_list = list(orphaned_ids)
        
        for i in range(0, total_orphaned, batch_size):
            batch_ids = orphaned_ids_list[i:i + batch_size]
            
            # Create placeholders for IN clause
            placeholders = ','.join(['%s'] * len(batch_ids))
            
            delete_query = f"""
            DELETE FROM {target_table}
            WHERE {key_column} IN ({placeholders})
            """
            
            logger.debug(f"🗑️ Выполняем DELETE: {delete_query}")
            logger.debug(f"🗑️ Параметры: {batch_ids[:3]}... (всего {len(batch_ids)})")
            
            # Execute delete
            with client.engine.connect() as conn:
                with conn.begin():
                    result = conn.execute(delete_query, batch_ids)
                    batch_deleted = result.rowcount
                    deleted_count += batch_deleted
                    
                    logger.info(f"🗑️ Пакет {i//batch_size + 1}: удалено {batch_deleted} записей")
                    logger.info(f"🗑️ Всего удалено: {deleted_count}/{total_orphaned}")
        
        # Log final statistics
        logger.info(f"🎯 Очистка завершена: удалено {deleted_count} устаревших записей")
        
        # Проверяем финальное состояние
        final_check_query = f"SELECT COUNT(*) as total FROM {target_table}"
        final_result = client.execute_query(final_check_query)
        final_count = final_result[0]['total'] if final_result else 0
        
        logger.info(f"📊 Финальное состояние: {final_count} записей в {target_table}")
        
        stats = {
            'target_table': target_table,
            'total_existing': len(existing_ids_normalized),
            'total_new': len(new_ids_normalized),
            'deleted_records': deleted_count,
            'final_count': final_count,
            'status': 'success',
            'message': f'Successfully deleted {deleted_count} orphaned records'
        }
        
        return stats
        
    except Exception as e:
        logger.exception(f"❌ Ошибка очистки устаревших записей: {str(e)}")
        return {
            'target_table': task_config.get('target_table', 'unknown'),
            'error': str(e),
            'status': 'failed'
        }
