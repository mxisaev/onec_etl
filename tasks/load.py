"""
PostgreSQL data loading module
"""

import pandas as pd
from loguru import logger
from oneC_etl.services.postgres.client import PostgresClient
from oneC_etl.config.settings import get_config
from oneC_etl.config.dax_mappings import get_dax_mapping
from oneC_etl.utils.dax_utils import get_business_columns_from_dax

def execute_etl_task(data, task):
    """
    Execute ETL task - load data to PostgreSQL
    
    Args:
        data (pd.DataFrame): Data to load
        task (dict): Task configuration containing:
            - target_table: Target table name
            - source_table: Source table name
            - mapping_name: Name of the DAX mapping to use
    
    Returns:
        dict: Operation statistics
    """
    try:
        # Get configuration
        config = get_config()
        
        # Get DAX mapping
        mapping = get_dax_mapping(task['mapping_name'])
        
        # Initialize PostgreSQL client
        client = PostgresClient()
        
        # Prepare data for loading
        data['is_vector'] = False  # Mark records for vector update
        
        # Rename columns according to mapping
        data = data.rename(columns=mapping['columns'])
        
        # Use 'id' as the primary key
        key_columns = ['id']
        
        if 'id' not in data.columns:
            raise ValueError(f"Required column 'id' not found in data for table {task['target_table']}")
        
        # Получаем бизнес-колонки из DAX (для анализа изменений)
        business_columns = get_business_columns_from_dax(mapping['query'])
        # Исключаем технические поля и идентификаторы (оставляем только бизнес-характеристики)
        technical_fields = {'id', 'item_number', 'is_vector', 'upload_timestamp', 'updated_at', 'vector'}
        columns_for_change_analysis = [col for col in business_columns if col not in technical_fields]

        # Load data in batches
        batch_size = config['batch_size']
        total_rows = len(data)
        processed_rows = 0
        updated_rows = 0
        
        for i in range(0, total_rows, batch_size):
            batch = data.iloc[i:i + batch_size]
            
            # Определяем правильные типы колонок
            def get_column_type(col_name):
                if col_name == 'id':
                    return 'UUID'
                elif col_name in ['withdrawn_from_range', 'on_order', 'is_vector']:
                    return 'BOOLEAN'
                else:
                    return 'TEXT'
            
            # Merge data (upsert)
            result = client.merge_data(
                table_name=task['target_table'],
                data=batch,
                key_columns=key_columns,
                columns=[{
                    'name': col,
                    'dataType': get_column_type(col)
                } for col in batch.columns],
                template_name=mapping['table_template'],
                columns_for_change_analysis=columns_for_change_analysis
            )
            
            processed_rows += len(batch)
            updated_rows += result.get('updated_rows', 0)
            
            logger.info(f"📦 Обработано {processed_rows}/{total_rows} строк")
        
        stats = {
            'source': task['source_table'],
            'target': task['target_table'],
            'total_rows': total_rows,
            'processed_rows': processed_rows,
            'updated_rows': updated_rows,
            'status': 'success'
        }
        
        return stats
        
    except Exception as e:
        logger.exception(f"❌ Ошибка загрузки данных в PostgreSQL: {str(e)}")
        return {
            'source': task['source_table'],
            'target': task['target_table'],
            'error': str(e),
            'status': 'failed'
        } 