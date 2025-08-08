"""
Configuration management for Power BI ETL process
"""

import json
from airflow.models import Variable

# Default configuration values
DEFAULT_CONFIG = {
    'batch_size': 1000,
    'max_retries': 3,
    'retry_delay': 300,  # seconds
    'enable_vector_updates': True,
    'vector_model': 'text-embedding-ada-002'
}

def get_config():
    """
    Get configuration from Airflow variables or use defaults
    
    Returns:
        dict: Configuration dictionary with the following keys:
            - batch_size: Number of records to process in one batch
            - max_retries: Maximum number of retry attempts
            - retry_delay: Delay between retries in seconds
            - enable_vector_updates: Whether to enable vector search updates
            - vector_model: Model to use for vector embeddings
    """
    try:
        config = json.loads(Variable.get('powerbi_etl_config', default_var='{}'))
    except json.JSONDecodeError:
        config = {}
    
    return {
        'batch_size': int(config.get('batch_size', DEFAULT_CONFIG['batch_size'])),
        'max_retries': int(config.get('max_retries', DEFAULT_CONFIG['max_retries'])),
        'retry_delay': int(config.get('retry_delay', DEFAULT_CONFIG['retry_delay'])),
        'enable_vector_updates': bool(config.get('enable_vector_updates', DEFAULT_CONFIG['enable_vector_updates'])),
        'vector_model': config.get('vector_model', DEFAULT_CONFIG['vector_model'])
    } 