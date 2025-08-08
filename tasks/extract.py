"""
PowerBI data extraction module
"""

import pandas as pd
from loguru import logger
from oneC_etl.services.powerbi.client import PowerBIClient

def extract_powerbi_data(task):
    """
    Extract data from PowerBI dataset
    
    Args:
        task (dict): Task configuration containing:
            - dataset_id: PowerBI dataset ID
            - dax_query: DAX query to execute
            - columns: Column mappings
    
    Returns:
        pd.DataFrame: Extracted data
    """
    try:
        logger.info(f"Extracting data from PowerBI dataset {task['dataset_id']}")
        
        # Initialize PowerBI client
        client = PowerBIClient()
        
        # Execute DAX query
        result = client.execute_query(
            dataset_id=task['dataset_id'],
            query=task['dax_query']
        )
        
        # Convert to DataFrame
        df = pd.DataFrame(result)
        
        # Rename columns if mappings provided
        if task.get('columns'):
            df = df.rename(columns=task['columns'])
        
        logger.info(f"Successfully extracted {len(df)} rows")
        return df
        
    except Exception as e:
        logger.exception(f"Error extracting data from PowerBI: {str(e)}")
        raise 