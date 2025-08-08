"""
Airflow Variables configuration handler
"""

from airflow.models import Variable
from typing import Dict, Any, Optional
from loguru import logger
import json
import re

def get_dataset_config(dataset_name: str) -> Dict[str, Any]:
    """
    Get dataset configuration from Airflow Variables
    
    Args:
        dataset_name (str): Name of the dataset configuration to retrieve
        
    Returns:
        Dict[str, Any]: Dataset configuration
    """
    try:
        datasets = Variable.get("datasets", deserialize_json=True)
        if not datasets or dataset_name not in datasets:
            raise ValueError(f"Dataset configuration '{dataset_name}' not found in Airflow Variables")
        return datasets[dataset_name]
    except Exception as e:
        logger.exception(f"Error retrieving dataset configuration: {str(e)}")
        raise

def get_available_datasets() -> Dict[str, str]:
    """
    Get list of available datasets with their descriptions
    
    Returns:
        Dict[str, str]: Dictionary of dataset names and their descriptions
    """
    try:
        datasets = Variable.get("datasets", deserialize_json=True)
        if not datasets:
            return {}
        return {name: config.get("description", "") for name, config in datasets.items()}
    except Exception as e:
        logger.exception(f"Error retrieving available datasets: {str(e)}")
        return {}

def get_available_queries() -> Dict[str, str]:
    """
    Get list of available DAX queries with their descriptions
    
    Returns:
        Dict[str, str]: Dictionary of query names and their descriptions
    """
    try:
        queries = Variable.get("dax_queries", deserialize_json=True)
        if not queries:
            return {}
        return {name: config.get("description", "") for name, config in queries.items()}
    except Exception as e:
        logger.exception(f"Error retrieving available queries: {str(e)}")
        return {}

def get_dax_query(query_name: str) -> Dict[str, Any]:
    """
    Get DAX query configuration from Airflow Variables
    
    Args:
        query_name (str): Name of the query configuration to retrieve
        
    Returns:
        Dict[str, Any]: Query configuration
    """
    try:
        queries = Variable.get("dax_queries", deserialize_json=True)
        if not queries or query_name not in queries:
            raise ValueError(f"Query configuration '{query_name}' not found in Airflow Variables")
        return queries[query_name]
    except Exception as e:
        logger.exception(f"Error retrieving query configuration: {str(e)}")
        raise 