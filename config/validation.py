"""
Configuration validation module
"""

from typing import Dict, Any, Tuple
from loguru import logger
from airflow.models import Variable

def validate_dataset_config(dataset_name: str) -> Tuple[bool, str]:
    """
    Validate dataset configuration
    
    Args:
        dataset_name (str): Name of the dataset to validate
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    try:
        datasets = Variable.get("datasets", deserialize_json=True)
        if not datasets:
            return False, "No datasets configured in Airflow Variables"
            
        if dataset_name not in datasets:
            return False, f"Dataset '{dataset_name}' not found in Airflow Variables"
            
        dataset = datasets[dataset_name]
        required_fields = ["id", "source_table", "target_table"]
        missing_fields = [field for field in required_fields if field not in dataset]
        
        if missing_fields:
            return False, f"Dataset '{dataset_name}' missing required fields: {', '.join(missing_fields)}"
            
        return True, ""
        
    except Exception as e:
        return False, f"Error validating dataset: {str(e)}"

def validate_query_config(query_name: str) -> Tuple[bool, str]:
    """
    Validate DAX query configuration
    
    Args:
        query_name (str): Name of the query to validate
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    try:
        queries = Variable.get("dax_queries", deserialize_json=True)
        if not queries:
            return False, "No DAX queries configured in Airflow Variables"
            
        if query_name not in queries:
            return False, f"DAX query '{query_name}' not found in Airflow Variables"
            
        query = queries[query_name]
        required_fields = ["query"]
        missing_fields = [field for field in required_fields if field not in query]
        
        if missing_fields:
            return False, f"DAX query '{query_name}' missing required fields: {', '.join(missing_fields)}"
            
        return True, ""
        
    except Exception as e:
        return False, f"Error validating DAX query: {str(e)}"

def validate_dag_config(dag_id: str, dataset_name: str, query_name: str) -> Tuple[bool, str]:
    """
    Validate complete DAG configuration
    
    Args:
        dag_id (str): DAG ID to validate
        dataset_name (str): Dataset name to validate
        query_name (str): Query name to validate
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    # Validate DAG ID
    if not dag_id or not isinstance(dag_id, str):
        return False, "Invalid DAG ID"
        
    # Validate dataset
    dataset_valid, dataset_error = validate_dataset_config(dataset_name)
    if not dataset_valid:
        return False, dataset_error
        
    # Validate query
    query_valid, query_error = validate_query_config(query_name)
    if not query_valid:
        return False, query_error
        
    return True, "" 