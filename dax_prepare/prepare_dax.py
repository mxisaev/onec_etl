#!/usr/bin/env python3
"""
Prepare DAX queries for Airflow Variables
This script processes raw DAX queries and prepares them for Airflow Variables.
"""

import json
import re
import sys
from pathlib import Path
from loguru import logger
from typing import Dict, Any

# Add parent directory to path to import raw_queries
sys.path.append(str(Path(__file__).parent))
from raw_queries import *  # Import all query variables

def clean_dax_query(query: str) -> str:
    """
    Clean DAX query for JSON storage
    
    Args:
        query (str): Raw DAX query
        
    Returns:
        str: Cleaned DAX query
    """
    # Replace Windows line endings with spaces
    query = query.replace('\r\n', ' ')
    
    # Remove control characters
    query = ''.join(char for char in query if ord(char) >= 32 or char in '\n\r\t')
    
    # Replace multiple spaces with single space
    query = re.sub(r'\s+', ' ', query)
    
    # Strip leading/trailing whitespace
    return query.strip()

def get_query_variables() -> Dict[str, str]:
    """
    Get all DAX query variables from raw_queries.py
    
    Returns:
        Dict[str, str]: Dictionary of query names and their values
    """
    # Get all variables from raw_queries module
    query_vars = {
        name: value for name, value in globals().items()
        if name.endswith('_QUERY') and isinstance(value, str)
    }
    return query_vars

def prepare_queries() -> Dict[str, Dict[str, str]]:
    """
    Prepare DAX queries for Airflow Variables
    
    Returns:
        Dict[str, Dict[str, str]]: Processed queries ready for Airflow Variables
    """
    try:
        # Get all query variables
        query_vars = get_query_variables()
        
        if not query_vars:
            raise ValueError("No DAX queries found in raw_queries.py")
        
        # Process each query
        result = {}
        for query_name, query in query_vars.items():
            # Convert query name to DAG name format with special mapping
            if query_name == 'PARTNERS_QUERY':
                dag_name = 'partners'
            elif query_name == 'COMPANY_PRODUCTS_QUERY':
                dag_name = 'company_products'
            else:
                dag_name = query_name.lower().replace('_query', '')
            
            # Clean and store query
            cleaned_query = clean_dax_query(query)
            result[dag_name] = {
                "description": f"DAX query for {query_name}",
                "query": cleaned_query
            }
            
            logger.info(f"Processed query: {query_name} -> {dag_name}")
            logger.debug(f"Original query: {query}")
            logger.debug(f"Cleaned query: {cleaned_query}")
        
        return result
        
    except Exception as e:
        logger.exception(f"Error preparing DAX queries: {str(e)}")
        raise

def save_to_json(queries: Dict[str, Dict[str, str]], output_file: str) -> None:
    """
    Save processed queries to JSON file
    
    Args:
        queries (Dict[str, Dict[str, str]]): Processed queries
        output_file (str): Path to output JSON file
    """
    try:
        # Save to output file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(queries, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Successfully saved queries to: {output_file}")
        
        # Validate output
        with open(output_file, 'r', encoding='utf-8') as f:
            json.load(f)
        logger.info("Output file is valid JSON")
            
    except Exception as e:
        logger.exception(f"Error saving queries: {str(e)}")
        raise

def main():
    """Main function to prepare and save DAX queries"""
    try:
        # Prepare queries
        queries = prepare_queries()
        
        # Save to JSON
        output_file = Path(__file__).parent / 'dax_queries.json'
        save_to_json(queries, str(output_file))
        
        logger.info("DAX query preparation completed successfully")
        
    except Exception as e:
        logger.exception("Error in main function")
        sys.exit(1)

if __name__ == '__main__':
    main() 