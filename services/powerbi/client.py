"""
PowerBI API client
"""

import os
import json
from loguru import logger
from msal import ConfidentialClientApplication
from airflow.models import Variable

class PowerBIClient:
    """PowerBI API client for data extraction"""
    
    def __init__(self):
        """Initialize PowerBI client with credentials from Airflow Variables"""
        try:
            # Get credentials from separate Airflow Variables
            self.client_id = Variable.get('powerbi_client_id')
            self.client_secret = Variable.get('powerbi_client_secret')
            self.tenant_id = Variable.get('powerbi_tenant_id')
            self.workspace_id = Variable.get('powerbi_workspace_id')
            
            if not all([self.client_id, self.client_secret, self.tenant_id, self.workspace_id]):
                raise ValueError("PowerBI credentials not found in Airflow Variables")
            
            # Initialize MSAL client
            self.client = ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}"
            )
            
            # Get access token
            self.token = self._get_access_token()
            
        except Exception as e:
            logger.exception(f"Error initializing PowerBI client: {str(e)}")
            raise
    
    def _get_access_token(self):
        """Get PowerBI API access token"""
        try:
            result = self.client.acquire_token_for_client(
                scopes=["https://analysis.windows.net/powerbi/api/.default"]
            )
            
            if "access_token" not in result:
                error_msg = f"Failed to acquire access token. Response: {json.dumps(result)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            return result["access_token"]
            
        except Exception as e:
            logger.exception(f"Error getting access token: {str(e)}")
            raise
    
    def execute_query(self, dataset_id, query):
        """
        Execute DAX query against PowerBI dataset
        
        Args:
            dataset_id (str): PowerBI dataset ID
            query (str): DAX query to execute
            
        Returns:
            list: Query results
        """
        try:
            import requests
            
            # Prepare request
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspace_id}/datasets/{dataset_id}/executeQueries"
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }
            body = {
                "queries": [{
                    "query": query,
                    "kind": "DAX"
                }],
                "serializerSettings": {
                    "includeNulls": True
                }
            }
            
            # Log request details (excluding sensitive data)
            # logger.info(f"Request URL: {url}")  # Убрано по требованию
            # logger.info(f"Request headers: {json.dumps({k: v for k, v in headers.items() if k != 'Authorization'})}")  # Убрано по требованию
            # logger.info(f"Request body: {json.dumps(body, indent=2, ensure_ascii=False)}")  # Убрано по требованию
            
            # Execute request
            response = requests.post(url, headers=headers, json=body)
            
            if response.status_code != 200:
                error_msg = f"Error response: {response.text}"
                logger.error(error_msg)
                response.raise_for_status()
            
            # Parse results
            result = response.json()
            
            if 'results' not in result or not result['results']:
                raise ValueError("No results found in response")
                
            if 'tables' not in result['results'][0] or not result['results'][0]['tables']:
                raise ValueError("No tables found in results")
                
            if 'rows' not in result['results'][0]['tables'][0]:
                raise ValueError("No rows found in table")
            
            return result['results'][0]['tables'][0]['rows']
            
        except requests.exceptions.RequestException as e:
            logger.exception(f"Request error executing PowerBI query: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response content: {e.response.text}")
            raise
        except Exception as e:
            logger.exception(f"Error executing PowerBI query: {str(e)}")
            raise 