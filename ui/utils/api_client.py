"""
API client for Siesta Framework.
Handles communication with the FastAPI backend.
"""
import requests
import json
from typing import Dict, Any, Optional
import streamlit as st


class SiestaAPIClient:
    """Client for interacting with Siesta Framework API."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
    
    def preprocess_run(
        self, 
        preprocess_config: Dict[str, Any], 
        log_file: Optional[bytes] = None,
        filename: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Call the Preprocess module API endpoint.
        
        Args:
            preprocess_config: Configuration dictionary for preprocessing
            log_file: Optional file bytes to upload
            filename: Optional filename for the uploaded file
            
        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}/preprocessor/run"
        
        # Prepare the form data
        files = {}
        if log_file and filename:
            files['log_file'] = (filename, log_file)
        
        data = {
            'preprocess_config': json.dumps(preprocess_config)
        }
        
        try:
            response = requests.post(url, data=data, files=files, timeout=300)
            response.raise_for_status()
            return {
                'success': True,
                'data': response.json() if response.headers.get('content-type') == 'application/json' else response.text,
                'status_code': response.status_code
            }
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error': str(e),
                'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            }
    
    def mining_run(self, mining_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Call the Mining module API endpoint.
        
        Args:
            mining_config: Configuration dictionary for mining
            
        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}/miner/run"
        
        data = {
            'mining_config': json.dumps(mining_config)
        }
        
        try:
            response = requests.post(url, data=data, timeout=300)
            response.raise_for_status()
            return {
                'success': True,
                'data': response.json() if response.headers.get('content-type') == 'application/json' else response.text,
                'status_code': response.status_code
            }
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error': str(e),
                'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            }
    
    def health_check(self) -> bool:
        """Check if API is accessible."""
        try:
            response = requests.get(f"{self.base_url}/docs", timeout=5)
            return response.status_code == 200
        except:
            return False
