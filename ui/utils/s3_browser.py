"""
S3 Storage Browser utilities.
Provides functions to browse and display S3 storage contents.
"""
import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Any, Optional
import pandas as pd
import streamlit as st


class S3Browser:
    """Browser for S3 storage in Siesta Framework."""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, region: str = 'us-east-1'):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.client = self._create_client()
    
    def _create_client(self):
        """Create boto3 S3 client."""
        try:
            return boto3.client(
                's3',
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region
            )
        except Exception as e:
            st.error(f"Failed to connect to S3: {e}")
            return None
    
    def list_buckets(self) -> List[str]:
        """List all available buckets."""
        if not self.client:
            return []
        
        try:
            response = self.client.list_buckets()
            return [bucket['Name'] for bucket in response.get('Buckets', [])]
        except ClientError as e:
            st.error(f"Error listing buckets: {e}")
            return []
    
    def list_logs(self, bucket: str) -> List[Dict[str, Any]]:
        """
        List all log prefixes (directories) in a bucket.
        
        Returns:
            List of dictionaries with log information
        """
        if not self.client:
            return []
        
        try:
            # List objects with delimiter to get "folders"
            response = self.client.list_objects_v2(
                Bucket=bucket,
                Delimiter='/'
            )
            
            logs = []
            for prefix in response.get('CommonPrefixes', []):
                log_name = prefix['Prefix'].rstrip('/')
                
                # Get metadata for this log
                metadata = self._get_log_metadata(bucket, log_name)
                logs.append({
                    'name': log_name,
                    'metadata': metadata
                })
            
            return logs
        except ClientError as e:
            st.error(f"Error listing logs: {e}")
            return []
    
    def _get_log_metadata(self, bucket: str, log_name: str) -> Dict[str, Any]:
        """Get metadata for a specific log."""
        if not self.client:
            return {}
        
        try:
            # Try to read metadata table
            metadata_path = f"{log_name}/metadata/"
            response = self.client.list_objects_v2(
                Bucket=bucket,
                Prefix=metadata_path,
                MaxKeys=5
            )
            
            objects = response.get('Contents', [])
            if objects:
                return {
                    'last_modified': max(obj['LastModified'] for obj in objects).isoformat(),
                    'total_size': sum(obj['Size'] for obj in objects),
                    'file_count': len(objects)
                }
            return {}
        except ClientError:
            return {}
    
    def list_tables(self, bucket: str, log_name: str) -> List[Dict[str, Any]]:
        """
        List all tables for a specific log.
        
        Returns:
            List of table information
        """
        if not self.client:
            return []
        
        try:
            prefix = f"{log_name}/"
            response = self.client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                Delimiter='/'
            )
            
            tables = []
            for common_prefix in response.get('CommonPrefixes', []):
                table_path = common_prefix['Prefix']
                table_name = table_path.replace(prefix, '').rstrip('/')
                
                # Get table info
                table_response = self.client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=table_path,
                    MaxKeys=10
                )
                
                contents = table_response.get('Contents', [])
                if contents:
                    tables.append({
                        'name': table_name,
                        'path': table_path,
                        'size': sum(obj['Size'] for obj in contents),
                        'files': len(contents),
                        'last_modified': max(obj['LastModified'] for obj in contents).isoformat() if contents else None
                    })
            
            return sorted(tables, key=lambda x: x['name'])
        except ClientError as e:
            st.error(f"Error listing tables: {e}")
            return []
    
    def get_object_info(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific S3 object."""
        if not self.client:
            return None
        
        try:
            response = self.client.head_object(Bucket=bucket, Key=key)
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'].isoformat(),
                'content_type': response.get('ContentType', 'Unknown'),
                'metadata': response.get('Metadata', {})
            }
        except ClientError as e:
            st.error(f"Error getting object info: {e}")
            return None
    
    def bucket_exists(self, bucket: str) -> bool:
        """Check if bucket exists."""
        if not self.client:
            return False
        
        try:
            self.client.head_bucket(Bucket=bucket)
            return True
        except ClientError:
            return False
