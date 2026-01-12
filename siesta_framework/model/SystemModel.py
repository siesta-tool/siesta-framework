from typing import Dict, Any


DEFAULT_CONFIG: Dict[str, Any] = {
    # Storage configuration
    "storage_type": "s3",
    "log_name": "default_log",
    "storage_namespace": "siesta",
    
    # S3/MinIO configuration
    "s3_access_key": "",
    "s3_secret_key": "",
    "s3_endpoint": "http://localhost:9000",
    "s3_region": "us-east-1",
    
    # Database configuration
    "clear_existing": False,
    "is_continued": False,
    "is_streaming": False,
    
    # Spark configuration
    "spark_master": "local",
    "spark_app_name": "SiestaFramework",
}
