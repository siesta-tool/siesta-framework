from typing import Dict, Any


DEFAULT_CONFIG: Dict[str, Any] = {
    # Storage configuration
    "storage_type": "s3",
    "log_name": "default_log",
    "storage_namespace": "siesta",
    "log_path": "datasets/test.xes",
    
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
    
    # Log parsing configuration
    "field_mappings": {
        "xes": {
            "activity": "concept:name",
            "trace_id": "concept:name",
            "position": None,  # Computed from sequence
            "start_timestamp": "time:timestamp",
        },
        "csv": {
            "activity": "activity",
            "trace_id": "caseID",
            "position": "position",
            "start_timestamp": "Timestamp",
        }
    },
    "trace_level_fields": ["trace_id"],
    "timestamp_fields": ["start_timestamp"],
}
