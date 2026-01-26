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

    "enable_streaming": True,
    
    # Spark configuration
    "spark_master": "spark://localhost:7077",
    "spark_app_name": "SiestaFramework",
    
    # Kafka configuration
    "kafka_bootstrap_servers": "localhost:9092",
    "kafka_topic": "log_events",
    "raw_events_dir": "raw_events",
    "checkpoint_dir": "checkpoints",

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
        }, 
        "json": {
            "activity": "activity",
            "trace_id": "caseID",
            "position": "position",
            "start_timestamp": "Timestamp",
        }
    },
    "trace_level_fields": ["trace_id"],
    "timestamp_fields": ["start_timestamp"],
}
