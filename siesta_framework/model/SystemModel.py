from typing import Dict, Any


DEFAULT_SYSTEM_CONFIG: Dict[str, Any] = {
    # Storage configuration
    "storage_type": "s3",
    "api": {
    "host": "0.0.0.0",
    "port": 8000
    },
    
    # S3/MinIO configuration
    "s3_access_key": "minioadmin",
    "s3_secret_key": "minioadmin",
    "s3_endpoint": "http://localhost:9000",
    "s3_region": "us-east-1",
    
    # Database configuration
    "empty_namespace": False,

    "enable_streaming": True,
    "enable_timing": True,
    
    # Spark configuration
    "spark_master": "spark://localhost:7077",
    "spark_app_name": "SiestaFramework",
    
    # Kafka configuration
    "kafka_bootstrap_servers": "localhost:9092",
    "raw_events_dir": "raw_events",
    "checkpoint_dir": "checkpoints",
}

DEFAULT_PREPROCESS_CONFIG: Dict[str, Any] = {
  "log_name": "example_log",
  "log_path": "../datasets/test.xes",
  "storage_namespace": "siesta",
  "clear_existing": False,
  "lookback": "7d",
  "kafka_topic": "example_log", # better match the log_name
  "field_mappings": {
    "xes": {
      "activity": "concept:name",
      "trace_id": "concept:name",
      "position": None,
      "start_timestamp": "time:timestamp",
      "attributes" : ["*"]
    },
    "csv": {
      "activity": "activity",
      "trace_id": "trace_id",
      "position": "position",
      "start_timestamp": "timestamp",
      "attributes" : ["resource", "cost"]
    },
    "json": {
        "activity": "activity",
        "trace_id": "caseID",
        "position": "position",
        "start_timestamp": "Timestamp"
    }
  },
  "trace_level_fields": ["trace_id"],
  "timestamp_fields": ["start_timestamp"]
}