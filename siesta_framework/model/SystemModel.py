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


DEFAULT_MINING_CONFIG: Dict[str, Any] = {
  "log_name": "example_log",
  "storage_namespace": "siesta",
  "storage_type": "s3",
  "force_recompute": False,
  "categories": ["*"], # or specific list of template categories(e.g. ["ordered", "positional", "existence", "negation"])
  # "templates": ["*"], # or specific list of templates (e.g. ["response", "precedence"]) ----NOT IMPLEMENTED YET
  "grouping": "trace",  # or "window"
  "identifiers": ["trace_id"],  # fields to identify groups (e.g. 'trace_id' for trace-based, irrelevant for window-based)
  "window_size": 30,  # position-based window size, if grouping by window
  "output_path": "output/example_log", # where to store results
  "include_trace_lists": False, # whether to include the list of trace_ids supporting each constraint in the output (can be large)
  "branching_bound": 2,
  "branching_policy": "and",
  "support_threshold": 0.0,
  "confidence_threshold": 0.0,
}
