from typing import List, Literal, TypedDict, Any, NotRequired as Optional ,Dict, Any



###############################################
# TYPES
###############################################

# QUERY

# Equivalent to type QUERY_EVENT
class Query_Event(TypedDict, total=False):
    activity: str
    position: int
    symbol: Optional[str]

# Equivalent to type QUERY_CONTSRAINT
Query_Constraint = Any

Pattern = List[Query_Event]

# Equivalent to type QUERY_METHOD_INPUT
class Query_Method_Input(TypedDict, total=False):
    pattern: Pattern
    explore_mode: Optional[str] # "fast", "accurate", "hybrid"
    explore_k: Optional[int] # 0 = fast exploration
    constraints: Optional[List[Query_Constraint]]

# Equivalent to type QUERY_CONFIG
class Query_Config(TypedDict, total=False):
    log_name: str
    storage_namespace: str
    method: Literal['stats', 'patterns', 'detection', 'explore', 'violations']
    query: Query_Method_Input
    mode: Optional[str]

###############################################
# DEFAULTS
###############################################

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


DEFAULT_QUERY_CONFIG: Query_Config = {
    "log_name": "example_log",
    "storage_namespace": "siesta",
    "method": "stats",
    "query": {
        "pattern": [],
        # "constraints" is optional
    },
    # "mode" is optional
}


DEFAULT_MINING_CONFIG: Dict[str, Any] = {
  "log_name": "example_log",
  "storage_namespace": "siesta",
  "storage_type": "s3",
  "force_recompute": False,
  "categories": ["*"], # or specific list of template categories(e.g. ["ordered", "positional", "existence", "negation"])
  # "templates": ["*"], # TODO or specific list of templates (e.g. ["response", "precedence"])
  "grouping": "trace",  # or "window"
  "identifiers": ["trace_id"],  # fields to identify groups (e.g. 'trace_id' for trace-based, irrelevant for window-based)
  "window_size": 30,  # position-based window size, if grouping by window
  "output_path": "../output/example_log", # where to store results
  "include_trace_lists": False, # whether to include the list of trace_ids supporting each constraint in the output (can be large)
  "branching_bound": 2, #TODO
  "branching_policy": "and", #TODO
  "support_threshold": 0.0,
  "confidence_threshold": 0.0, #TODO
}

DEFAULT_COMPARATOR_CONFIG: Dict[str, Any] = {
  "log_name": "example_log",
  "storage_namespace": "siesta",
  "storage_type": "s3",

  # Comparison method: "ngrams", "rare_rules", or "targeted_rules"
  "method": "ngrams",

  # Method-specific parameters:
  #
  # method: "ngrams"
  #   "n"   (int)  - length of n-tuples to extract (e.g. 2 for bigrams)
  #   "vis" (bool) - generate an interactive HTML network visualization
  #
  # method: "rare_rules"
  #   (no extra params; uses top-level support_threshold)
  #
  # method: "targeted_rules"
  #   "target_label"       (int)   - label for target traces (default 1)
  #   "filtering_support"  (float) - min fraction of target traces a rule must
  #                                  appear in before final filtering (default 1.0)
  #   (uses top-level support_threshold)
  "method_params": {"n": 2, "vis": True},

  # Column whose values define the two groups to compare (e.g. "activity", "resource")
  "separating_key": "activity",

  # Two groups of values to compare; first group = target (label 1), second = control (label 0)
  "separating_groups": [["fail_1", "fail_2"], ["success_1", "success_2"]],

  # Minimum support threshold (interpretation varies by method — see method_params notes above)
  "support_threshold": 0.0,

  # Output directory; a Unix timestamp is automatically appended to avoid overwriting results
  "output_path": "../output/example_log",
}

