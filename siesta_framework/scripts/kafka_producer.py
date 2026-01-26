#!/usr/bin/env python3
"""
Kafka Producer Script for sending event data to Kafka topics.

This script reads events from a file or generates sample events and sends them
to a Kafka topic for streaming processing.
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from confluent_kafka import Producer

# Import default config from SystemModel
try:
    from siesta_framework.model.SystemModel import DEFAULT_CONFIG
except ImportError:
    # Fallback if running from different location
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from model.SystemModel import DEFAULT_CONFIG


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from JSON file or use defaults.
    
    Args:
        config_path: Path to config file. If None or file doesn't exist, uses DEFAULT_CONFIG.
        
    Returns:
        Configuration dictionary
    """
    if config_path and Path(config_path).exists():
        with open(config_path, 'r') as f:
            return json.load(f)
    elif config_path:
        print(f"Warning: Config file '{config_path}' not found, using DEFAULT_CONFIG from SystemModel")
    
    # Use default config from SystemModel
    return DEFAULT_CONFIG


def get_json_field_mappings(config: Dict[str, Any]) -> Dict[str, str]:
    """Extract JSON field mappings from config.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Dictionary mapping internal field names to JSON field names
    """
    # Try to get json mappings, fall back to csv mappings if not present
    field_mappings = config.get("field_mappings", {})
    json_mappings = field_mappings.get("json")
    
    if json_mappings:
        return json_mappings
    
    # If no json mappings, use csv mappings as fallback
    csv_mappings = field_mappings.get("csv", {})
    print("Warning: No 'json' field mappings found, using 'csv' mappings as fallback")
    return csv_mappings


def create_producer(bootstrap_servers: str) -> Producer:
    """Create and return a Kafka producer instance.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers (e.g., 'localhost:9092')
        
    Returns:
        Producer instance
    """
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'acks': 'all'
    }
    return Producer(conf)


def delivery_callback(err, msg):
    """Callback for message delivery reports."""
    if err:
        print(f"✗ Failed to send event: {err}", file=sys.stderr)
    else:
        print(f"✓ Sent event to {msg.topic()} partition {msg.partition()} offset {msg.offset()}")


def send_event(producer: Producer, topic: str, event: Dict[str, Any], key: str = None):
    """Send a single event to Kafka topic.
    
    Args:
        producer: Producer instance
        topic: Topic name
        event: Event data as dictionary
        key: Optional partition key
    """
    try:
        value = json.dumps(event).encode('utf-8')
        key_bytes = key.encode('utf-8') if key else None
        producer.produce(topic, value=value, key=key_bytes, callback=delivery_callback)
        producer.poll(0)  # Trigger callback
    except Exception as e:
        print(f"✗ Failed to send event: {e}", file=sys.stderr)


def generate_sample_events(field_mappings: Dict[str, str], count: int = 10):
    """Generate sample log events for testing.
    
    Events are generated based on the field mappings from config, with all values as strings.
    
    Args:
        field_mappings: Mapping from internal fields to JSON field names
        count: Number of events to generate
        
    Yields:
        Event dictionaries
    """
    activities = ["A", "B", "C", "D"]
    
    # Get field names from mappings; the keys below should match the DataModel.Event fields
    trace_id_field = field_mappings.get("trace_id", "caseID")
    activity_field = field_mappings.get("activity", "activity")
    position_field = field_mappings.get("position", "position")
    timestamp_field = field_mappings.get("start_timestamp", "Timestamp")
    
    for i in range(count):
        event = {}
        if trace_id_field:
            event[trace_id_field] = str(f"trace_{i % 3 + 1}")
        if activity_field:
            event[activity_field] = str(activities[i % len(activities)])
        if position_field:
            event[position_field] = str((i % 4))  # Position in trace (0-based)
        if timestamp_field:
            event[timestamp_field] = datetime.now().isoformat()
        
        yield event


def main():
    """Main function to run the Kafka producer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Send events to Kafka topic')
    parser.add_argument('--config', type=str,
                        help='Path to config file (optional, defaults to SystemModel.DEFAULT_CONFIG)')
    parser.add_argument('--bootstrap-servers', type=str,
                        help='Kafka bootstrap servers (overrides config)')
    parser.add_argument('--topic', type=str,
                        help='Kafka topic name (overrides config)')
    parser.add_argument('--count', type=int, default=10,
                        help='Number of sample events to generate (default: 10)')
    parser.add_argument('--delay', type=float, default=0.5,
                        help='Delay between events in seconds (default: 0.5)')
    parser.add_argument('--file', type=str,
                        help='JSON file with events to send (one event per line)')
    
    args = parser.parse_args()
    
    # Load configuration (uses DEFAULT_CONFIG if no config file specified)
    print("Loading configuration...")
    config = load_config(args.config)
    field_mappings = get_json_field_mappings(config)
    print(f"Using field mappings: {field_mappings}")
    
    # Get connection parameters from config or args
    bootstrap_servers = args.bootstrap_servers or config.get('kafka_bootstrap_servers', '172.17.0.1:9092')
    topic = args.topic or config.get('kafka_topic', 'log_events')
    trace_id_field = field_mappings.get('trace_id', 'trace_id')
    
    print(f"Connecting to Kafka at {bootstrap_servers}...")
    producer = create_producer(bootstrap_servers)
    
    try:
        if args.file:
            # Read events from file
            print(f"Reading events from {args.file}")
            with open(args.file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        event = json.loads(line.strip())
                        send_event(producer, topic, event, key=event.get(trace_id_field))
                        time.sleep(args.delay)
                    except json.JSONDecodeError as e:
                        print(f"✗ Error parsing line {line_num}: {e}", file=sys.stderr)
        else:
            # Generate sample events
            print(f"Generating {args.count} sample events...")
            for event in generate_sample_events(field_mappings, args.count):
                send_event(producer, topic, event, key=event.get(trace_id_field))
                time.sleep(args.delay)
        
        print(f"\n✓ Successfully sent events to topic '{topic}'")
        
    except KeyboardInterrupt:
        print("\n⚠ Interrupted by user")
    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        # Wait for any outstanding messages to be delivered
        print("Flushing remaining messages...")
        producer.flush()
        print("✓ Producer closed")
        print("✓ Producer closed")


if __name__ == "__main__":
    main()
