#!/usr/bin/env python3
"""
Kafka Consumer Script for reading messages from Kafka topics.

This script consumes messages from a Kafka topic and prints them to stdout.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, KafkaError

# Import default config from SystemModel
try:
    from siesta.model.SystemModel import DEFAULT_SYSTEM_CONFIG, DEFAULT_INDEX_CONFIG
    DEFAULT_SYSTEM_CONFIG = DEFAULT_SYSTEM_CONFIG | DEFAULT_INDEX_CONFIG
except ImportError:
    # Fallback if running from different location
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from model.SystemModel import DEFAULT_SYSTEM_CONFIG
    from model.SystemModel import DEFAULT_PREPROCESS_CONFIG
    DEFAULT_SYSTEM_CONFIG = DEFAULT_SYSTEM_CONFIG | DEFAULT_INDEX_CONFIG


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
    return DEFAULT_SYSTEM_CONFIG


def create_consumer(bootstrap_servers: str, group_id: str = "kafka-consumer-script") -> Consumer:
    """Create and configure a Kafka consumer.
    
    Args:
        bootstrap_servers: Kafka broker address(es)
        group_id: Consumer group ID
        
    Returns:
        Configured Kafka Consumer instance
    """
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start from beginning if no offset exists
        'enable.auto.commit': True,
    }
    
    return Consumer(config)


def consume_messages(
    topic: str,
    bootstrap_servers: str = "172.17.0.1:9092",
    group_id: str = "kafka-consumer-script",
    max_messages: int = None,
    timeout: float = 1.0
):
    """Consume and print messages from a Kafka topic.
    
    Args:
        topic: Topic name to consume from
        bootstrap_servers: Kafka broker address
        group_id: Consumer group ID
        max_messages: Maximum number of messages to consume (None = unlimited)
        timeout: Poll timeout in seconds
    """
    consumer = create_consumer(bootstrap_servers, group_id)
    
    try:
        # Subscribe to topic
        consumer.subscribe([topic])
        print(f"Subscribed to topic: {topic}")
        print(f"Kafka broker: {bootstrap_servers}")
        print(f"Consumer group: {group_id}")
        print("-" * 60)
        
        message_count = 0
        
        while True:
            # Poll for messages
            msg = consumer.poll(timeout=timeout)
            
            if msg is None:
                # No message received within timeout
                if max_messages and message_count >= max_messages:
                    break
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition reached
                    print(f"Reached end of partition {msg.partition()}")
                else:
                    print(f"Error: {msg.error()}", file=sys.stderr)
                continue
            
            # Decode and print message
            try:
                value = msg.value().decode('utf-8')
                # Try to pretty-print JSON
                try:
                    json_value = json.loads(value)
                    print(json.dumps(json_value, indent=2))
                except json.JSONDecodeError:
                    # Not JSON, print as-is
                    print(value)
            except Exception as e:
                print(f"Error decoding message: {e}", file=sys.stderr)
                print(f"Raw value: {msg.value()}")
            
            message_count += 1
            
            # Check if we've reached max messages
            if max_messages and message_count >= max_messages:
                print(f"\n{'-' * 60}")
                print(f"Consumed {message_count} messages")
                break
                
    except KeyboardInterrupt:
        print(f"\n{'-' * 60}")
        print(f"Interrupted. Consumed {message_count} messages")
    finally:
        # Close consumer
        consumer.close()
        print("Consumer closed")


def main():
    """Parse arguments and start consuming messages."""
    parser = argparse.ArgumentParser(
        description="Consume messages from a Kafka topic",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to config file (optional, defaults to SystemModel.DEFAULT_CONFIG)'
    )
    
    parser.add_argument(
        '--topic',
        type=str,
        help='Kafka topic to consume from (overrides config)'
    )
    
    parser.add_argument(
        '--bootstrap-server',
        type=str,
        help='Kafka bootstrap server address (host:port) (overrides config)'
    )
    
    parser.add_argument(
        '--group-id',
        type=str,
        default='kafka-consumer-script',
        help='Consumer group ID'
    )
    
    parser.add_argument(
        '--max-messages',
        type=int,
        default=None,
        help='Maximum number of messages to consume (unlimited if not specified)'
    )
    
    parser.add_argument(
        '--timeout',
        type=float,
        default=1.0,
        help='Poll timeout in seconds'
    )
    
    args = parser.parse_args()
    
    # Load configuration (uses DEFAULT_CONFIG if no config file specified)
    print("Loading configuration...")
    config = load_config(args.config)
    
    # Get connection parameters from config or args
    bootstrap_servers = args.bootstrap_server or config.get('kafka_bootstrap_servers', '172.17.0.1:9092')
    topic = args.topic or config.get('kafka_topic', 'log_events')
    
    print(f"Using topic: {topic}")
    print(f"Using bootstrap servers: {bootstrap_servers}")
    
    try:
        consume_messages(
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            group_id=args.group_id,
            max_messages=args.max_messages,
            timeout=args.timeout
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
