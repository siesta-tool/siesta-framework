#!/bin/bash
set -e

# Start Kafka in the background
/usr/bin/start-kafka.sh &
KAFKA_PID=$!

# Wait for Kafka to be ready on the INSIDE listener
echo "Waiting for Kafka to be ready..."
RETRIES=30
until kafka-topics.sh --bootstrap-server localhost:9093 --list &>/dev/null || [ $RETRIES -eq 0 ]; do
    echo "Waiting for Kafka... ($RETRIES retries left)"
    sleep 3
    RETRIES=$((RETRIES - 1))
done

if [ $RETRIES -eq 0 ]; then
    echo "ERROR: Kafka failed to start"
    exit 1
fi

echo "Kafka is ready!"

# Create default topics from KAFKA_CREATE_TOPICS environment variable
if [ ! -z "$KAFKA_CREATE_TOPICS" ]; then
    echo "creating topics: $KAFKA_CREATE_TOPICS"
    IFS=',' read -ra TOPICS <<< "$KAFKA_CREATE_TOPICS"
    for TOPIC_CONFIG in "${TOPICS[@]}"; do
        IFS=':' read -ra TOPIC_PARTS <<< "$TOPIC_CONFIG"
        TOPIC_NAME=${TOPIC_PARTS[0]}
        PARTITIONS=${TOPIC_PARTS[1]:-1}
        REPLICATION=${TOPIC_PARTS[2]:-1}
        
        echo "Creating topic: $TOPIC_NAME (partitions: $PARTITIONS, replication: $REPLICATION)"
        kafka-topics.sh --bootstrap-server localhost:9093 \
            --create \
            --if-not-exists \
            --topic "$TOPIC_NAME" \
            --partitions "$PARTITIONS" \
            --replication-factor "$REPLICATION" 2>&1
    done
    
    echo "Topics created successfully!"
    kafka-topics.sh --bootstrap-server localhost:9093 --list
fi

# Wait for Kafka process
wait $KAFKA_PID
