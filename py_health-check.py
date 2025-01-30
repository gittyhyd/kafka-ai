import time
from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.admin import  AdminClient

# Kafka configuration (change these to your Kafka broker details)
conf = {
    #'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker addresses
    'bootstrap.servers': 'localhost:9092',  # Change to your Kafka server address            
    'group.id': 'health-check-group',
    'auto.offset.reset': 'earliest'
}

# Initialize Kafka AdminClient for monitoring broker status
admin_client = AdminClient({'bootstrap.servers': conf['bootstrap.servers']})

# Function to check the health of Kafka brokers
def check_broker_health():
    try:
        # Fetch the broker metadata to check if brokers are reachable
        metadata = admin_client.list_topics(timeout=10)
        print("Kafka brokers are reachable.")
        return True
    except KafkaException as e:
        print(f"Error connecting to Kafka brokers: {e}")
        return False

# Function to check topic health (whether topics are available and healthy)
def check_topic_health():
    try:
        # List topics and check if they exist
        metadata = admin_client.list_topics(timeout=10)
        topics = metadata.topics
        if topics:
            print("Kafka topics are available:")
            for topic in topics:
                print(f" - {topic}")
        else:
            print("No topics found.")
        return True
    except KafkaException as e:
        print(f"Error checking topics: {e}")
        return False

# Function to check if consumer can connect to Kafka
def check_consumer_health():
    try:
        consumer = Consumer(conf)
        consumer.subscribe(['test-topic'])  # Replace with a valid topic name
        msg = consumer.poll(timeout=10)
        if msg is None:
            print("Consumer was unable to fetch messages in time.")
        elif msg.error():
            print(f"Consumer error: {msg.error()}")
        else:
            print(f"Consumer successfully received message: {msg.value()}")
        consumer.close()
        return True
    except KafkaException as e:
        print(f"Error with consumer: {e}")
        return False

# Main loop to constantly monitor Kafka health
def monitor_kafka_health():
    while True:
        print("\nChecking Kafka Health...")

        # Check broker, topic, and consumer health
        broker_status = check_broker_health()
        topic_status = check_topic_health()
        consumer_status = check_consumer_health()

        # Overall health status
        if broker_status and topic_status and consumer_status:
            print("Kafka Health Status: OK")
        else:
            print("Kafka Health Status: Issues detected")

        # Sleep before the next check
        time.sleep(30)  # Adjust the sleep interval as needed

# Start monitoring Kafka health
if __name__ == "__main__":
    monitor_kafka_health()
