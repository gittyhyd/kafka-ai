import time
from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka configuration (replace these with your Kafka broker details)
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'message-reader-group',     # Consumer group id
    'auto.offset.reset': 'latest'         # Start consuming from the beginning if no offset
}

# Initialize the Kafka consumer
consumer = Consumer(conf)

# Topic to read from (replace with your desired topic name)
topic_name = 'my_topic'  # Replace with the actual topic name

# Function to read messages from Kafka topic
def read_messages_from_topic():
    consumer.subscribe([topic_name])  # Subscribe to the topic

    try:
        # Poll for messages from the Kafka topic
        msg = consumer.poll(timeout=3.0)  # Wait for up to 10 seconds for new messages

        if msg is None:
            # print(f"No new messages in topic '{topic_name}' in the last 3 seconds.")
            print()
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached for topic '{topic_name}'.")
            else:
                print(f"Consumer error: {msg.error()}")
        else:
            # Process the received message
            print(f"Received message: {msg.value().decode('utf-8')}")

    except KafkaException as e:
        print(f"Error while consuming messages: {e}")

# Main loop to consume messages every 10 minutes
def consume_messages_every_10_minutes():
    while True:
        # print("\nChecking for new messages...")
        read_messages_from_topic()  # Read messages from the Kafka topic
        time.sleep(6)  # Wait for 10 minutes (600 seconds) before checking again

# Start consuming messages every 10 minutes
if __name__ == "__main__":
    try:
        consume_messages_every_10_minutes()
    except KeyboardInterrupt:
        print("\nProgram interrupted. Closing consumer.")
        consumer.close()

#docker exec -it kafka_container kafka-console-producer --broker-list localhost:9092 --topic my_topic
