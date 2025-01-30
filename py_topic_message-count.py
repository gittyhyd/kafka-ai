import sys
from confluent_kafka import Consumer, KafkaError, TopicPartition

def get_message_count(topic):
    # Configure the Kafka consumer
    conf = {
        #'bootstrap.servers': 'localhost:9092',  # Change to your Kafka server address
        'bootstrap.servers': 'localhost:9092',  # Change to your Kafka server address        
        'group.id': 'message-count-group',  # Temporary group ID
        'auto.offset.reset': 'earliest'
    }

    # Create a consumer instance
    consumer = Consumer(conf)

    try:
        # Get the topic's metadata
        metadata = consumer.list_topics(topic, timeout=10)

        if topic not in metadata.topics:
            print(f"Topic '{topic}' does not exist.")
            return

        partitions = metadata.topics[topic].partitions
        total_messages = 0

        for partition_id in partitions:
            # Create a TopicPartition object
            partition = TopicPartition(topic, partition_id)

            # Query the watermark offsets (earliest and latest)
            low, high = consumer.get_watermark_offsets(partition, timeout=10)

            # Number of messages is the difference between the high and low offsets
            total_messages += high - low

        print(f"Total messages in topic '{topic}': {total_messages}")

    except KafkaError as e:
        print(f"Error: {e}")
    finally:
        # Close the consumer
        consumer.close()

def main():
    if len(sys.argv) != 2:
        print("Usage: python get_message_count.py <topic>")
        sys.exit(1)

    topic = sys.argv[1]
    get_message_count(topic)

if __name__ == "__main__":
    main()
