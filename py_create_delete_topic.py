from confluent_kafka.admin import AdminClient, NewTopic

# Kafka configuration (update with your Kafka broker addresses)
conf = {
    'bootstrap.servers': 'localhost:9092'  # Change to your Kafka brokers
}

# Initialize AdminClient
admin_client = AdminClient(conf)

# Function to create a Kafka topic
def create_topic(topic_name, num_partitions=1, replication_factor=1):
    new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    
    # Create the topic asynchronously
    fs = admin_client.create_topics([new_topic])

    # Wait for the operation to complete
    try:
        fs[topic_name].result()  # This will raise an exception if the creation fails
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")

# Function to delete a Kafka topic
def delete_topic(topic_name):
    # Delete the topic asynchronously
    fs = admin_client.delete_topics([topic_name])

    # Wait for the operation to complete
    try:
        fs[topic_name].result()  # This will raise an exception if the deletion fails
        print(f"Topic '{topic_name}' deleted successfully.")
    except Exception as e:
        print(f"Failed to delete topic '{topic_name}': {e}")

# Example Usage
if __name__ == "__main__":
    # Create a topic
    # create_topic('demo-topic')

    # Delete a topic (uncomment after testing the creation)
     delete_topic('demo-topic')
