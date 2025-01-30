from confluent_kafka.admin import AdminClient
from tabulate import tabulate
import sys

def describe_topic_configuration(broker, topic_name=None):
    """
    Fetch and display topic configuration, including
    partitions, replication factor, and retention policies.

    :param broker: Kafka broker address (e.g., "localhost:9092")
    :param topic_name: Kafka topic name to describe. If None, describe all topics.
    """
    try:
        # Initialize the Kafka Admin Client
        admin_client = AdminClient({
            'bootstrap.servers': broker
        })

        # Fetch metadata for all topics
        metadata = admin_client.list_topics(timeout=10)

        if topic_name:
            # Single topic description
            if topic_name not in metadata.topics:
                print(f"Topic '{topic_name}' does not exist.")
                return

            topics_to_describe = [topic_name]
        else:
            # Describe all topics
            topics_to_describe = list(metadata.topics.keys())

        topic_details = []

        for topic in topics_to_describe:
            topic_metadata = metadata.topics[topic]

            # Get partitions and replication factor
            partitions = len(topic_metadata.partitions)
            replication_factor = len(next(iter(topic_metadata.partitions.values())).replicas)

            # Fetch topic configuration using Admin API
            resource = admin_client.describe_configs([
                ('topic', topic)
            ])

            retention_ms = "N/A"
            retention_bytes = "N/A"

            for config_resource, config_values in resource.items():
                if config_resource.error:
                    print(f"Error fetching configs for {topic}: {config_resource.error}")
                    continue

                for key, value in config_values.items():
                    if key == "retention.ms":
                        retention_ms = value.value
                    elif key == "retention.bytes":
                        retention_bytes = value.value

            topic_details.append([
                topic, partitions, replication_factor, retention_ms, retention_bytes
            ])

        # Display topic details in tabular form
        print(tabulate(topic_details, headers=["Topic Name", "Partitions", "Replication Factor", "Retention (ms)", "Retention (bytes)"], tablefmt="grid"))

    except Exception as e:
        print(f"Error describing topic: {e}")


if __name__ == "__main__":
    # Kafka broker details
    kafka_broker = "localhost:9092"  # Change to your broker address

    # Get topic name from command-line arguments if provided
    topic = sys.argv[1] if len(sys.argv) > 1 else None

    # Describe topic configuration
    describe_topic_configuration(kafka_broker, topic)
