import re
from confluent_kafka.admin import AdminClient

# Kafka broker configuration
KAFKA_BROKER = 'localhost:9092'  # Replace with your Kafka broker address

def list_topics_with_wildcards(pattern=''):
    # Create an AdminClient instance to interact with the Kafka cluster
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})

    # Replace '*' with regex equivalent to match any characters
    # For example, *test will match any topic ending with 'test'
    # test* will match any topic starting with 'test'
    regex_pattern = pattern.replace('*', '.*')

    try:
        # Get the list of all topics
        topics = admin_client.list_topics(timeout=10).topics

        # Filter topics using the regular expression
        filtered_topics = [
            topic for topic in topics
            if re.match(regex_pattern, topic)
        ]

        # Display the filtered topics
        if filtered_topics:
            print("Filtered Topics:")
            for topic in filtered_topics:
                print(topic)
        else:
            print("No topics match the given pattern.")

    except Exception as e:
        print(f"Error while listing topics: {e}")

# Example usage:
# List topics that match *test (ends with 'test')
list_topics_with_wildcards('*')

# List topics that match test* (starts with 'test')
#list_topics_with_wildcards('test*')

# List topics that match *test* (contains 'test')
#list_topics_with_wildcards('*test*')
