from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

# Kafka broker and topic configuration
KAFKA_BROKER = 'localhost:9092'  # Replace with your Kafka broker address
TOPIC = 'my_topic'               # Replace with your topic name

# Callback function to handle message delivery confirmation
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Producer to send a message to the topic
def produce_message():
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})

    try:
        # Produce a message to the topic
        producer.produce(TOPIC, key='key', value='Hello, demo message 2', callback=delivery_report)
        producer.flush()  # Wait for all messages to be delivered
        print("Message sent successfully")
    except Exception as e:
        print(f"Error while producing message: {e}")


# Consumer to read messages from the topic
def consume_message():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'  # Start consuming from the earliest offset
    })

    try:
        # Subscribe to the topic
        consumer.subscribe([TOPIC])
        
        print("Waiting for messages...")
        
        # Consume messages
        while True:
            msg = consumer.poll(timeout=10.0)  # Timeout in seconds
            if msg is None:
                print("No message received within timeout period.")
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition()} at offset {msg.offset()}")
                else:
                    raise KafkaException(msg.error())
            else:
                print(f"Message received: {msg.value().decode('utf-8')} from partition {msg.partition()}")
                break  # Exit after receiving one message
    except Exception as e:
        print(f"Error while consuming message: {e}")
    finally:
        consumer.close()



# Main function to produce and consume a message
if __name__ == '__main__':
#    produce_message()  # Send a message to Kafka
    consume_message()  # Consume a message from Kafka


#docker exec -it kafka_container kafka-console-consumer --bootstrap-server localhost:9092 --topic my_topic --from-beginning
