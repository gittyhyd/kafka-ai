import sys
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    if len(sys.argv) < 4:
        print("Usage: python produce_message.py <topic> <partition> <message>")
        sys.exit(1)

    topic = sys.argv[1]
    partition = int(sys.argv[2])
    message = " ".join(sys.argv[3:])  # To handle message with spaces

    # Configure the Kafka producer
    conf = {
        'bootstrap.servers': 'localhost:9092',  # Change to your Kafka server address
    }
    
    producer = Producer(conf)

    # Produce the message to the specified topic and partition
    producer.produce(topic, key=None, value=message, partition=partition, callback=delivery_report)

    # Wait for any outstanding messages to be delivered
    producer.flush()

if __name__ == "__main__":
    main()
