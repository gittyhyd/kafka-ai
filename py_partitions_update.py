import subprocess
from confluent_kafka.admin import AdminClient

def show_and_update_partitions(admin_client, topic_name, num_partitions, kafka_server):
    """Show the current number of partitions and update if needed."""
    try:
        metadata = admin_client.list_topics(timeout=10)
        
        if topic_name in metadata.topics:
            existing_partitions = len(metadata.topics[topic_name].partitions)
            print(f"\nCurrent number of partitions for '{topic_name}': {existing_partitions}")
            
            if existing_partitions < num_partitions:
                print(f"\nYou are about to update '{topic_name}' to have {num_partitions} partitions.")
                confirm = input("Do you want to proceed? (yes/no): ").strip().lower()
                
                if confirm == "yes":
                    cmd = [
                        "docker", "exec", "kafka",
                        "kafka-topics", "--alter", "--topic", topic_name,
                        "--partitions", str(num_partitions),
                        "--bootstrap-server", kafka_server
                    ]
                    try:
                        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                        print("\nPartitions updated successfully!")
                        print(result.stdout)
                    except subprocess.CalledProcessError as e:
                        print(f"\nFailed to update partitions: {e.stderr}")
                else:
                    print("\nPartition update canceled by the user.")
            else:
                print(f"\nTopic '{topic_name}' already has {existing_partitions} partitions or more.")
        else:
            print(f"\nTopic '{topic_name}' does not exist.")
    except Exception as e:
        print(f"\nError: {e}")

def main():
    kafka_server = "localhost:9092"  # Change this to your Kafka broker address
    admin_client = AdminClient({"bootstrap.servers": kafka_server})
    
    metadata = admin_client.list_topics(timeout=10)
    print("\nExisting Kafka Topics:")
    for topic in metadata.topics:
        print(f"- {topic}")

    topic_name = input("\nEnter the Kafka topic name to update partitions: ").strip()
    
    while True:
        try:
            num_partitions = int(input(f"Enter the desired number of partitions for '{topic_name}': "))
            if num_partitions > 0:
                break
            else:
                print("Number of partitions must be greater than 0.")
        except ValueError:
            print("Please enter a valid integer for the number of partitions.")
    
    show_and_update_partitions(admin_client, topic_name, num_partitions, kafka_server)

if __name__ == "__main__":
    main()
