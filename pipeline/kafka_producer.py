from kafka import KafkaProducer
import csv
import json

# Configuration
kafka_broker = 'localhost:9092'  # Adjust this to your Kafka broker's address
topic_name = 'datascience'  # The Kafka topic you're sending data to
batch_size = 10  # Number of records to send in each batch
csv_file_path = '0000_part_00'  # Path to your CSV file

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=[kafka_broker],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def send_batch(batch):
    """Send a batch of messages to the Kafka topic."""
    producer.send(topic_name, batch)
    producer.flush()  # Ensure data is sent to Kafka
    print(f"Sent batch of {len(batch)} messages")

def read_and_send_data_in_batches(csv_file_path, batch_size):
    """Read CSV data and send it in batches."""
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) == batch_size:
                send_batch(batch)
                batch = []  # Reset batch after sending
        if batch:  # Send any remaining data as the last batch
            send_batch(batch)

# Execute the function to read data and send it in batches
read_and_send_data_in_batches(csv_file_path, batch_size)
