from kafka import KafkaConsumer
import json
import datetime
import os

# Kafka configuration
topic_name = 'datascience'
kafka_broker = 'localhost:9092'

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[kafka_broker],
    auto_offset_reset='earliest',
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Directory to save data files
data_directory = "./data"
os.makedirs(data_directory, exist_ok=True)

# Initialize an empty list to store messages
messages = []


# Function to save data
def save_data(messages, timestamp):
    filename = f"{data_directory}/data_{timestamp.strftime('%Y%m%d%H%M%S')}.json"
    with open(filename, 'w') as f:
        json.dump(messages, f)
    print(f"Saved {len(messages)} messages to {filename}")


# Setup timing mechanism
start_time = datetime.datetime.now()

# Processing messages
for message in consumer:
    current_time = datetime.datetime.now()
    # Print each received message
    print(f"Received message: {message.value}")
    messages.append(message.value)

    # Check if a minute has passed
    if (current_time - start_time).total_seconds() >= 10:
        # Save the aggregated messages
        save_data(messages, current_time)
        # Reset messages and start time for the next cycle
        messages = []
        start_time = current_time
