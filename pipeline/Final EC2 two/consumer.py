from kafka import KafkaConsumer
import json
import pandas as pd
from query_procesing import QueryProcessor  # Ensure this import matches your actual module and class name
import os
import datetime
from io import StringIO
import logging

# Kafka configuration
topic_name = 'datascience'
kafka_broker = 'localhost:9092'

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[kafka_broker],
    auto_offset_reset='earliest',
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # This lambda function correctly decodes and parses the JSON string
)

# Directory to save processed data
data_directory = "./data"
os.makedirs(data_directory, exist_ok=True)

# Initialize or load the processed data list
processed_data_list = []

# File to save processed data
filename = f"{data_directory}/processed_data_{datetime.datetime.now().strftime('%Y%m%d')}.json"


# Forgiving JSON deserializer
def forgiving_json_deserializer(v):
    if v is None:
        return None
    try:
        return json.loads(v.decode('utf-8'))
    except json.decoder.JSONDecodeError:
        logging.exception('Unable to decode: %s', v)
        return None



def process_message(data):
    global processed_data_list, filename

    if data is None:
        print("Received message that could not be decoded, skipping...")
        return

    try:
        # Create DataFrame directly from the Python object
        df = pd.DataFrame(data)
    except ValueError as e:
        print(f"Error creating DataFrame: {e}")
        return  # Exit the function to avoid further processing

    try:
        qp = QueryProcessor(df=df)
        qp.find_query_instance_count(table="instance_count", df=df)

        processed_data_list.append(len(qp.QUERY_HISTORY))

        with open(filename, 'w') as f:
            json.dump(processed_data_list, f)
        print(f"Updated QUERY_HISTORY length: {len(qp.QUERY_HISTORY)} saved to {filename}")
    except Exception as e:
        print(f"Error during query processing or saving processed data: {e}")

def show_message(message):
    print(message)

# Processing messages from Kafka
for message in consumer:
    process_message(message)
    # show_message(message)