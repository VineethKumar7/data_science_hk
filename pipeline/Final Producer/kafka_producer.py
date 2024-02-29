from kafka import KafkaProducer
import json
import pandas as pd
import time  # Import the time module

# Configuration for the waiting period
wait_seconds = 5  # Time to wait between sending batches, in seconds

# Read the dataset
df = pd.read_csv("./dataset/STL_QUERY/sample.csv")
cols = [f'column{i}' for i in range(len(df.columns))]
df.columns = cols
time_column_name = df.columns[7]
df[time_column_name] = pd.to_datetime(df[time_column_name])
df.set_index(time_column_name, inplace=True)

# Group by 1-hour intervals
groups = df.resample('H')

# Kafka configuration
kafka_broker = 'localhost:9092'
topic_name = 'datascience'

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=[kafka_broker],
                         value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'))


def send_group_to_kafka(group_df):
    """
    Convert the group DataFrame to a JSON string and send it to Kafka.
    Each row in the DataFrame will be a separate message in Kafka.
    """
    for index, row in group_df.iterrows():
        # Convert each row to JSON format
        record = row.to_json()
        # Send the record to Kafka
        producer.send(topic_name, record)
        producer.flush()  # Ensure data is sent to Kafka
        print(f"Sent record to Kafka: {record}")


# Iterate over each group and send it to Kafka
for name, group in groups:
    if len(group) > 0:
        # Send each row of the group as a separate record to Kafka
        send_group_to_kafka(group)
        print(group)
        # Wait for the specified period before sending the next batch
        time.sleep(wait_seconds)
