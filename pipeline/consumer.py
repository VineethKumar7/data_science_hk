from kafka import KafkaConsumer
import json

# Kafka configuration
topic_name = 'your_topic_name'  # The Kafka topic you're subscribing to
kafka_broker = 'localhost:9092'  # Adjust this to your Kafka broker's address

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[kafka_broker],
    auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id='my-group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserializer for JSON
)

# Print messages received from the topic
for message in consumer:
    print(f"Received message: {message.value}")
