import pyspark
from pyspark.sql import SparkSession
import json

def main():
    # Initialize SparkSession
    spark = SparkSession.builder.appName("CSV to JSON Producer").getOrCreate()
    

    
    # Read the CSV file
    df = spark.read.csv('0000_part_00', header=True, inferSchema=True)
    
    # Convert DataFrame rows to JSON strings
    json_rdd = df.toJSON()
    
    # Simulate sending each JSON string to a consumer
    for json_string in json_rdd.collect():
        produce_message(json_string)
    
    # Stop the Spark session
    spark.stop()

def produce_message(message):
    """
    Simulate sending a message to a consumer.
    In a real application, this function would send the message to a Kafka topic or another message queue.
    """
    print(f"Producing message: {message}")

if __name__ == "__main__":
    main()