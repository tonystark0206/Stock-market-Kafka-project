import pandas as pd
from kafka import KafkaProducer
from json import dumps
import logging
import json
import random
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
bootstrap_servers = ['your_kafka_ip:9092']  # Replace 'your_kafka_ip' with actual Kafka broker IP
topic = 'demo_test'

# Producer configuration
producer_config = {
    'bootstrap_servers': bootstrap_servers,
    'value_serializer': lambda x: dumps(x).encode('utf-8'),
    # Add more producer configurations here if needed
}

def create_producer():
    """Create Kafka producer."""
    return KafkaProducer(**producer_config)

def send_messages(producer, messages):
    """Send messages in batch."""
    for message in messages:
        producer.send(topic, value=message)

def produce_messages():
    """Produce messages."""
    producer = create_producer()
    df = pd.read_csv("data/indexProcessed.csv")
    
    while True:
        try:
            # Sample random record from DataFrame
            dict_stock = df.sample(1).to_dict(orient="records")[0]
            # Send message asynchronously
            producer.send(topic, value=dict_stock)
            time.sleep(1)  # Introduce delay to control message rate
        except Exception as e:
            logger.error(f"Failed to produce message: {e}")
            # Depending on your requirements, you may implement retry logic here
            # or handle the error in a different way

def main():
    try:
        produce_messages()
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        logger.info("Flushing and closing the producer.")
        producer.flush()  # Ensure all messages are sent before closing the producer
        producer.close()  # Close the producer to release resources

if __name__ == "__main__":
    main()
  
