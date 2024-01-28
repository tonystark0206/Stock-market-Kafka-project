import logging
from kafka import KafkaConsumer
from json import loads, dump
from s3fs import S3FileSystem
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
bootstrap_servers = ['your_kafka_ip:9092']
topic = 'demo_test'
group_id = 'consumer_group'
auto_offset_reset = 'earliest'  # Start consuming from the earliest message
enable_auto_commit = False  # Manually commit offsets

# S3 configuration
s3_bucket = 'your_s3_bucket_name'
s3_prefix = 'kafka-stock-market-tutorial-youtube-darshil/'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    topic,
    group_id=group_id,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset=auto_offset_reset,
    enable_auto_commit=enable_auto_commit,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Initialize S3 filesystem
s3 = S3FileSystem()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def upload_to_s3(messages):
    """
    Upload messages to S3 in batch with retry logic.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    s3_path = f"{s3_prefix}stock_market_{timestamp}.json"
    with s3.open(s3_path, 'w') as file:
        dump(messages, file)
    logger.info(f"Uploaded {len(messages)} messages to S3: {s3_path}")

def consume_and_process():
    """
    Consume messages from Kafka, process them, and upload to S3.
    """
    batch_size = 1000  # Adjust batch size as needed
    messages = []
    
    for message in consumer:
        messages.append(message.value)
        
        if len(messages) >= batch_size:
            upload_to_s3(messages)
            messages = []  # Reset messages after upload
    
    # Upload remaining messages
    if messages:
        upload_to_s3(messages)
    
    # Commit offsets
    consumer.commit()

def main():
    try:
        consume_and_process()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
