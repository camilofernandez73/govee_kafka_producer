from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
import logging


def try_create_producer(producer_config, max_retries=9999, delay=10):
    """
    Attempts to create a KafkaProducer instance with retries.

    Parameters:
    - bootstrap_servers: List of Kafka server addresses.
    - max_retries: Maximum number of retry attempts.
    - delay: Delay between retry attempts in seconds.
    
    Returns:
    - A KafkaProducer instance if successful, None otherwise.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"Attempt {attempt} to connect to Kafka...")
            producer = KafkaProducer(**producer_config)
            logging.info("Kafka connection established.")
            return producer
        except NoBrokersAvailable:
            logging.error(f"Kafka server not available. Retrying in {delay} seconds...",exc_info=True)
            time.sleep(delay)
    logging.critical("Failed to connect to Kafka after several attempts.")
    return None



       
def send_to_kafka2(producer, topic, message, future_timeout=10):
    
    record_key = message['key'].encode('utf-8') if message['key'] is not None else None
    record_value = json.dumps( message['value']).encode('utf-8')
    record_timestamp = message['timestamp']
    
    future = producer.send(topic, value=record_value, key= record_key, timestamp_ms= record_timestamp)

    # Get record metadata of the message that was produced
    record_metadata = future.get(timeout=future_timeout)
    logging.debug(f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")


def on_send_success(record_metadata):
    logging.debug(f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")

def on_send_error(exception):
    logging.error(f"Error sending message: {exception}")
        
def send_to_kafka(producer, topic, message):
    record_key = message['key'].encode('utf-8') if message['key'] is not None else None
    record_value = json.dumps(message['value']).encode('utf-8')
    record_timestamp = message['timestamp']
    
    producer.send(topic, value=record_value, key=record_key, timestamp_ms=record_timestamp).add_callback(on_send_success).add_errback(on_send_error)