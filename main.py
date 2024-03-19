import argparse
import os
import time
import sys
from kafka.errors import KafkaError
from utils.log_utils import  load_offsets, get_log_entries, save_offsets
from utils.kafka_utils import try_create_producer, send_to_kafka
from utils.config_manager import load_config 
import logging


# Delivery callback function
def handle_delivery_report(err, msg):
    if err is not None:
        logging.error(f"Failed to deliver message: {err}")
    else:
        logging.info(f"Message delivered to topic {msg.topic()} [partition {msg.partition()}]")
        
        

def main(mode, skip_offset):
    # Your program logic here
    kafka_enabled = mode == "kafka"
    set_offset = not skip_offset
 
    script_dir = os.path.dirname(os.path.abspath(__file__))
        
    # Load the configuration
    config = load_config()

    log_dir = config['log_dir']
    devices = config['devices']
    iteration_delay = config['iteration_delay']

    offset_file = os.path.join(script_dir, 'file_offsets.txt')
   

    # Kafka producer configuration
    topic = config['topic_name']
    max_retries = config['create_producer_max_retries']
    retry_delay = config['create_producer_retry_delay']
    batch_size = 100

    if kafka_enabled:
        # Create producer
        producer_config = {
            'bootstrap_servers':  config['bootstrap_servers'],
            'batch_size': batch_size,  # Set the maximum number of messages in a batch
            'linger_ms': 1000,  # Set the maximum time to wait before sending a batch,
            'compression_type': 'gzip'  # Set the compression algorithm (e.g., gzip)
        }
        
        producer = try_create_producer(producer_config, max_retries = max_retries, delay= retry_delay)
        if producer is None:
            sys.exit(1)  # Exit the entire program with a status code
    

    try:
        while True:
        
            offsets = load_offsets(offset_file)
            entries = get_log_entries(log_dir, devices, offsets)
            total_entries = len(entries)
          
            if total_entries == 0:
                logging.info(f"Nothing to send. Next try in {iteration_delay} seconds.")

           
            # Process entries
            entry_index = 0
            for entry in entries:
                entry_index += 1  
                try:
                    
                    line = entry["line"]
                    logging.debug(f"Sending... ({entry_index}/{total_entries}) : {entry['message']}")
          
                    if kafka_enabled:
                        send_to_kafka(producer, topic, message = entry['message'], future_timeout = config['future_timeout'])

                    if set_offset:
                        offsets[entry['filename']] = {
                            'offset': entry['offset'] + len(entry['line']) + 1,  # Assuming +1 for newline character
                            }
                        save_offsets(offset_file, offsets)
                
                except KafkaError as e:
                    logging.error(f"KafkaError: {e}")
                
                except Exception as e:
                    logging.error(f"An error occurred: {e}")
                    logging.error(f"Next try in {iteration_delay} seconds.")
                    break
            
            if kafka_enabled:
                producer.flush()
                
            # Delay before the next iteration
            time.sleep(iteration_delay)

    except KeyboardInterrupt:
         logging.info("Interrupted by user. Stopping producer...")

    finally:
        if kafka_enabled and producer is not None:
            producer.flush()
            producer.close()
   

if __name__ == '__main__':
    # Create argument parser
    parser = argparse.ArgumentParser(description="Kafka producer for Govee log files.")

    # optional arguments
    parser.add_argument("-so", "--skipoffset", action="store_true", help="Skip tracking the offset of read files.")
       
    # positional arguments
    parser.add_argument("mode", type=str, choices=["kafka", "pass"], help="'kafka' will enable the producer, 'pass' will only go through without sending.")

    # Parse
    args = parser.parse_args()
    
    # Call the main function with parsed arguments
    main(args.mode, args.skipoffset)
