import os
import time
import sys
from log_utils import read_log_entries, load_offsets, save_offsets
from kafka_utils import create_producer, send_to_kafka


def main():
    
    my_app_env = os.getenv('GOVEE_KAFKA_PRODUCER_ENV', 'development')  # Default to 'development' if not set
    print(f"Enviroment set as '{my_app_env}'.")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if my_app_env == 'production':
        log_dir = "/var/log/goveebttemplogger"
    else:
        log_dir = os.path.join(script_dir, 'govee_files')
    

    offset_file = os.path.join(script_dir, 'file_offsets.txt')
    offsets = load_offsets(offset_file)
    
    device_list = [
    "A4C138C6CA69",
    "A4C1380B36C5",
    "A4C1388BED5C",
    "A4C13890C250",
    "A4C1385448FD"
    ]

    # Kafka producer configuration
    bootstrap_servers = ['192.168.177.12:9093']
    topic = 'govee-temp'

    iteration_delay = 60

    producer = create_producer(bootstrap_servers, max_retries = 9999, delay=10)
    if producer is None:
        sys.exit(1)  # Exit the entire program with a status code
    
    try:
        while True:
        
            entries = read_log_entries(log_dir, offsets, device_list)
            bootstrap_servers = ['192.168.177.12:9093']
            topic = 'govee-temp'
            total_entries = len(entries)
            entry_index = 0
            
            if total_entries == 0:
                print(f"Nothing to send. Next try in {iteration_delay} seconds.")
                
            # Process entries
            for entry in entries:
                entry_index += 1  
                try:
                    print(f"Sending... ({entry_index}/{total_entries})")
                    send_to_kafka(producer, topic, message = entry['record'], future_timeout=10)
                    offsets[entry['filename']] = {
                        'offset': entry['offset'] + len(entry['line']) + 1,  # Assuming +1 for newline character
                        'lastwritten': entry['lastwritten']
                        }
                    save_offsets(offset_file, offsets)
                except Exception as e:
                    print(f"An error occurred: {e}")
                    print(f"Next try in {iteration_delay} seconds.")
                    break

   
            # Delay before the next iteration
            time.sleep(iteration_delay)

    except KeyboardInterrupt:
        print("Interrupted by user. Stopping producer...")

    finally:
        if producer is not None:
            producer.close()

if __name__ == '__main__':
    main()
