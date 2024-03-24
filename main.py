import argparse
import time
import sys
import logging
from kafka.errors import KafkaError
from utils.log_utils import get_log_entries, is_newer_than_milliseconds
from utils.kafka_utils import try_create_producer, send_to_kafka
from utils.config_manager import load_config
from utils.offset_manager import OffsetManager
from utils.profiler import MyProfiler


def main(mode, skip_offset):
    # Your program logic here
    kafka_enabled = mode == "kafka"
    set_offset = not skip_offset

    # Load the configuration
    config = load_config()

    heartbeat_signal_lines = 10000
    log_dir = config["log_dir"]
    devices = config["devices"]
    iteration_delay = config["iteration_delay"]
    skip_older_entries_ms = config["skip_older_entries_ms"]

    db_file = "offsets_v1.db"
    offset_manager = OffsetManager(db_file)
    offset_save_interval = 1

    # Kafka producer configuration
    topic = config["topic_name"]
    max_retries = config["create_producer_max_retries"]
    retry_delay = config["create_producer_retry_delay"]
    batch_size = 1000

    if kafka_enabled:
        # Create producer
        producer_config = {
            "bootstrap_servers": config["bootstrap_servers"],
            "batch_size": batch_size,  # Set the maximum number of messages in a batch
            "linger_ms": 1000,  # Set the maximum time to wait before sending a batch,
            "compression_type": "gzip",  # Set the compression algorithm (e.g., gzip)
        }

        producer = try_create_producer(
            producer_config, max_retries=max_retries, delay=retry_delay
        )
        if producer is None:
            sys.exit(1)  # Exit the entire program with a status code

    mp = MyProfiler()
    mp.add_counters("Entries", "Discarded", "Sended2Kafka", "Sended Since Start")
    mp.add_time_metrics("Log Parser", "Last Batch")

    try:
        while True:

            offsets = offset_manager.load_offsets()
            mp.start_time_metric("Log Parser")
            entries = get_log_entries(log_dir, devices, offsets)
            total_entries = len(entries)
            mp.stop_time_metric("Log Parser")
            mp.report_rate("Log Parser", total_entries, "lines")

            if total_entries > 0:
                mp.reset_counters("Entries", "Discarded", "Sended2Kafka")
                mp.set_heartbeat_signal(
                    "Entries", "Last Batch", heartbeat_signal_lines, total_entries
                )
            else:
                logging.debug(
                    f"Nothing to send. Next try in {iteration_delay} seconds."
                )

            current_file = ""
            file_entry_counter = 0

            mp.delete_temporal_metrics()
            mp.start_time_metric("Last Batch")

            if set_offset and total_entries > 0:
                offset_manager.start_async_save(offset_save_interval)

            for entry in entries:
                file_entry_counter += 1
                mp.counter_increment("Entries")
                try:

                    if current_file == "":
                        current_file = f"File '{entry['filename']}'"
                        mp.start_time_metric(current_file)
                        logging.debug(f"Processing file {current_file}...")
                    elif current_file != f"File '{entry['filename']}'":
                        mp.stop_time_metric(current_file)
                        mp.report_rate(current_file, file_entry_counter, "lines")
                        current_file = f"File '{entry['filename']}'"
                        mp.start_time_metric(current_file)
                        file_entry_counter = 0
                        logging.debug(f"Processing file {current_file}...")

                    entry_ts = entry["message"]["value"]["timestamp"]
                    is_current = is_newer_than_milliseconds(
                        entry_ts, skip_older_entries_ms
                    )

                    if not is_current:
                        mp.counter_increment("Discarded")

                    if kafka_enabled and is_current:
                        send_to_kafka(producer, topic, message=entry["message"])
                        mp.counter_increment("Sended2Kafka")
                        mp.counter_increment("Sended Since Start")

                    if set_offset:
                        offset_manager.update_offset(
                            entry["filename"],
                            entry["offset"] + len(entry["line"]) + 1,
                            entry_ts,
                        )

                except KafkaError as ker:
                    logging.error(f"KafkaError: {ker}", exc_info=True)

                except Exception as e:
                    logging.error(f"An error occurred: {e}", exc_info=True)
                    logging.error(f"Next try in {iteration_delay} seconds.")
                    break

            if set_offset and total_entries > 0:
                offset_manager.stop_async_save()
                offset_manager.save_offsets()  # flush ofssets

            if kafka_enabled:
                producer.flush()

            if total_entries > 0:
                mp.stop_time_metric(current_file)
                mp.stop_time_metric("Last Batch")
                mp.report_rate(current_file, file_entry_counter, "lines")
                mp.report_rate("Last Batch", mp.get_counter_value("Entries"), "lines")
                mp.report_counters()

            # Delay before the next iteration
            time.sleep(iteration_delay)

    except KeyboardInterrupt:
        logging.info("Interrupted by user. Stopping producer...")
    except Exception as e:
        logging.critical(f"An error occurred: {e}", exc_info=True)

    finally:
        offset_manager.close()
        if kafka_enabled and producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    # Create argument parser
    parser = argparse.ArgumentParser(description="Kafka producer for Govee log files.")

    # optional arguments
    parser.add_argument(
        "-s",
        "--skipoffset",
        action="store_true",
        help="Skip tracking the offset of read files.",
    )

    # positional arguments
    parser.add_argument(
        "mode",
        type=str,
        choices=["kafka", "pass"],
        help="'kafka' will enable the producer, 'pass' will only go through without sending.",
    )

    # Parse
    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(args.mode, args.skipoffset)
