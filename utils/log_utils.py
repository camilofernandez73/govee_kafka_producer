from datetime import datetime, timedelta, timezone
import os
import re
import logging


def get_log_entries(log_dir, devices, offsets):
    entries = []

    for device in devices:
        log_filenames = get_device_log_files(log_dir, device)
        sorted_files = sort_files_by_last_written(log_dir, log_filenames)

        for file_name in sorted_files:
            file_path = os.path.join(log_dir, file_name)
            file_size = os.path.getsize(file_path)
            offset_info = offsets.get(
                file_name, {"offset": 0, "lastwritten": "1970-01-01 00:00:00"}
            )

            with open(file_path, "r") as file:

                file.seek(offset_info["offset"])
                while True:
                    current_position = file.tell()
                    line = file.readline()
                    if not line:
                        break
                    if not (line := line.strip()) or not complies_format(line):
                        logging.error(
                            f"Invalid line in file {file_name} at position {current_position}/{file_size}."
                        )
                        logging.error(f"Line: '{line}'")
                        continue

                    # logging.debug(f"Read line of {file_name} at position {current_position}/{file_size}.")

                    entries.append(
                        {
                            "device_id": device,
                            "filename": file_name,
                            "line": line,
                            "offset": current_position,
                            "message": convert_to_message(device, line),
                        }
                    )

    return entries


def to_unix_timestamp(datetime_string):
    dt = datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")
    unix_timestamp_ms = int(dt.timestamp() * 1000)
    return unix_timestamp_ms


def convert_to_message(device_id, line):
    timestamp, temperature, humidity, battery = line.split("\t")
    message = {
        "key": device_id,
        "timestamp": to_unix_timestamp(timestamp),
        "value": {
            "timestamp": timestamp,
            # 'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "temperature": float(temperature),
            "humidity": float(humidity),
            "battery": int(battery),
        },
    }
    # time.sleep(5)
    return message


log_pattern = re.compile(
    r"^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\s\d+(?:\.\d+)?\s\d+(?:\.\d+)?\s\d+$"
)


def complies_format(line):
    return bool(log_pattern.match(line))


def get_device_log_files(folder_path, device):
    matching_files = []

    d_lower = device.lower()

    for filename in os.listdir(folder_path):
        f_lower = filename.lower()
        if d_lower in f_lower:
            matching_files.append(filename)

    return matching_files


def sort_files_by_last_written(folder_path, file_list, ascending=False):
    # Get the last written time for each file
    file_times = []
    for filename in file_list:
        # Construct the full file path
        file_path = os.path.join(folder_path, filename)
        # Get the last modified time of the file
        last_written = os.path.getmtime(file_path)
        # Append the tuple (filename, last_written) to file_times
        file_times.append((filename, last_written))

    # Sort the list of file names based on last written time
    sorted_files = sorted(file_times, key=lambda x: x[1], reverse=not ascending)

    # Extract and return the sorted list of filenames
    sorted_filenames = [filename for filename, _ in sorted_files]

    return sorted_filenames


def is_newer_than_milliseconds(date_string, milliseconds):
    # Parse the date string into a datetime object
    date = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")

    # Make the date timezone-aware by replacing the timezone with UTC
    date = date.replace(tzinfo=timezone.utc)

    # Get the current UTC time as a timezone-aware datetime object
    current_time = datetime.now(timezone.utc)

    # Calculate the time difference between the given date and the current time
    time_diff = current_time - date

    # Check if the time difference is less than the specified number of milliseconds
    return time_diff < timedelta(milliseconds=milliseconds)
