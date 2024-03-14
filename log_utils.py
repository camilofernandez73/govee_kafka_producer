from datetime import datetime
import os

def load_offsets(offset_file_path):
    offsets = {}
    try:
        with open(offset_file_path, 'r') as file:
            for line in file:
                parts = line.strip().split('\t')
                if len(parts) == 3:
                    offsets[parts[0]] = {'offset': int(parts[1]), 'lastwritten': parts[2]}
    except FileNotFoundError:
        pass
    return offsets


def save_offsets(offset_file_path, offsets):
    with open(offset_file_path, 'w') as file:
        for filename, info in offsets.items():
            file.write(f"{filename}\t{info['offset']}\t{info['lastwritten']}\n")

def get_matching_device_id(filename, substrings):
    for substring in substrings:
        if substring in filename:
            return substring
    return None


def read_log_entries(log_dir, offsets, device_list):
    entries_to_process = []

    for filename in os.listdir(log_dir):
        device_id = get_matching_device_id(filename, device_list)
        if device_id is None:
            continue

        filepath = os.path.join(log_dir, filename)
        offset_info = offsets.get(filename, {'offset': 0, 'lastwritten': '1970-01-01 00:00:00'})
        file_last_modified_time = datetime.utcfromtimestamp(os.path.getmtime(filepath)).strftime('%Y-%m-%d %H:%M:%S')

        #if file_last_modified_time <= offset_info['lastwritten']:
        #   continue

        with open(filepath, 'r') as file:
            file.seek(offset_info['offset'])
            while True:
                current_position = file.tell()
                line = file.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue

                record =  convert_to_record(device_id, line)
                entries_to_process.append({
                    'device_id':device_id,
                    'filename': filename,
                    'line': line,
                    'offset': current_position,
                    'lastwritten': file_last_modified_time,
                    'record': record
                })

    return entries_to_process



def to_unix_timestamp(datetime_string):
    dt = datetime.strptime(datetime_string, '%Y-%m-%d %H:%M:%S')
    unix_timestamp_ms = int(dt.timestamp() * 1000)
    return unix_timestamp_ms

def convert_to_record(device_id, line):
    timestamp, temperature, humidity, battery = line.split('\t')
    record = {
        'key' : device_id,
        'timestamp' : to_unix_timestamp(timestamp),
        'value': {                
            'timestamp': timestamp,
            'temperature': float(temperature),
            'humidity': float(humidity),
            'battery': int(battery)
            },

    }
 
    return record