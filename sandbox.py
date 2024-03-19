import re

log_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\s\d+\.\d+\s\d+\.\d+\s\d+$')

def validate_log_line(line):
    return bool(log_pattern.match(line))

log_lines = [
    "2024-03-12 01:06:23\t23.6\t41\t50",
    "2024-03-12 01:06:27\t23.5\t40.9\t50",
    "2024-03-12 01:06:33\t23.5\t40.9\t50",
    "2024-03-12 01:07:00\t23.5\t41.1\t50",
    "2024-03-12 01:07:02\t23.6\t41\t50",
    "2024-03-12 01:07:04\t23.6\t41.1\t50",
    "2024-03-12 01:07:14\t23.5\t41.1\t50"
]

# Validate each log line
for line in log_lines:
    if re.match(log_pattern, line):
        print(f"Valid line: {line}")
    else:
        print(f"Invalid line: {line}")