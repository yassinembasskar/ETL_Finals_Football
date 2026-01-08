import os
import csv
from typing import List, Dict

def read_csv_as_dict(file_path: str) -> List[Dict[str, str]]:
    """
    Read a CSV file and return its content as a list of dictionaries.
    Each row is represented as {column_name: value}.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    with open(file_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]

def write_csv_row(file_path: str, headers: List[str], row: List[str]):
    """
    Safely write a row to a CSV file. Creates the file and writes headers if it does not exist.
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_exists = os.path.isfile(file_path)

    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists or os.stat(file_path).st_size == 0:
            writer.writerow(headers)
        writer.writerow(row)
