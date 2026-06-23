"""
For each day in [2022-11-20, 2022-11-29]:
  - open raw/{date}_sofascore.csv
  - for every row, parse the date part of the "kickoff" column (format: "YYYY-MM-DD HH:MM")
  - if that date != the date in the filename, the row is removed
  - collect the event_id of every removed row
  - overwrite raw/{date}_sofascore.csv without those rows
  - open raw/{date}_match_data.csv and remove any row whose event_id is in the removed set
  - overwrite raw/{date}_match_data.csv

Assumptions (change RAW_DIR / column names below if they differ):
  - Files live in a folder named "raw" (RAW_DIR), relative to where this script is run,
    or pass an absolute path via RAW_DIR.
  - Both files have a column literally named "event_id".
  - sofascore file has a column literally named "kickoff" with values like "2022-11-21 17:00".
  - CSVs are comma-separated, UTF-8.
"""

import csv
import os
from datetime import date, timedelta

RAW_DIR = "raw"  # change to an absolute path if needed, e.g. "/home/claude/raw"

START_DATE = date(2022, 11, 20)
END_DATE = date(2022, 11, 29)

KICKOFF_COL = "kickoff"
EVENT_ID_COL = "event_id"


def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def read_csv(path):
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames
    return fieldnames, rows


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def process_day(day):
    date_str = day.strftime("%Y-%m-%d")
    sofascore_path = os.path.join(RAW_DIR, f"{date_str}_sofascore.csv")
    match_data_path = os.path.join(RAW_DIR, f"{date_str}_match_data.csv")

    if not os.path.isfile(sofascore_path):
        print(f"[{date_str}] SKIP: missing {sofascore_path}")
        return

    fieldnames, rows = read_csv(sofascore_path)

    if KICKOFF_COL not in (fieldnames or []):
        print(f"[{date_str}] ERROR: '{KICKOFF_COL}' column not found in {sofascore_path}. "
              f"Found columns: {fieldnames}")
        return
    if EVENT_ID_COL not in (fieldnames or []):
        print(f"[{date_str}] ERROR: '{EVENT_ID_COL}' column not found in {sofascore_path}. "
              f"Found columns: {fieldnames}")
        return

    kept_rows = []
    removed_event_ids = set()

    for row in rows:
        kickoff_val = (row.get(KICKOFF_COL) or "").strip()
        kickoff_date = kickoff_val.split(" ")[0] if kickoff_val else ""

        if kickoff_date != date_str:
            removed_event_ids.add(row.get(EVENT_ID_COL))
        else:
            kept_rows.append(row)

    write_csv(sofascore_path, fieldnames, kept_rows)
    print(f"[{date_str}] sofascore: removed {len(removed_event_ids)} row(s), "
          f"kept {len(kept_rows)} row(s)")

    if removed_event_ids and os.path.isfile(match_data_path):
        md_fieldnames, md_rows = read_csv(match_data_path)
        if EVENT_ID_COL not in (md_fieldnames or []):
            print(f"[{date_str}] ERROR: '{EVENT_ID_COL}' column not found in {match_data_path}. "
                  f"Found columns: {md_fieldnames}")
            return

        before_count = len(md_rows)
        md_kept_rows = [r for r in md_rows if r.get(EVENT_ID_COL) not in removed_event_ids]
        removed_count = before_count - len(md_kept_rows)

        write_csv(match_data_path, md_fieldnames, md_kept_rows)
        print(f"[{date_str}] match_data: removed {removed_count} row(s), "
              f"kept {len(md_kept_rows)} row(s)")
    elif removed_event_ids and not os.path.isfile(match_data_path):
        print(f"[{date_str}] WARNING: {len(removed_event_ids)} event_id(s) flagged for removal "
              f"but {match_data_path} not found")
    else:
        print(f"[{date_str}] match_data: nothing to remove")


def main():
    for day in daterange(START_DATE, END_DATE):
        process_day(day)


if __name__ == "__main__":
    main()