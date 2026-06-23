"""
transform/run_world_cup_transform.py

Production driver: runs transform.transform_csv(date_str) for a fixed list
of dates (the same World Cup backfill dates used by
extract/run_world_cup_backfill.py).

No batching/pauses -- transform_csv is purely local (reads raw CSVs, writes
parquet files), so there's no need to be gentle the way the network-bound
scrape backfill is. Runs straight through all dates.

If a date's raw CSV doesn't exist yet (e.g. that date's scrape hasn't run
or failed entirely), that date is skipped gracefully: logged, and the loop
continues to the next date.

Usage:
    python -m transform.run_world_cup_transform
"""

import os

from transform.transform import transform_csv
from utils.logging_setup import setup_logger

logger = setup_logger("run_world_cup_transform", "logs/run_world_cup_transform.log")

DATES = [
    '2022-11-20', '2022-11-21', '2022-11-22', '2022-11-23', '2022-11-24',
    '2022-11-25', '2022-11-26', '2022-11-27', '2022-11-28', '2022-11-29',
    '2022-11-30', '2022-12-01', '2022-12-02', '2022-12-03', '2022-12-04',
    '2022-12-05', '2022-12-06', '2022-12-09', '2022-12-10', '2022-12-13',
    '2022-12-14', '2022-12-17', '2022-12-18', '2026-06-11', '2026-06-12',
    '2026-06-13', '2026-06-14', '2026-06-15', '2026-06-16'
]


def run_transform_backfill(dates=None, csv_dir='raw', output_dir='processed'):
    """
    Runs transform_csv(date_str, csv_dir, output_dir) for every date in
    `dates`, straight through. A date whose raw CSV doesn't exist is
    skipped (logged, not a hard failure). A date that fails for any other
    reason is also logged and does not stop the remaining dates.
    """
    dates = dates if dates is not None else DATES

    logger.info(f"run_transform_backfill: starting for {len(dates)} dates")

    results = []

    for date_str in dates:
        csv_path = f"{csv_dir}/{date_str}_match_data.csv"

        if not os.path.exists(csv_path):
            logger.warning(f"run_transform_backfill: skipping date_str={date_str} -- raw CSV not found at {csv_path}")
            results.append({'date': date_str, 'status': 'skipped', 'error': f"raw CSV not found: {csv_path}"})
            continue

        logger.info(f"run_transform_backfill: running transform_csv for date_str={date_str}")
        try:
            transform_csv(date_str, csv_dir=csv_dir, output_dir=output_dir)
            results.append({'date': date_str, 'status': 'success', 'error': None})
            logger.info(f"run_transform_backfill: succeeded for date_str={date_str}")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}"
            results.append({'date': date_str, 'status': 'failed', 'error': error_message})
            logger.error(f"run_transform_backfill: failed for date_str={date_str} | {error_message}")

    n_success = sum(1 for r in results if r['status'] == 'success')
    n_failed = sum(1 for r in results if r['status'] == 'failed')
    n_skipped = sum(1 for r in results if r['status'] == 'skipped')
    logger.info(f"run_transform_backfill: finished -- {n_success} succeeded, {n_failed} failed, {n_skipped} skipped")

    print(f"\n=== Transform backfill finished: {n_success} succeeded, {n_failed} failed, {n_skipped} skipped ===")
    for r in results:
        marker = {'success': 'OK', 'failed': 'FAILED', 'skipped': 'SKIPPED'}[r['status']]
        print(f"  [{marker}] {r['date']}" + (f" -- {r['error']}" if r['error'] else ""))

    return results


def main():
    run_transform_backfill(dates=DATES)


if __name__ == "__main__":
    main()