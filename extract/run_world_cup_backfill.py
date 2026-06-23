"""
extract/run_world_cup_backfill.py

Production driver: runs scrape.py's main(date_str, tournaments) for a fixed
list of dates, restricted to FIFA World Cup (tournament_id=16), in batches
with a pause between batches to avoid hammering the site.

Unlike test_scrape.py (which calls get_data_from_match directly for
diagnostics, with no CSV/state side effects), this calls the real
main() for each date -- so it writes the actual raw/<date>_sofascore.csv
and raw/<date>_match_data.csv files, and updates pipeline_state.csv's
state_extract column for real, exactly as production usage intends.

Usage:
    python -m extract.run_world_cup_backfill
    python -m extract.run_world_cup_backfill --batch-size 5 --batch-pause-seconds 60
"""

import argparse
import asyncio

from extract.scrape import main as scrape_main
from utils.logging_setup import setup_logger

logger = setup_logger("run_world_cup_backfill", "logs/run_world_cup_backfill.log")

DATES = [
    '2022-11-30', '2022-12-01', '2022-12-02', '2022-12-03', '2022-12-04',
    '2022-12-05', '2022-12-06', '2022-12-09', '2022-12-10', '2022-12-13',
    '2022-12-14', '2022-12-17', '2022-12-18', '2026-06-11', '2026-06-12',
    '2026-06-13', '2026-06-14', '2026-06-15', '2026-06-16'
]

WORLD_CUP = {16: "FIFA World Cup"}

BATCH_SIZE = 5
BATCH_PAUSE_SECONDS = 60


def chunk_dates(dates, batch_size):
    """Splits dates into a list of lists, each up to batch_size long."""
    return [dates[i:i + batch_size] for i in range(0, len(dates), batch_size)]


async def run_backfill(dates=None, tournaments=None, batch_size=BATCH_SIZE,
                        batch_pause_seconds=BATCH_PAUSE_SECONDS):
    """
    Runs scrape.main(date_str, tournaments) for every date in `dates`, in
    batches of `batch_size`, pausing `batch_pause_seconds` between batches.

    Each date's failures/successes are logged but do NOT stop the run --
    a failure on one date is independent of every other date, same
    philosophy as the per-row handling inside transform.py/scrape.py.
    """
    dates = dates if dates is not None else DATES
    tournaments = tournaments if tournaments is not None else WORLD_CUP

    batches = chunk_dates(dates, batch_size)
    logger.info(f"run_backfill: {len(dates)} dates split into {len(batches)} batches of up to {batch_size}")
    logger.info(f"run_backfill: tournaments={tournaments}")

    results = []

    for batch_num, batch_dates in enumerate(batches, start=1):
        logger.info(f"run_backfill: starting batch {batch_num}/{len(batches)} -- dates: {batch_dates}")

        for date_str in batch_dates:
            logger.info(f"run_backfill: running main() for date_str={date_str}")
            try:
                await scrape_main(date_str, tournaments=tournaments)
                results.append({'date': date_str, 'status': 'success', 'error': None})
                logger.info(f"run_backfill: succeeded for date_str={date_str}")
            except Exception as e:
                error_message = f"{type(e).__name__}: {e}"
                results.append({'date': date_str, 'status': 'failed', 'error': error_message})
                logger.error(f"run_backfill: failed for date_str={date_str} | {error_message}")

        logger.info(f"run_backfill: finished batch {batch_num}/{len(batches)}")

        is_last_batch = (batch_num == len(batches))
        if not is_last_batch:
            logger.info(f"run_backfill: pausing {batch_pause_seconds}s before next batch")
            await asyncio.sleep(batch_pause_seconds)

    n_success = sum(1 for r in results if r['status'] == 'success')
    n_failed = sum(1 for r in results if r['status'] == 'failed')
    logger.info(f"run_backfill: finished all batches -- {n_success} succeeded, {n_failed} failed")

    print(f"\n=== Backfill finished: {n_success} succeeded, {n_failed} failed ===")
    for r in results:
        marker = "OK" if r['status'] == 'success' else "FAILED"
        print(f"  [{marker}] {r['date']}" + (f" -- {r['error']}" if r['error'] else ""))

    return results


def main():
    parser = argparse.ArgumentParser(description="Backfill scrape data for a fixed list of World Cup dates.")
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE)
    parser.add_argument('--batch-pause-seconds', type=int, default=BATCH_PAUSE_SECONDS)
    args = parser.parse_args()

    asyncio.run(run_backfill(
        dates=DATES,
        tournaments=WORLD_CUP,
        batch_size=args.batch_size,
        batch_pause_seconds=args.batch_pause_seconds,
    ))


if __name__ == "__main__":
    main()