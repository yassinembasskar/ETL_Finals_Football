"""
extract/test_scrape.py

Profiling / validation script for the scrape pipeline -- NOT a unit test
suite (no pass/fail assertions). Runs get_todays_matches + get_data_from_match
for a fixed list of dates, restricted to a single tournament (FIFA World Cup,
id=16 by default), and reports:

    - per-date: how many matches were found, how many succeeded/failed
      extraction
    - per-endpoint (MATCH_ENDPOINTS): how often each endpoint was missing
      across every match actually scraped, computed directly from each
      match's returned data

Dates are processed in batches of BATCH_SIZE, with a pause of
BATCH_PAUSE_SECONDS between batches. Each batch saves its own intermediate
report (scrape_report_by_date_batch<N>.csv / scrape_report_by_endpoint_batch<N>.csv)
so partial progress survives even if a later batch fails/crashes. A final
combined report across all batches is saved at the end
(scrape_report_by_date.csv / scrape_report_by_endpoint.csv).

Usage:
    python -m extract.test_scrape
    python -m extract.test_scrape --tournament-id 16 --tournament-name "FIFA World Cup" --report-dir reports
"""

import argparse
import asyncio
import os

import pandas as pd

from extract.scrape import get_todays_matches, MATCH_ENDPOINTS, get_data_from_match
from utils.pipeline_state import load_state
from utils.logging_setup import setup_logger

logger = setup_logger("test_scrape", "logs/test_scrape.log")

DATES = [
    '2022-11-20', '2022-11-21', '2022-11-22', '2022-11-23', '2022-11-24',
    '2022-11-25', '2022-11-26', '2022-11-27', '2022-11-28', '2022-11-29',
    '2022-11-30', '2022-12-01', '2022-12-02', '2022-12-03', '2022-12-04',
    '2022-12-05', '2022-12-06', '2022-12-09', '2022-12-10', '2022-12-13',
    '2022-12-14', '2022-12-17', '2022-12-18', '2026-06-11', '2026-06-12',
    '2026-06-13', '2026-06-14', '2026-06-15', '2026-06-16'
]

BATCH_SIZE = 5
BATCH_PAUSE_SECONDS = 60


def endpoint_key(endpoint: str) -> str:
    """Same key transform used in get_data_from_match (endpoint -> match_data key)."""
    return endpoint.replace("/", "_").replace("-", "_")


def chunk_dates(dates, batch_size):
    """Splits dates into a list of lists, each up to batch_size long."""
    return [dates[i:i + batch_size] for i in range(0, len(dates), batch_size)]


async def process_date(date_str, tournaments):
    """
    Processes a single date: gets the match list, scrapes each match's
    detail data, and returns (by_date_row, endpoint_presence_rows) for
    that date alone.
    """
    logger.info(f"process_date: processing date_str={date_str}")
    try:
        matches_df = await get_todays_matches(target_date=date_str, tournaments=tournaments)
    except Exception as e:
        logger.error(f"process_date: get_todays_matches failed for date_str={date_str} | {type(e).__name__}: {e}")
        by_date_row = {
            'date': date_str, 'matches_found': 0, 'matches_scraped_success': 0,
            'matches_scraped_failed': 0, 'extract_failure_rate': None,
            'error': f"get_todays_matches failed: {type(e).__name__}: {e}",
        }
        return by_date_row, []

    n_found = len(matches_df)
    n_success = 0
    n_failed = 0
    endpoint_presence_rows = []

    for _, row in matches_df.iterrows():
        event_id = row['event_id']
        try:
            match_data = await get_data_from_match(event_id, row['slug'], row['custom_id'])
            n_success += 1
        except Exception as e:
            n_failed += 1
            logger.error(f"process_date: get_data_from_match failed for event_id={event_id} | {type(e).__name__}: {e}")
            continue

        for endpoint in MATCH_ENDPOINTS:
            present = endpoint_key(endpoint) in match_data
            endpoint_presence_rows.append({
                'date': date_str,
                'event_id': event_id,
                'endpoint': endpoint,
                'present': present,
            })

    extract_failure_rate = round(n_failed / n_found, 4) if n_found else None

    by_date_row = {
        'date': date_str,
        'matches_found': n_found,
        'matches_scraped_success': n_success,
        'matches_scraped_failed': n_failed,
        'extract_failure_rate': extract_failure_rate,
        'error': None,
    }
    return by_date_row, endpoint_presence_rows


def build_endpoint_summary(endpoint_presence_rows):
    """Aggregates a list of {date, event_id, endpoint, present} rows into
    a per-endpoint missing-rate summary DataFrame."""
    if not endpoint_presence_rows:
        return pd.DataFrame(columns=['endpoint', 'n_checked', 'n_present', 'n_missing', 'missing_rate'])

    presence_df = pd.DataFrame(endpoint_presence_rows)
    summary = (
        presence_df.groupby('endpoint')['present']
        .agg(n_checked='count', n_present='sum')
        .reset_index()
    )
    summary['n_missing'] = summary['n_checked'] - summary['n_present']
    summary['missing_rate'] = round(summary['n_missing'] / summary['n_checked'], 4)
    return summary[['endpoint', 'n_checked', 'n_present', 'n_missing', 'missing_rate']]


def save_reports(by_date_df, by_endpoint_df, report_dir, suffix=''):
    """Saves the two report DataFrames to report_dir, with an optional filename suffix."""
    os.makedirs(report_dir, exist_ok=True)
    by_date_path = os.path.join(report_dir, f'scrape_report_by_date{suffix}.csv')
    by_endpoint_path = os.path.join(report_dir, f'scrape_report_by_endpoint{suffix}.csv')
    by_date_df.to_csv(by_date_path, index=False)
    by_endpoint_df.to_csv(by_endpoint_path, index=False)
    logger.info(f"save_reports: saved {by_date_path} and {by_endpoint_path}")
    return by_date_path, by_endpoint_path


async def run_scrape_test(dates=None, tournaments=None, report_dir='reports',
                           batch_size=BATCH_SIZE, batch_pause_seconds=BATCH_PAUSE_SECONDS):
    """
    Processes `dates` in batches of `batch_size`, pausing
    `batch_pause_seconds` between batches. Saves an intermediate report
    after each batch, then a final combined report across all batches.

    Returns (by_date_df, by_endpoint_df) for the FULL combined run.
    """
    dates = dates if dates is not None else DATES
    tournaments = tournaments if tournaments is not None else {16: "FIFA World Cup"}

    batches = chunk_dates(dates, batch_size)
    logger.info(f"run_scrape_test: {len(dates)} dates split into {len(batches)} batches of up to {batch_size}")

    all_by_date_rows = []
    all_endpoint_presence_rows = []

    for batch_num, batch_dates in enumerate(batches, start=1):
        logger.info(f"run_scrape_test: starting batch {batch_num}/{len(batches)} -- dates: {batch_dates}")

        batch_by_date_rows = []
        batch_endpoint_presence_rows = []

        for date_str in batch_dates:
            by_date_row, endpoint_presence_rows = await process_date(date_str, tournaments)
            batch_by_date_rows.append(by_date_row)
            batch_endpoint_presence_rows.extend(endpoint_presence_rows)

        all_by_date_rows.extend(batch_by_date_rows)
        all_endpoint_presence_rows.extend(batch_endpoint_presence_rows)

        batch_by_date_df = pd.DataFrame(batch_by_date_rows)
        batch_by_endpoint_df = build_endpoint_summary(batch_endpoint_presence_rows)
        save_reports(batch_by_date_df, batch_by_endpoint_df, report_dir, suffix=f'_batch{batch_num}')

        logger.info(f"run_scrape_test: finished batch {batch_num}/{len(batches)}")

        is_last_batch = (batch_num == len(batches))
        if not is_last_batch:
            logger.info(f"run_scrape_test: pausing {batch_pause_seconds}s before next batch")
            await asyncio.sleep(batch_pause_seconds)

    by_date_df = pd.DataFrame(all_by_date_rows)
    by_endpoint_df = build_endpoint_summary(all_endpoint_presence_rows)

    # Cross-check against pipeline_state.csv's own state_extract column,
    # in case scrape_all_matches was also run separately and updated it.
    try:
        state_df = load_state()
        if not state_df.empty and 'state_extract' in state_df.columns:
            state_failure_rate = (state_df['state_extract'] == 'failed').mean()
            logger.info(f"run_scrape_test: pipeline_state.csv overall state_extract failure rate: {round(state_failure_rate, 4)}")
    except Exception as e:
        logger.warning(f"run_scrape_test: could not read pipeline_state.csv for cross-check | {type(e).__name__}: {e}")

    print("\n=== Scrape test results by date (all batches combined) ===")
    with pd.option_context('display.max_rows', None, 'display.width', 140):
        print(by_date_df.to_string(index=False))

    print("\n=== MATCH_ENDPOINTS missing-rate (all batches combined) ===")
    with pd.option_context('display.max_rows', None, 'display.width', 100):
        print(by_endpoint_df.to_string(index=False))

    save_reports(by_date_df, by_endpoint_df, report_dir, suffix='')

    return by_date_df, by_endpoint_df


def main():
    parser = argparse.ArgumentParser(description="Profile the scrape pipeline's extraction success rate and endpoint coverage.")
    parser.add_argument('--tournament-id', type=int, default=16)
    parser.add_argument('--tournament-name', default="FIFA World Cup")
    parser.add_argument('--report-dir', default='reports')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE)
    parser.add_argument('--batch-pause-seconds', type=int, default=BATCH_PAUSE_SECONDS)
    args = parser.parse_args()

    tournaments = {args.tournament_id: args.tournament_name}
    asyncio.run(run_scrape_test(
        dates=DATES,
        tournaments=tournaments,
        report_dir=args.report_dir,
        batch_size=args.batch_size,
        batch_pause_seconds=args.batch_pause_seconds,
    ))


if __name__ == "__main__":
    main()