import argparse
import os
import traceback

import pandas as pd

from transform.transform import (
    PlayerRegistry,
    transform_row,
)


def profile_table(name, df):
    """
    Builds a one-row-per-column summary DataFrame for `df`:
    column, dtype, n_missing, isna_rate.
    Returns the summary DataFrame (does not print).
    """
    if df.empty:
        return pd.DataFrame(columns=['column', 'dtype', 'n_missing', 'isna_rate'])

    summary = pd.DataFrame({
        'column': df.columns,
        'dtype': [str(df[c].dtype) for c in df.columns],
        'n_missing': [int(df[c].isna().sum()) for c in df.columns],
        'isna_rate': [round(float(df[c].isna().mean()), 4) for c in df.columns],
    })
    return summary


def print_table_profile(name, df, summary):
    """Prints a profile block for one table to the console."""
    print(f"\n=== {name} ({len(df)} rows, {len(df.columns) if not df.empty else 0} columns) ===")
    if df.empty:
        print("  (empty -- no rows produced)")
        return
    with pd.option_context('display.max_rows', None, 'display.width', 120):
        print(summary.to_string(index=False))

    if name == 'passing_network' and 'action_coordinates' in df.columns and 'type' in df.columns:
        missing_rate_by_type = df['action_coordinates'].isna().groupby(df['type']).mean()
        print("\n  missing action_coordinates rate by type:")
        print(missing_rate_by_type.to_string())


def transform_csv_with_profiling(date_str, csv_dir='raw', output_dir='processed', report_dir='reports'):
    """
    Same overall flow as transform.transform_csv, but:
      - collects per-row exceptions with full context (not just a print)
      - profiles every output table (dtype + isna rate per column)
      - saves both the per-table profile and the error log to report_dir,
        in addition to printing them to the console

    Returns (final_tables, errors_df, profiles) where:
      final_tables: dict of table_name -> DataFrame (same as transform_csv)
      errors_df: DataFrame of every row-level exception encountered
      profiles: dict of table_name -> profile summary DataFrame
    """
    csv_path = f"{csv_dir}/{date_str}_match_data.csv"
    df = pd.read_csv(csv_path)

    registry = PlayerRegistry()

    accumulated = {
        'match': [], 'team': [], 'match_team': [], 'match_team_stats': [],
        'match_players': [], 'match_player_stats': [],
        'goals': [], 'cards': [], 'substitutions': [], 'passing_network': [], 'highlights': [],
        'shotmaps': [],
    }

    errors = []

    for i in range(len(df)):
        row = df.iloc[[i]]
        event_id = row['event_id'].iloc[0] if 'event_id' in row.columns else None
        try:
            tables = transform_row(row, registry)
        except Exception as e:
            errors.append({
                'row_index': i,
                'event_id': event_id,
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': traceback.format_exc(),
            })
            print(f"[ERROR] row_index={i} event_id={event_id} | {type(e).__name__}: {e}")
            continue

        for table_name, table_df in tables.items():
            accumulated[table_name].append(table_df)

    final_tables = {}
    for table_name, frames in accumulated.items():
        if frames:
            combined = pd.concat(frames, ignore_index=True)
        else:
            combined = pd.DataFrame()
        if table_name == 'team':
            combined = combined.drop_duplicates(subset='team_id').reset_index(drop=True)
        final_tables[table_name] = combined

    final_tables['players'] = registry.to_dataframe()

    # --- Profiling: build + print + collect for saving ---
    profiles = {}
    for table_name, table_df in final_tables.items():
        summary = profile_table(table_name, table_df)
        profiles[table_name] = summary
        print_table_profile(table_name, table_df, summary)

    # --- Error report ---
    errors_df = pd.DataFrame(errors, columns=['row_index', 'event_id', 'error_type', 'error_message', 'traceback'])
    print(f"\n=== Errors ({len(errors_df)} / {len(df)} rows failed) ===")
    if not errors_df.empty:
        with pd.option_context('display.max_colwidth', 80):
            print(errors_df[['row_index', 'event_id', 'error_type', 'error_message']].to_string(index=False))
    else:
        print("  (no errors)")

    # --- Save reports to disk ---
    date_report_dir = f"{report_dir}/{date_str}"
    os.makedirs(date_report_dir, exist_ok=True)

    for table_name, summary in profiles.items():
        out_path = f"{date_report_dir}/profile_{table_name}.csv"
        summary.to_csv(out_path, index=False)

    errors_out_path = f"{date_report_dir}/errors.csv"
    errors_df.to_csv(errors_out_path, index=False)

    print(f"\nSaved {len(profiles)} table profiles and error log to {date_report_dir}/")

    return final_tables, errors_df, profiles


def main():
    parser = argparse.ArgumentParser(description="Profile the transform pipeline's output tables and capture errors.")
    parser.add_argument('date_str', help="Date string for the raw CSV, e.g. 2026-06-17")
    parser.add_argument('--csv-dir', default='raw')
    parser.add_argument('--output-dir', default='processed')
    parser.add_argument('--report-dir', default='reports')
    args = parser.parse_args()

    transform_csv_with_profiling(
        args.date_str,
        csv_dir=args.csv_dir,
        output_dir=args.output_dir,
        report_dir=args.report_dir,
    )


if __name__ == '__main__':
    main()