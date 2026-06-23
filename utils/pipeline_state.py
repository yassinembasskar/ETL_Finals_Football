"""
Pipeline state tracking.

Maintains a single persistent CSV (state/pipeline_state.csv) keyed by
event_id, with independent state columns per pipeline stage:
    event_id, state_extract, state_transform, state_transform_error,
    state_transform_timestamp, state_load

Only the transform-stage columns (state_transform, state_transform_error,
state_transform_timestamp) are ever written by this module's
update_transform_state function. state_extract and state_load are owned by
other pipeline stages and are read/preserved untouched if already present.
"""

import os
from datetime import datetime, timezone

import pandas as pd

STATE_COLUMNS = [
    'event_id',
    'state_extract',
    'state_extract_error',
    'state_extract_timestamp',
    'state_transform',
    'state_transform_error',
    'state_transform_timestamp',
    'state_load',
]

DEFAULT_STATE_PATH = 'state/pipeline_state.csv'


def load_state(state_path=DEFAULT_STATE_PATH):
    """
    Loads the persistent state file if it exists, otherwise returns an
    empty DataFrame with the correct columns. event_id is used as the
    DataFrame index for fast per-row lookup/update.
    """
    if os.path.exists(state_path):
        state_df = pd.read_csv(state_path)
        for col in STATE_COLUMNS:
            if col not in state_df.columns:
                state_df[col] = pd.NA
        state_df = state_df[STATE_COLUMNS]
    else:
        state_df = pd.DataFrame(columns=STATE_COLUMNS)

    state_df = state_df.set_index('event_id', drop=False)
    return state_df


def save_state(state_df, state_path=DEFAULT_STATE_PATH):
    """Writes the state DataFrame back to disk, creating the folder if needed."""
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    state_df.reset_index(drop=True)[STATE_COLUMNS].to_csv(state_path, index=False)


def update_transform_state(state_df, event_id, status, error_message=None):
    """
    Updates ONLY the transform-stage columns for one event_id, in place,
    on the given state_df. Leaves state_extract/state_load untouched if the
    row already exists; creates a new row (with state_extract/state_load
    left blank) if this event_id hasn't been seen before.

    status: 'success' or 'failed'
    error_message: error text to record when status == 'failed' (ignored
                    otherwise; the error column is cleared on success)
    """
    timestamp = datetime.now(timezone.utc).isoformat()

    if event_id not in state_df.index:
        new_row = {col: pd.NA for col in STATE_COLUMNS}
        new_row['event_id'] = event_id
        state_df.loc[event_id] = new_row

    state_df.loc[event_id, 'state_transform'] = status
    state_df.loc[event_id, 'state_transform_error'] = error_message if status == 'failed' else pd.NA
    state_df.loc[event_id, 'state_transform_timestamp'] = timestamp

    return state_df


def update_extract_state(state_df, event_id, status, error_message=None):
    """
    Updates ONLY the extract-stage columns for one event_id, in place, on
    the given state_df. Leaves state_transform/state_load untouched if the
    row already exists; creates a new row (with state_transform/state_load
    left blank) if this event_id hasn't been seen before.

    status: 'success' or 'failed'
    error_message: error text to record when status == 'failed' (ignored
                    otherwise; the error column is cleared on success)
    """
    timestamp = datetime.now(timezone.utc).isoformat()

    if event_id not in state_df.index:
        new_row = {col: pd.NA for col in STATE_COLUMNS}
        new_row['event_id'] = event_id
        state_df.loc[event_id] = new_row

    state_df.loc[event_id, 'state_extract'] = status
    state_df.loc[event_id, 'state_extract_error'] = error_message if status == 'failed' else pd.NA
    state_df.loc[event_id, 'state_extract_timestamp'] = timestamp

    return state_df