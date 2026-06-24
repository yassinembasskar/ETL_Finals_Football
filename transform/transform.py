import json
import os
import pandas as pd
import argparse
import tempfile
import shutil
pd.set_option('future.no_silent_downcasting', True)
from utils.pipeline_state import load_state, save_state, update_transform_state
from utils.logging_setup import setup_logger

logger = setup_logger("transform", "logs/transform.log")

# ---------------------------------------------------------------------------
# Player identity extraction + registry
# ---------------------------------------------------------------------------

def extract_player_id_and_info(player_dict):
    if player_dict is None or (isinstance(player_dict, float) and pd.isna(player_dict)):
        return None, None

    try:
        player_id = player_dict.get('id')
        country = player_dict.get('country') or {}
        market_value = (player_dict.get('proposedMarketValueRaw') or {}).get('value')
        dob_ts = player_dict.get('dateOfBirthTimestamp')

        identity_row = {
            'IdPlayer': player_id,
            'Name': player_dict.get('name'),
            'Country': country.get('name'),
            'marketValue': market_value,
            'dateOfBirth': pd.to_datetime(dob_ts, unit='s') if dob_ts is not None else None,
            'height': player_dict.get('height'),
        }
        return player_id, identity_row
    except Exception as e:
        logger.error(
            f"extract_player_id_and_info: failed to parse player_dict "
            f"(keys={list(player_dict.keys()) if isinstance(player_dict, dict) else type(player_dict)}) "
            f"| {type(e).__name__}: {e}"
        )
        raise


class PlayerRegistry:

    def __init__(self):
        self._seen_ids = set()
        self._identity_rows = []

    def get_or_add(self, player_dict):
        player_id, identity_row = extract_player_id_and_info(player_dict)
        if player_id is None:
            return None

        try:
            if player_id not in self._seen_ids:
                self._seen_ids.add(player_id)
                self._identity_rows.append(identity_row)
        except Exception as e:
            logger.error(
                f"PlayerRegistry.get_or_add: failed to register player_id={player_id} | "
                f"{type(e).__name__}: {e}"
            )
            raise

        return player_id

    def to_dataframe(self):
        if not self._identity_rows:
            return pd.DataFrame(columns=['IdPlayer', 'Name', 'Country',
                                          'marketValue', 'dateOfBirth', 'height'])
        return pd.DataFrame(self._identity_rows).reset_index(drop=True)

# ---------------------------------------------------------------------------
# match
# ---------------------------------------------------------------------------

def get_full_highlight(raw, event_id):
    try:
        highlights_raw = raw['highlights'].iloc[0]
        if pd.isna(highlights_raw):
            return None
        highlights_json = json.loads(highlights_raw)
        highlights_df = pd.DataFrame(highlights_json['highlights'].tolist()
                                      if hasattr(highlights_json['highlights'], 'tolist')
                                      else highlights_json['highlights'])
        if highlights_df.empty or 'keyHighlight' not in highlights_df.columns:
            return None
        key_rows = highlights_df.loc[highlights_df['keyHighlight'] == True, 'url']
        if key_rows.empty:
            return None
        return key_rows.iloc[0]
    except Exception as e:
        logger.warning(f"get_full_highlight: failed to parse highlights for event_id={event_id}, returning None | {type(e).__name__}: {e}")
        return None        


def get_match_table(row):
    event_id = row['event_id'].iloc[0]
    try:
        result = pd.DataFrame([{
            'event_id': event_id,
            'competition': row['competition'].iloc[0],
            'kickoff': row['kickoff'].iloc[0],
            'home_team_id': row['home_team_id'].iloc[0],
            'away_team_id': row['away_team_id'].iloc[0],
            'home_score': row['home_score'].iloc[0],
            'away_score': row['away_score'].iloc[0],
            'slug': row['slug'].iloc[0],
            'custom_id': row['custom_id'].iloc[0],
            'sofascore_link': row['sofascore_link'].iloc[0],
            'full_highlight_url': get_full_highlight(row, event_id),
        }])
        logger.info(f"get_match_table: succeeded for event_id={event_id}")
        return result
    except Exception as e:
        logger.error(f"get_match_table: failed for event_id={event_id} | {type(e).__name__}: {e}")
        raise

# ---------------------------------------------------------------------------
# team
# ---------------------------------------------------------------------------

def get_team(team_id, team_name):
    return pd.DataFrame([{'team_id': team_id, 'teamName': team_name}])

# ---------------------------------------------------------------------------
# lineups -> meta-only home/away player frames + long player stats
# ---------------------------------------------------------------------------

def _get_lineups_players(row, registry):
    event_id = row['event_id'].iloc[0]
    try:
        lineups = json.loads(row["lineups"].iloc[0])
        lineups = pd.DataFrame(lineups)

        formations = {
            'home': lineups.loc['formation', 'home'],
            'away': lineups.loc['formation', 'away'],
        }

        home_players = pd.DataFrame(lineups.loc['players', 'home'])
        away_players = pd.DataFrame(lineups.loc['players', 'away'])

        home_players['IdPlayer'] = home_players['player'].apply(registry.get_or_add)
        away_players['IdPlayer'] = away_players['player'].apply(registry.get_or_add)

        def build_stats_long(players_df, event_id):
            rows = []
            for player_id, team_id, stats_dict in zip(players_df['IdPlayer'], players_df['teamId'], players_df['statistics']):
                if not isinstance(stats_dict, dict):
                    continue
                for stat_label, stat_value in stats_dict.items():
                    if isinstance(stat_value, dict):
                        continue  # skip nested-dict stats (e.g. ratingVersions) -- not useful for ML
                    rows.append({'eventId': event_id, 'teamId': team_id, 'playerId': player_id,
                                 'stat_label': stat_label, 'stat_value': stat_value})
            return pd.DataFrame(rows, columns=['eventId', 'teamId', 'playerId', 'stat_label', 'stat_value'])

        home_players_stats = build_stats_long(home_players, event_id)
        away_players_stats = build_stats_long(away_players, event_id)

        home_players = home_players.drop(columns=['statistics', 'player'])
        away_players = away_players.drop(columns=['statistics', 'player'])

        return home_players, home_players_stats, away_players, away_players_stats, formations
    except Exception as e:
        logger.error(f"_get_lineups_players: failed for event_id={event_id} | {type(e).__name__}: {e}")
        raise


# ---------------------------------------------------------------------------
# match_team (one row per team per match, includes formation; stats live
# separately in match_team_stats, kept long for ML/reporting flexibility)
# ---------------------------------------------------------------------------

def get_match_team(row, formations):
    event_id = row['event_id'].iloc[0]
    try:
        home_team_id = row['home_team_id'].iloc[0]
        away_team_id = row['away_team_id'].iloc[0]
        home_score = row['home_score'].iloc[0]
        away_score = row['away_score'].iloc[0]

        return pd.DataFrame([
            {
                'event_id': event_id,
                'team_id': home_team_id,
                'isHome': True,
                'score': home_score,
                'formation': formations['home'],
            },
            {
                'event_id': event_id,
                'team_id': away_team_id,
                'isHome': False,
                'score': away_score,
                'formation': formations['away'],
            },
        ])
    except Exception as e:
        logger.error(f"get_match_team: failed for event_id={event_id} | {type(e).__name__}: {e}")
        raise

# ---------------------------------------------------------------------------
# match_team_stats (long format: event_id, team_id, stat_name, stat_value)
# ---------------------------------------------------------------------------

def _build_team_stats_long(groups, side, event_id, team_id):
    value_key = f'{side}Value'
    total_key = f'{side}Total'
    rows = []
    for group in groups:
        group_name = group['groupName']
        for item in group['statisticsItems']:
            name = item['name']
            if total_key in item:
                rows.append({
                    'event_id': event_id, 'team_id': team_id,
                    'stat_name': f'Successful {group_name} {name}',
                    'stat_value': item[value_key],
                })
                rows.append({
                    'event_id': event_id, 'team_id': team_id,
                    'stat_name': f'Total {group_name} {name}',
                    'stat_value': item[total_key],
                })
            else:
                rows.append({
                    'event_id': event_id, 'team_id': team_id,
                    'stat_name': f'{group_name} {name}',
                    'stat_value': item[value_key],
                })
    return pd.DataFrame(rows)


def get_match_team_stats(row):
    event_id = row['event_id'].iloc[0]
    try:
        home_team_id = row['home_team_id'].iloc[0]
        away_team_id = row['away_team_id'].iloc[0]

        statistics = json.loads(row['statistics'].iloc[0])
        statistics_df = pd.DataFrame(statistics['statistics'])
        groups = statistics_df['groups'].iloc[0]

        home_long = _build_team_stats_long(groups, 'home', event_id, home_team_id)
        away_long = _build_team_stats_long(groups, 'away', event_id, away_team_id)

        return pd.concat([home_long, away_long], ignore_index=True)
    except Exception as e:
        logger.error(f"get_match_team_stats: failed for event_id={event_id} | {type(e).__name__}: {e}")
        raise

# ---------------------------------------------------------------------------
# match_players (wide: event_id, team_id, IdPlayer + identity-ish match
# fields like jerseyNumber/position/substitute/captain + avg position)
# ---------------------------------------------------------------------------

def get_match_players(row, home_players, away_players, avg_home_positions, avg_away_positions):
    event_id = row['event_id'].iloc[0]
    try:
        home = home_players.copy()
        away = away_players.copy()

        home_avg = pd.json_normalize(avg_home_positions['player']) if 'player' in avg_home_positions else None
        away_avg = pd.json_normalize(avg_away_positions['player']) if 'player' in avg_away_positions else None

        def attach_avg_position(side_df, avg_positions_df, avg_id_df):
            if avg_id_df is None or avg_positions_df is None or avg_positions_df.empty:
                side_df = side_df.copy()
                side_df['averageX'] = 50
                side_df['averageY'] = 50
                return side_df
            avg = avg_positions_df.copy()
            avg['IdPlayer'] = avg_id_df['id'].values
            avg = avg.rename(columns={'averageX': 'averageX', 'averageY': 'averageY'})
            merged = side_df.merge(avg[['IdPlayer', 'averageX', 'averageY']], on='IdPlayer', how='left')
            merged['averageX'] = merged['averageX'].fillna(50)
            merged['averageY'] = merged['averageY'].fillna(50)
            return merged

        home = attach_avg_position(home, avg_home_positions, home_avg)
        away = attach_avg_position(away, avg_away_positions, away_avg)

        keep_cols = ['IdPlayer', 'teamId', 'jerseyNumber', 'position', 'substitute',
                     'captain', 'averageX', 'averageY']

        home_out = home.reindex(columns=keep_cols)
        away_out = away.reindex(columns=keep_cols)

        out = pd.concat([home_out, away_out], ignore_index=True)
        out.insert(0, 'event_id', event_id)

        out['captain'] = out['captain'].fillna(False).infer_objects(copy=False).astype(bool)
        out = out.dropna(subset=['averageX', 'averageY'])

        return out
    except Exception as e:
        logger.error(f"get_match_players: failed for event_id={event_id} | {type(e).__name__}: {e}")
        raise
# ---------------------------------------------------------------------------
# incidents -> goals, cards, substitutions, passing_network
# ---------------------------------------------------------------------------
def _team_id_from_is_home(is_home_series, home_team_id, away_team_id):
    return is_home_series.map({True: home_team_id, False: away_team_id})


def get_passing_network_table(goal_id, network_actions, registry):
    if not isinstance(network_actions, list):
        return pd.DataFrame(columns=['goal_id', 'playerId', 'type', 'order',
                                      'player_coordinates', 'action_coordinates'])

    rows = []
    order = 0

    for action in network_actions:
        event_type = action.get('eventType')
        player_dict = action.get('player')
        player_id = registry.get_or_add(player_dict)

        if event_type == 'pass' and action.get('isAssist'):
            row_type = 'assist'
        else:
            row_type = event_type

        if event_type in ('pass', 'cross'):
            player_coords = action.get('playerCoordinates')
            action_coords = action.get('passEndCoordinates')
        elif event_type in ('goal', 'save'):
            player_coords = action.get('playerCoordinates')
            action_coords = action.get('goalShotCoordinates')
        else:
            player_coords = action.get('playerCoordinates')
            action_coords = None

        rows.append({
            'goal_id': goal_id,
            'playerId': player_id,
            'type': row_type,
            'order': order,
            'player_coordinates': player_coords,
            'action_coordinates': action_coords,
        })
        order += 1

        # Synthesize a 'keeper' row right after the goal action.
        if event_type == 'goal' and action.get('goalkeeper') is not None:
            keeper_id = registry.get_or_add(action.get('goalkeeper'))
            rows.append({
                'goal_id': goal_id,
                'playerId': keeper_id,
                'type': 'keeper',
                'order': order,
                'player_coordinates': action.get('gkCoordinates'),
                'action_coordinates': action.get('goalMouthCoordinates'),
            })
            order += 1

    return pd.DataFrame(rows, columns=['goal_id', 'playerId', 'type', 'order',
                                        'player_coordinates', 'action_coordinates'])


def get_incidents_tables(row, registry):
    event_id = row['event_id'].iloc[0]
    try:
        home_team_id = row['home_team_id'].iloc[0]
        away_team_id = row['away_team_id'].iloc[0]

        incidents_json = json.loads(row['incidents'].iloc[0])
        incidents = pd.DataFrame(incidents_json['incidents'])
        incidents['id'] = incidents['id'].astype('Int64')

        if 'player' not in incidents.columns:
            incidents['player'] = None

        substitutions = incidents.loc[
            incidents['incidentType'] == 'substitution',
            ['id', 'isHome', 'playerIn', 'playerOut', 'injury', 'time', 'addedTime']
        ].copy().rename(columns={'id': 'sub_id'})

        cards = incidents.loc[
            incidents['incidentType'] == 'card',
            ['id', 'isHome', 'incidentClass', 'time', 'addedTime', 'player']
        ].copy().rename(columns={'id': 'card_id'})

        if 'assist1' not in incidents.columns:
            incidents['assist1'] = None
        if 'footballPassingNetworkAction' not in incidents.columns:
            incidents['footballPassingNetworkAction'] = None

        goals = incidents.loc[
            incidents['incidentType'] == 'goal',
            ['id', 'isHome', 'homeScore', 'awayScore', 'time', 'addedTime', 'player', 'assist1', 'footballPassingNetworkAction']
        ].copy().rename(columns={'id': 'goal_id'})
        goals['hasAssist'] = goals['assist1'].notna()

        if not cards.empty:
            cards['player_id'] = cards['player'].apply(registry.get_or_add)
        else:
            cards['player_id'] = pd.Series(dtype='object')
        cards = cards.drop(columns=['player'])
        cards['player_id'] = cards['player_id'].astype('Int64')

        if not substitutions.empty:
            substitutions['playerIn_id'] = substitutions['playerIn'].apply(registry.get_or_add)
            substitutions['playerOut_id'] = substitutions['playerOut'].apply(registry.get_or_add)
            substitutions = substitutions.drop(columns=['playerIn', 'playerOut'])

        cards['isHome'] = cards['isHome'].astype(bool)

        substitutions['isHome'] = substitutions['isHome'].astype(bool)
        substitutions['injury'] = substitutions['injury'].astype(bool)

        goals['isHome'] = goals['isHome'].astype(bool)

        passing_networks = []
        goal_team_ids = {}
        if not goals.empty:
            goals['player_id'] = goals['player'].apply(registry.get_or_add)
            goals['assist1_id'] = goals['assist1'].apply(registry.get_or_add)

            goal_team_ids = dict(zip(goals['goal_id'], _team_id_from_is_home(goals['isHome'], home_team_id, away_team_id)))

            for goal_id, network_actions in zip(goals['goal_id'], goals['footballPassingNetworkAction']):
                passing_networks.append(get_passing_network_table(goal_id, network_actions, registry))

            goals = goals.drop(columns=['player', 'assist1', 'footballPassingNetworkAction'])

        if passing_networks:
            passing_network_df = pd.concat(passing_networks, ignore_index=True)
        else:
            passing_network_df = pd.DataFrame(columns=['goal_id', 'playerId', 'type', 'order',
                                                         'player_coordinates', 'action_coordinates'])

        if not passing_network_df.empty:
            passing_network_df.insert(0, 'event_id', event_id)
            passing_network_df['team_id'] = passing_network_df['goal_id'].map(goal_team_ids)

        passing_network_df['has_action_coordinates'] = passing_network_df['action_coordinates'].notna()

        for df_ in (substitutions, cards, goals):
            df_.insert(0, 'event_id', event_id)
            df_['team_id'] = _team_id_from_is_home(df_['isHome'], home_team_id, away_team_id)

        return {'goals': goals, 'cards': cards, 'substitutions': substitutions,
                'passing_network': passing_network_df}
    except Exception as e:
        logger.error(f"get_incidents_tables: failed for event_id={event_id} | {type(e).__name__}: {e}")
        raise

# ---------------------------------------------------------------------------
# highlights (per match)
# ---------------------------------------------------------------------------

def get_highlights_table(row):
    event_id = row['event_id'].iloc[0]
    empty_result = pd.DataFrame(columns=['event_id', 'title', 'subtitle', 'url', 'createdAtTimestamp'])

    try:
        highlights_str = row['highlights'].iloc[0]

        highlights_json = json.loads(highlights_str)
        highlights = pd.DataFrame(highlights_json)

        key_subtitles = ['Goal', 'Goal (replay)', 'Chance', 'Chance (replay)', 'Big chance', 'Big chance (replay)', 'Cross', 'Goal Disallowed', 'Goal Disallowed (replay)'
                          'Penalty', 'Penalty (replay)', 'Penalty missed', 'VAR (Replay)', 'Penalty Disallowed (VAR decision)', 'Penalty Disallowed']

        highlights_df = pd.DataFrame(highlights['highlights'].tolist()).sort_values('createdAtTimestamp')

        highlights_table = highlights_df.loc[(highlights_df['subtitle'].isin(key_subtitles)), ['title', 'subtitle', 'url', 'createdAtTimestamp']].copy()
        highlights_table.insert(0, 'event_id', event_id)

        return highlights_table
    except Exception as e:
        logger.warning(f"get_highlights_table: failed to parse highlights for event_id={event_id}, returning empty table | {type(e).__name__}: {e}")
        return empty_result


# ---------------------------------------------------------------------------
# shotmaps (per match)
# ---------------------------------------------------------------------------

def get_shotmaps_table(row, registry):
    event_id = row['event_id'].iloc[0]
    cols = ['eventId', 'playerId', 'teamId', 'shotType', 'situation', 'playerCoordinates',
            'bodyPart', 'goalMouthLocation', 'goalMouthCoordinates',
            'blockCoordinates', 'xg', 'xgot', 'goalkeeperId', 'time', 'addedTime']
    empty_result = pd.DataFrame(columns=cols)

    try:
        home_team_id = row['home_team_id'].iloc[0]
        away_team_id = row['away_team_id'].iloc[0]

        shotmaps = json.loads(row['shotmap'].iloc[0])
        shotmaps_df = pd.DataFrame(shotmaps['shotmap'])

        shotmaps_df = shotmaps_df.copy()
        shotmaps_df['playerId'] = shotmaps_df['player'].apply(registry.get_or_add)
        shotmaps_df['goalkeeperId'] = shotmaps_df['goalkeeper'].apply(registry.get_or_add)

        shotmaps_df['teamId'] = _team_id_from_is_home(shotmaps_df['isHome'], home_team_id, away_team_id)

        keep_cols = ['playerId', 'teamId', 'shotType', 'situation', 'playerCoordinates',
                'bodyPart', 'goalMouthLocation', 'goalMouthCoordinates',
                'blockCoordinates', 'xg', 'xgot', 'goalkeeperId', 'time', 'addedTime']
        shotmaps_df = shotmaps_df.reindex(columns=keep_cols)
        shotmaps_df.insert(0, 'eventId', event_id)
        return shotmaps_df
    except Exception as e:
        logger.warning(f"get_shotmaps_table: failed to parse shotmap for event_id={event_id}, returning empty table | {type(e).__name__}: {e}")
        return empty_result
# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def transform_row(row, registry):
    event_id = row['event_id'].iloc[0]

    home_players, home_players_stats, away_players, away_players_stats, formations = _get_lineups_players(row, registry)

    try:
        avg_positions = json.loads(row['average_positions'].iloc[0])
        avg_home_positions = pd.DataFrame(avg_positions['home'])
        avg_away_positions = pd.DataFrame(avg_positions['away'])
    except Exception as e:
        logger.warning(f"transform_row: failed to parse average_positions for event_id={event_id}, falling back to (50, 50) | {type(e).__name__}: {e}")
        avg_home_positions = pd.DataFrame()
        avg_away_positions = pd.DataFrame()

    match_df = get_match_table(row)

    team_home = get_team(row['home_team_id'].iloc[0], row['home_team'].iloc[0])
    team_away = get_team(row['away_team_id'].iloc[0], row['away_team'].iloc[0])
    team_df = pd.concat([team_home, team_away], ignore_index=True)

    match_team_df = get_match_team(row, formations)
    match_team_stats_df = get_match_team_stats(row)

    match_players_df = get_match_players(row, home_players, away_players,
                                          avg_home_positions, avg_away_positions)
    match_player_stats_df = pd.concat([home_players_stats, away_players_stats], ignore_index=True)

    incidents_tables = get_incidents_tables(row, registry)

    highlights_df = get_highlights_table(row)
    shotmaps_df = get_shotmaps_table(row, registry)

    return {
        'match': match_df,
        'team': team_df,
        'match_team': match_team_df,
        'match_team_stats': match_team_stats_df,
        'match_players': match_players_df,
        'match_player_stats': match_player_stats_df,
        'goals': incidents_tables['goals'],
        'cards': incidents_tables['cards'],
        'substitutions': incidents_tables['substitutions'],
        'passing_network': incidents_tables['passing_network'],
        'highlights': highlights_df,
        'shotmaps': shotmaps_df,
    }

def transform_csv(date_str, csv_dir='raw', output_dir='processed'):
    try:
        csv_path = f"{csv_dir}/{date_str}_match_data.csv"
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"transform_csv: failed to read raw CSV at {csv_path} | {type(e).__name__}: {e}")
        raise

    registry = PlayerRegistry()

    accumulated = {
        'match': [], 'team': [], 'match_team': [], 'match_team_stats': [],
        'match_players': [], 'match_player_stats': [],
        'goals': [], 'cards': [], 'substitutions': [], 'passing_network': [], 'highlights': [],
        'shotmaps': [],
    }

    state_df = load_state()
    succeeded_event_ids = []

    for i in range(len(df)):
        row = df.iloc[[i]]
        event_id = row['event_id'].iloc[0]
        try:
            tables = transform_row(row, registry)
            update_transform_state(state_df, event_id, status='success')
            succeeded_event_ids.append(event_id)
            logger.info(f"transform_row: succeeded for event_id={event_id}")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}"
            update_transform_state(state_df, event_id, status='failed', error_message=error_message)
            logger.error(f"transform_row: failed for event_id={event_id} | {error_message}")
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

    date_output_dir = f"{output_dir}/{date_str}"

    try:
        with tempfile.TemporaryDirectory() as staging_dir:
            for table_name, table_df in final_tables.items():
                staging_path = f"{staging_dir}/{table_name}.parquet"
                table_df.to_parquet(staging_path, index=False)

            os.makedirs(date_output_dir, exist_ok=True)
            for table_name in final_tables:
                staging_path = f"{staging_dir}/{table_name}.parquet"
                final_path = f"{date_output_dir}/{table_name}.parquet"
                shutil.move(staging_path, final_path)
                print(f"Wrote {final_path} ({len(final_tables[table_name])} rows)")
                logger.info(f"transform_csv: wrote {final_path} ({len(final_tables[table_name])} rows)")
    except Exception as e:
        error_message = f"write failed: {type(e).__name__}: {e}"
        logger.error(f"transform_csv: failed to write output tables for date_str={date_str} | {error_message}")
        for event_id in succeeded_event_ids:
            update_transform_state(state_df, event_id, status='failed', error_message=error_message)
        save_state(state_df)
        raise

    save_state(state_df)

    return final_tables


def main():
    parser = argparse.ArgumentParser(description="Run the transform pipeline and save output tables as parquet.")
    parser.add_argument('date_str', help="Date string for the raw CSV, e.g. 2026-06-17")
    parser.add_argument('--csv-dir', default='raw')
    parser.add_argument('--output-dir', default='processed')
    args = parser.parse_args()

    transform_csv(args.date_str, csv_dir=args.csv_dir, output_dir=args.output_dir)

if __name__ == '__main__':
    main()