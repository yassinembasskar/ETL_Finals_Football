# Load and display the saved CSV file
import pandas as pd
from sqlalchemy import table
from utils.logging_setup import setup_logger
from transform.bs4_utils import extract_tables_from_html, extract_fbref_scorebox_team, extract_fbref_events
from transform.cleaning_tables_utils import clean_lineup_tables, clean_match_summary_table, clean_minute, integer
from transform.cleaning_stats_utils import get_stats, merge_results, get_player_stats

logger = setup_logger("transform", "logs/transform.log")

#You need to see events Id 9 and 10 because they affect the same team for both team events

def transformation_process():
    today = "2026-01-07"
    data = pd.read_csv(f"raw/{today}_fbref.csv") # Modify the date to today this is just for testing
    teams = []
    players = []
    events = []
    for index, row in data.iterrows():
        oracle_id = row['oracle_id']
        scraped_html = row['scraped_html']
        try: 
            with open(scraped_html, "r", encoding="utf-8") as f:
                html = f.read()
                point = "tables"
                tables_dict = extract_tables_from_html(html)
                point = "scorebox"
                scorebox_teams = extract_fbref_scorebox_team(html)
                point = "events"
                sub_events = extract_fbref_events(html)
        except:
            print(f"error extracting {point} from {index}")
            continue

        try:
            point = "tables"
            cleanup_tables(tables_dict)
            point = "events"
            cleanup_events(sub_events, scorebox_teams, oracle_id) #always clean events before scorebox
            point = "scorebox"
            cleanup_scorebox(scorebox_teams)
        except:
            print(f"error scraping {point} from {index}")
            continue

        for order in ["first", "second"]:
            scorebox = scorebox_teams[0] if order == "first" else scorebox_teams[1]
            players.extend(match_players_table(oracle_id, tables_dict, order))
            teams.append(match_teams_table(oracle_id, tables_dict, scorebox, order))
        
        events.extend(sub_events)
        print(f"transformed the record {index} successfully")
        
    save_events(events)
    save_players(players)
    save_teams(teams)



def save_teams(teams):
    teams_header = ["match_id", "Team", "Formation", "Manager", "Captain", "Goals", "Expected Goals", "Possession", "Total Passes", "Accurate Passes", "Total Shots", "Shots on Target", "Saves"]
    teams = pd.DataFrame(teams)[teams_header]
    teams = teams.drop_duplicates()
    teams.fillna(0).to_csv('teams.csv', index=False)

def save_events(events):
    events_header = ["match_id", "Team","minute", "event_type", "player_name"]
    events_replace_columns = {"minute": "Minute", "event_type": "Event Type", "player_name": "Player Name"}
    events = pd.DataFrame(events)[events_header]
    events = events.rename(columns=events_replace_columns)
    events = events.drop_duplicates()
    events.fillna(0).to_csv('events.csv', index=False)

def save_players(players):
    players = pd.DataFrame(players)
    players = players[players['Minutes Played'] > 0]
    players = players.fillna(0).infer_objects(copy=False)
    players.to_csv('players.csv', index=False)



def cleanup_tables(tables_dict):
    table_9 = tables_dict['table_9']
    table_10 = tables_dict['table_10']
    table_11 = tables_dict['table_11']
    
    for order in ["first", "second"]:
        table_name_summary = f"{order}_team_summary"
        table_name_starters = f"{order}_team_starters"
        table_name_bench = f"{order}_team_bench"
        table_name = f"{order}_team"
        tables_dict[table_name_summary] = clean_match_summary_table(table_11, order)
        table = table_9 if order == "first" else table_10
        tables_dict[table_name], tables_dict[table_name_starters], tables_dict[table_name_bench] = clean_lineup_tables(table, order)
        
    for i in range(12):
        tables_dict.pop(f"table_{i}", None)

def cleanup_scorebox(list_of_teams):
    for i in list_of_teams:
        i.pop("team_url", None)
        i.pop("team_id", None)
        i.pop("logo_url", None)
        i['manager'] = i['manager'].replace("\xa0", " ")
        i['captain'] = i['captain'].replace("\xa0", " ")

def cleanup_events(list_of_events, scorebox_teams, oracle_id):
    remove_indices = []
    for i in range(len(list_of_events)):
        list_of_events[i].pop("player_url", None)
        list_of_events[i]['match_id'] = oracle_id
        print(list_of_events[i])
        print(scorebox_teams[:2])
        list_of_events[i]['Team'] = scorebox_teams[0]['team_name'] if list_of_events[i]['team_logo'] == scorebox_teams[0]['logo_url'] else scorebox_teams[1]['team_name']
        print(list_of_events[i])
        list_of_events[i].pop("team_logo", None)
        if not list_of_events[i]['minute']:
            remove_indices.append(i)
        else:
            list_of_events[i]['minute'] = int(clean_minute(list_of_events[i]['minute']))

    for i in remove_indices:
        print(i)
        list_of_events.pop(i)

def match_teams_table(oracle_id, tables_dict, scorebox_team, team_order):
    table_name_team = f"{team_order}_team"
    teams = {}
    teams["match_id"] = oracle_id
    
    teams["Team"] = scorebox_team['team_name']
    teams["Formation"] = tables_dict[table_name_team].iloc[0]['formation']
    teams["Goals"] = integer(scorebox_team['goals'])
    teams["Expected Goals"] = scorebox_team['xg']
    teams["Manager"] = scorebox_team['manager']
    teams["Captain"] = scorebox_team['captain']

    table_name_summary = f"{team_order}_team_summary"
    for name in ["Possession", "Total Passes", "Accurate Passes", "Total Shots", "Shots on Target", "Saves"]:
        try:
            teams[name] = integer(tables_dict[table_name_summary].iloc[0][name])
        except:
            logger.info(f"there is no value in {name}")
            teams[name] = 0
    
    return teams

def match_players_table(oracle_id, tables_dict, team_order):
    players = []
    table_name_starters = f"{team_order}_team_starters"
    table_name_bench = f"{team_order}_team_bench"
    results = get_stats(tables_dict, team_order)
    if results:
        merge = merge_results(results)

    for type in ["Starting", "Bench"]:
        table_name = table_name_starters if type == "Starting" else table_name_bench
        for index, row in tables_dict[table_name].iterrows():
            player = {
                "match_id": oracle_id,
                "Team": tables_dict[f"{team_order}_team"].iloc[0]['team_name'],
                "Starting/Bench": type,
                "Jersey Number": row['#'],
                "Player Name": row['Name'],
            }

            if merge is not None:
                player_stats = get_player_stats(merge, row['Name'])
                player.update(player_stats)
            else:
                print(f"{row['Name']} does not exist")
            

            players.append(player)
        if len(players) == 0:
            print(f"{oracle_id} this one is vide")
    return players

if __name__ == "__main__":
    transformation_process()