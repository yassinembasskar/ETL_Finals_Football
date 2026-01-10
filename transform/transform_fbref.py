# Load and display the saved CSV file
import pandas as pd
from sqlalchemy import table
from utils.logging_setup import setup_logger
import re
from bs4 import BeautifulSoup, Comment
from io import StringIO

logger = setup_logger("transform", "logs/transform.log")
data = pd.read_csv(f"raw/2026-01-07_fbref.csv") # Modify the date to today this is just for testing

def extract_tables_from_html(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    tables = []
    
    for table in soup.find_all("table"):
        table_id = table.get("id")
        html_io = StringIO(str(table))
        df_table = pd.read_html(html_io)[0]
        tables.append((table_id, df_table))

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        if "<table" in comment:
            comment_soup = BeautifulSoup(comment, "lxml")
            for table in comment_soup.find_all("table"):
                table_id = table.get("id")
                html_io = StringIO(str(table))
                df_table = pd.read_html(html_io)[0]
                tables.append((table_id, df_table))

    tables_dict = {}
    
    for table_id, df_table in tables:
        if table_id is None:
            table_id = f"table_{len(tables_dict)}"
        tables_dict[table_id] = df_table

    return tables_dict


def extract_fbref_scorebox_team(html: str) -> list[dict]:
    """
    Extract detailed team-level information from FBref scorebox.

    Returns:
        List[Dict]: One dict per team with:
            - team_id
            - team_name
            - team_url
            - logo_url
            - goals
            - xg
            - manager
            - captain
    """
    soup = BeautifulSoup(html, "lxml")
    teams_data = []

    scorebox = soup.find("div", class_="scorebox")
    if not scorebox:
        return teams_data

    for team_div in scorebox.find_all("div", class_="scorebox_team"):

        team_link = team_div.find("strong")
        team_anchor = team_link.find("a") if team_link else None

        team_name = team_anchor.text.strip() if team_anchor else None

        logo_img = team_div.find("img", class_="teamlogo")
        logo_url = logo_img.get("src") if logo_img else None

        score_div = team_div.find("div", class_="score")
        xg_div = team_div.find("div", class_="score_xg")

        goals = int(score_div.text.strip()) if score_div and score_div.text.strip().isdigit() else None
        xg = float(xg_div.text.strip()) if xg_div else None

        manager = None
        captain = None

        for dp in team_div.find_all("div", class_="datapoint"):
            label_tag = dp.find("strong")
            if not label_tag:
                continue

            label = label_tag.text.strip()
            value = dp.text.replace(label, "").replace(":", "").strip()

            if label == "Manager":
                manager = value
            elif label == "Captain":
                captain = value

        teams_data.append({
            "team_name": team_name,
            "logo_url": logo_url,
            "goals": goals,
            "xg": xg,
            "manager": manager,
            "captain": captain
        })

    return teams_data

def extract_fbref_events(html: str) -> list[str]:
    """
    Extract team names from FBref scorebox.

    Returns:
        List[str]: List of team names.
    """
    soup = BeautifulSoup(html, "lxml")
    events = []

    for event_div in soup.find_all("div", class_="event"):
        minute_div = event_div.find("div")
        minute = minute_div.text.strip().replace("’", "") if minute_div else None

        icon = event_div.find("div", class_="event_icon")
        event_type = icon["class"][1] if icon else None

        player_link = event_div.find("a")
        player_name = player_link.text.strip() if player_link else None
        player_url = player_link.get("href") if player_link else None

        logo = event_div.find("img", class_="teamlogo")
        team_logo = logo.get("src") if logo else None

        events.append({
            "minute": minute,
            "event_type": event_type,
            "player_name": player_name,
            "player_url": player_url,
            "team_logo": team_logo
        })
    return events


def extract_team_and_formation(col_name: str):
    """
    Example input: 'Liverpool (4-3-3)'
    Output: ('Liverpool', (4, 3, 3))
    """
    match = re.match(r"(.+?)\s*\(([\d\-]+)\)", col_name)
    if not match:
        logger.info("No match found for column name:", col_name)
        print("No match found for column name:", col_name)
        return None, None

    team = match.group(1).strip()
    formation = tuple(map(int, match.group(2).split("-")))
    return team, formation



def cleanup_tables(tables_dict):
    for i in range(9):
        tables_dict.pop(f"table_{i}", None)

    def clean_lineup_tables():
        table_9 = tables_dict['table_9']
        table_10 = tables_dict['table_10']
        first_column_name = table_9.columns[0]
        second_column_name = table_9.columns[1]
        team_formation = extract_team_and_formation(first_column_name)
        tables_dict['first_team'] = {'team_name': team_formation[0], 'formation': team_formation[1]}
        tables_dict['first_team_starters'] = [{"#": row[first_column_name], "Name": row[second_column_name]} for index, row in table_9.iterrows() if index < 11]
        tables_dict['first_team_bench'] = [{"#": row[first_column_name], "Name": row[second_column_name]} for index, row in table_9.iterrows() if index > 11]
        first_column_name = table_10.columns[0]
        second_column_name = table_10.columns[1]
        team_formation = extract_team_and_formation(first_column_name)
        tables_dict['second_team'] = {'team_name': team_formation[0], 'formation': team_formation[1]}
        tables_dict['second_team_starters'] = [{"#": row[first_column_name], "Name": row[second_column_name]} for index, row in table_10.iterrows() if index < 11]
        tables_dict['second_team_bench'] = [{"#": row[first_column_name], "Name": row[second_column_name]} for index, row in table_10.iterrows() if index > 11]
        tables_dict.pop('table_9', None)
        tables_dict.pop('table_10', None)
        tables_dict['first_team'] = pd.DataFrame([tables_dict['first_team']])
        tables_dict['second_team'] = pd.DataFrame([tables_dict['second_team']])
        tables_dict['first_team_starters'] = pd.DataFrame(tables_dict['first_team_starters'])
        tables_dict['first_team_bench'] = pd.DataFrame(tables_dict['first_team_bench'])
        tables_dict['second_team_starters'] = pd.DataFrame(tables_dict['second_team_starters'])
        tables_dict['second_team_bench'] = pd.DataFrame(tables_dict['second_team_bench'])        

    clean_lineup_tables()

    def clean_match_summary_table(team: str = None):
        table_11 = tables_dict['table_11']
        first_column_name = table_11.columns[0]
        second_column_name = table_11.columns[1]
        def extract_possession(text):
            """Extract possession percentage as decimal"""
            if not text or pd.isna(text):
                return None
            match = re.search(r'(\d+)%', text)
            if match:
                return int(match.group(1)) / 100
            return None
        def extract_passes(text, team):
            """Extract total passes and accurate passes"""
            if not text or pd.isna(text):
                return None, None
            
            if team == 'first':
                match = re.search(r'(\d+)\s+of\s+(\d+)', text)
            else:
                match = re.search(r'(\d+)\s+of\s+(\d+)', text)
            
            if match:
                accurate = int(match.group(1))
                total = int(match.group(2))
                return total, accurate
            return None, None
        
        def extract_shots(text, team):
            """Extract total shots and shots on target"""
            if not text or pd.isna(text):
                return None, None
            
            if team == 'first':
                # Format: "9 of 24 — 38%"
                match = re.search(r'(\d+)\s+of\s+(\d+)', text)
            else:
                # Format: "25% — 1 of 4"
                match = re.search(r'(\d+)\s+of\s+(\d+)', text)
            
            if match:
                on_target = int(match.group(1))
                total = int(match.group(2))
                return total, on_target
            return None, None
        
        def extract_saves(text, team):
            """Extract saves"""
            if not text or pd.isna(text):
                return None
            
            if team == 'first':
                # Format: "0 of 1 — 0%"
                match = re.search(r'(\d+)\s+of\s+(\d+)', text)
            else:
                # Format: "100% — 9 of 9"
                match = re.search(r'(\d+)\s+of\s+(\d+)', text)
            
            if match:
                saves = int(match.group(1))
                return saves
            return None
        

        tables_dict['first_team_summary'] = {} 
        tables_dict["first_team_summary"]["Possession"] = extract_possession(table_11.iloc[0][first_column_name])
        tables_dict['second_team_summary'] = {}
        tables_dict["second_team_summary"]["Possession"] = extract_possession(table_11.iloc[0][second_column_name])
        tables_dict["first_team_summary"]["Total Passes"], tables_dict["first_team_summary"]["Accurate Passes"] = extract_passes(table_11.iloc[2][first_column_name], 'first')
        tables_dict["second_team_summary"]["Total Passes"], tables_dict["second_team_summary"]["Accurate Passes"] = extract_passes(table_11.iloc[2][second_column_name], 'second')
        tables_dict["first_team_summary"]["Total Shots"], tables_dict["first_team_summary"]["Shots on Target"] = extract_shots(table_11.iloc[4][first_column_name], 'first')
        tables_dict["second_team_summary"]["Total Shots"], tables_dict["second_team_summary"]["Shots on Target"] = extract_shots(table_11.iloc[4][second_column_name], 'second')
        tables_dict["first_team_summary"]["Saves"] = extract_saves(table_11.iloc[6][first_column_name], 'first')
        tables_dict["second_team_summary"]["Saves"] = extract_saves(table_11.iloc[6][second_column_name], 'second')
        tables_dict["first_team_summary"] = pd.DataFrame([tables_dict["first_team_summary"]])
        tables_dict["second_team_summary"] = pd.DataFrame([tables_dict["second_team_summary"]])
        tables_dict.pop('table_11', None)

    clean_match_summary_table()
    def clean_stats_summary_table(team: str):
        tables_dict["stats_{team}_summary"].columns = ['_'.join(col).strip() if col[0].startswith('Unnamed') == False else col[1] 
              for col in tables_dict["stats_summary"].columns]
        

def cleanup_scorebox(list_of_teams):
    for i in list_of_teams:
        i.pop("team_url", None)
        i.pop("team_id", None)
        i['manager'] = i['manager'].replace("\xa0", " ")
        i['captain'] = i['captain'].replace("\xa0", " ")

def cleanup_events(list_of_events, scorebox_teams, oracle_id):
    def clean_minute(minute_str):
        if minute_str is None:
            return None
        match = re.search(r'(\d+)(?:\+(\d+))?', minute_str)
        if match:
            base_minute = int(match.group(1))
            added_time = int(match.group(2)) if match.group(2) else 0
            return base_minute + added_time
        return None
    
    remove_indices = []
    for i in range(len(list_of_events)):
        list_of_events[i].pop("player_url", None)
        list_of_events[i]['match_id'] = oracle_id
        list_of_events[i]['Team'] = scorebox_teams[0]['team_name'] if list_of_events[i]['team_logo'] == scorebox_teams[0]['logo_url'] else scorebox_teams[1]['team_name']
        list_of_events[i].pop("team_logo", None)
        if not list_of_events[i]['minute']:
            remove_indices.append(i)
        else:
            list_of_events[i]['minute'] = int(clean_minute(list_of_events[i]['minute']))

    for i in remove_indices:
        list_of_events.pop(i)


def get_player_stats(stats_df, player_name):
    """
    Extract stats for a specific player from the stats dataframe.
    Handles MultiIndex columns.
    """
    # Find the player row
    player_row = stats_df[stats_df[('Unnamed: 0_level_0', 'Player')] == player_name]
    
    if player_row.empty:
        return {}
    
    # Extract the first matching row
    player_row = player_row.iloc[0]
    
    # Build stats dictionary
    stats = {
        # Basic info
        "Nation": player_row.get(('Unnamed: 2_level_0', 'Nation')),
        "Position": player_row.get(('Unnamed: 3_level_0', 'Pos')),
        "Age": player_row.get(('Unnamed: 4_level_0', 'Age')),
        "Minutes": player_row.get(('Unnamed: 5_level_0', 'Min')),
        
        # Performance stats
        "Goals": player_row.get(('Performance', 'Gls')),
        "Assists": player_row.get(('Performance', 'Ast')),
        "Penalty Goals": player_row.get(('Performance', 'PK')),
        "Penalty Attempts": player_row.get(('Performance', 'PKatt')),
        "Shots": player_row.get(('Performance', 'Sh')),
        "Shots on Target": player_row.get(('Performance', 'SoT')),
        "Yellow Cards": player_row.get(('Performance', 'CrdY')),
        "Red Cards": player_row.get(('Performance', 'CrdR')),
        "Touches": player_row.get(('Performance', 'Touches')),
        "Tackles": player_row.get(('Performance', 'Tkl')),
        "Interceptions": player_row.get(('Performance', 'Int')),
        "Blocks": player_row.get(('Performance', 'Blocks')),
        
        # Expected stats
        "Expected Goals": player_row.get(('Expected', 'xG')),
        "Non-Penalty Expected Goals": player_row.get(('Expected', 'npxG')),
        "Expected Assists": player_row.get(('Expected', 'xAG')),
        
        # Shot/Goal Creating Actions
        "Shots Creating Actions": player_row.get(('SCA', 'SCA')),
        "Goals Creating Actions": player_row.get(('SCA', 'GCA')),
        
        # Passes
        "Passes Completed": player_row.get(('Passes', 'Cmp')),
        "Passes Attempted": player_row.get(('Passes', 'Att')),
        "Pass Completion Percentage": player_row.get(('Passes', 'Cmp%')),
        "Progressive Passes": player_row.get(('Passes', 'PrgP')),
        
        # Carries
        "Carries": player_row.get(('Carries', 'Carries')),
        "Progressive Carries": player_row.get(('Carries', 'PrgC')),
        
        # Take-Ons
        "Dribbles Attempted": player_row.get(('Take-Ons', 'Att')),
        "Dribbles Successful": player_row.get(('Take-Ons', 'Succ')),
    }
    
    # Remove None values
    stats = {k: v for k, v in stats.items() if v is not None and not pd.isna(v)}
    return stats


def match_teams_table(oracle_id, tables_dict, scorebox_team, team_order):
    teams = {}
    teams["match_id"] = oracle_id
    teams["Team"] = scorebox_team['team_name']
    teams["Formation"] = tables_dict[f"{team_order}_team"].iloc[0]['formation']
    teams["Goals"] = int(scorebox_team['goals'])
    teams["Expected Goals"] = scorebox_team['xg']
    teams["Manager"] = scorebox_team['manager']
    teams["Captain"] = scorebox_team['captain']
    table_name = f"{team_order}_team_summary"
    teams["Possesion"] = int(tables_dict[table_name].iloc[0]['Possession'])
    teams["Total Passes"] = int(tables_dict[table_name].iloc[0]['Total Passes'])
    teams["Accurate Passes"] = int(tables_dict[table_name].iloc[0]['Accurate Passes'])
    teams["Total Shots"] = int(tables_dict[table_name].iloc[0]['Total Shots'])
    teams["Shots on Target"] = int(tables_dict[table_name].iloc[0]['Shots on Target'])
    teams["Saves"] = int(tables_dict[table_name].iloc[0]['Saves'])
    return teams

def match_players_table(oracle_id, tables_dict, team_order):
    players = []
    table_name_starters = f"{team_order}_team_starters"
    table_name_bench = f"{team_order}_team_bench"
    table_names_stats = [key for key in tables_dict.keys() if key.startswith(f"stats_") and key.endswith("_summary")]
    df_stats = tables_dict[table_names_stats[0]] if team_order == 'first' else tables_dict[table_names_stats[1]]
    for index, row in tables_dict[table_name_starters].iterrows():
        player = {
            "match_id": oracle_id,
            "Team": tables_dict[f"{team_order}_team"].iloc[0]['team_name'],
            "Starting/Bench": "Starting",
            "Jersey Number": row['#'],
            "Player Name": row['Name'],
        }
        if df_stats is not None:
            player_stats = get_player_stats(df_stats, row['Name'])
            player.update(player_stats)

        players.append(player)

    for index, row in tables_dict[table_name_bench].iterrows():
        player = {
            "match_id": oracle_id,
            "Team": tables_dict[f"{team_order}_team"].iloc[0]['team_name'],
            "Starting/Bench": "Bench",
            "Jersey Number": row['#'],
            "Player Name": row['Name'],
        }
        if df_stats is not None:
            player_stats = get_player_stats(df_stats, row['Name'])
            player.update(player_stats)

        players.append(player)

    
    return players

if __name__ == "__main__":
    for index, row in data.iterrows():
        teams = []
        players = []
        oracle_id = row['oracle_id']
        scraped_html = row['scraped_html']
        with open(scraped_html, "r", encoding="utf-8") as f:
            html = f.read()
            tables_dict = extract_tables_from_html(html)
            scorebox_teams = extract_fbref_scorebox_team(html)
            events = extract_fbref_events(html)
        cleanup_tables(tables_dict)
        cleanup_events(events, scorebox_teams, oracle_id)
        cleanup_scorebox(scorebox_teams)
        players.extend(match_players_table(oracle_id, tables_dict, 'first'))
        players.extend(match_players_table(oracle_id, tables_dict, 'second'))
        print(tables_dict["stats_822bd0ba_passing"].iloc[0])
        break
        ''''stats_822bd0ba_summary', 'stats_822bd0ba_passing', 'stats_822bd0ba_passing_types', 'stats_822bd0ba_defense', 'stats_822bd0ba_possession', 'stats_822bd0ba_misc', 'keeper_stats_822bd0ba', 'stats_53a2f082_summary', 'stats_53a2f082_passing', 'stats_53a2f082_passing_types', 'stats_53a2f082_defense', 'stats_53a2f082_possession', 'stats_53a2f082_misc', 'keeper_stats_53a2f082', 'shots_all', 'shots_822bd0ba', 'shots_53a2f082'''
        teams.append(match_teams_table(oracle_id, tables_dict, scorebox_teams[0], 'first'))
        teams.append(match_teams_table(oracle_id, tables_dict, scorebox_teams[1], 'second'))
        players.extend(match_players_table(oracle_id, tables_dict, 'first'))
        players.extend(match_players_table(oracle_id, tables_dict, 'second'))
        teams_header = ["match_id", "Team", "Formation", "Manager", "Captain", "Goals", "Expected Goals", "Possesion", "Total Passes", "Accurate Passes", "Total Shots", "Shots on Target", "Saves"]
        teams = pd.DataFrame(teams)[teams_header]
        teams = teams.drop_duplicates()
        events_header = ["match_id", "Team","minute", "event_type", "player_name"]
        events_replace_columns = {"minute": "Minute", "event_type": "Event Type", "player_name": "Player Name"}
        events = pd.DataFrame(events)[events_header]
        events = events.rename(columns=events_replace_columns)
        events = events.drop_duplicates()
        break
        """right now i just finished the summary stats only I need keeper stats and passes etc"""




