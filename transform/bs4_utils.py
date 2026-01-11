# Load and display the saved CSV file
import pandas as pd
from utils.logging_setup import setup_logger
import re
from bs4 import BeautifulSoup, Comment
from io import StringIO

logger = setup_logger("transform", "logs/transform.log")

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
        return None, None

    team = match.group(1).strip()
    formation = tuple(map(int, match.group(2).split("-")))
    return team, formation