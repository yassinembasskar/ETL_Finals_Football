from bs4 import BeautifulSoup, Comment
from io import StringIO
import pandas as pd


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
        team_id = team_div.get("id")

        team_link = team_div.find("strong")
        team_anchor = team_link.find("a") if team_link else None

        team_name = team_anchor.text.strip() if team_anchor else None
        team_url = team_anchor.get("href") if team_anchor else None

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
            "team_id": team_id,
            "team_name": team_name,
            "team_url": team_url,
            "logo_url": logo_url,
            "goals": goals,
            "xg": xg,
            "manager": manager,
            "captain": captain
        })

    return teams_data
