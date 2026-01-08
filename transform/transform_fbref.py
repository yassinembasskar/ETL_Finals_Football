# Load and display the saved CSV file
import pandas as pd
from utils.logging_setup import setup_logger
from utils.csv_utils import read_csv_as_dict
from datetime import date
from utils.bs4_utils import extract_fbref_scorebox_team, extract_tables_from_html
import re


logger = setup_logger("transform", "logs/transform.log")
data = pd.read_csv(f"raw/2026-01-07_fbref.csv")


def extract_team_and_formation(col_name: str):
    """
    Example input: 'Liverpool (4-3-3)'
    Output: ('Liverpool', (4, 3, 3))
    """
    match = re.match(r"(.+?)\s*\(([\d\-]+)\)", col_name)
    if not match:
        print("No match found for column name:", col_name)
        return None, None

    team = match.group(1).strip()
    formation = tuple(map(int, match.group(2).split("-")))
    return team, formation



def cleanup_tables(tables_dict):
    for i in range(9):
        tables_dict.pop(f"table_{i}", None)
    tables_dict["general"] = {"first_team": extract_team_and_formation(tables_dict['table_9'].columns[0])[0],
                              "first_formation": extract_team_and_formation(tables_dict['table_9'].columns[0])[1],
                              "second_team": extract_team_and_formation(tables_dict['table_10'].columns[1])[0],
                              "second_formation": extract_team_and_formation(tables_dict['table_10'].columns[1])[1]}
    print("General Info: ", tables_dict["general"])



'''
for i in range(9):  # 0 to 8
    tables_dict.pop(f"table_{i}", None)

print(f"Extracted {len(tables_dict)} tables from the first match's HTML.")
print("Tables extracted: ", list(tables_dict.keys()))

for name, df_table in tables_dict.items():
    print(f"\n{name}")
    print(df_table)'''

if __name__ == "__main__":
    for index, row in data.iterrows():
        oracle_id = row['oracle_id']
        scraped_html = row['scraped_html']
        with open(scraped_html, "r", encoding="utf-8") as f:
            html = f.read()
            tables_dict = extract_tables_from_html(html)
            scorebox_teams = extract_fbref_scorebox_team(html)
        cleanup_tables(tables_dict)


