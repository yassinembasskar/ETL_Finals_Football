# extract/scrape_wc.py
from bs4 import BeautifulSoup
import pandas as pd

FILES = [
    {"path": "2022_wc.html", "limit": 70},
    {"path": "2026_wc.html", "limit": 20},
]

def parse_fixtures(html: str, limit: int) -> list:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")

    if not table:
        print("Table not found")
        return []

    rows = table.find("tbody").find_all("tr")
    matches = []
    count = 0

    for row in rows:
        if count >= limit:
            break

        classes = row.get("class", [])
        if "spacer" in classes or "thead" in classes:
            continue

        date_td    = row.find("td", {"data-stat": "date"})
        home_td    = row.find("td", {"data-stat": "home_team"})
        away_td    = row.find("td", {"data-stat": "away_team"})
        score_td   = row.find("td", {"data-stat": "score"})
        report_td  = row.find("td", {"data-stat": "match_report"})

        if not date_td or not home_td or not away_td:
            continue

        date       = date_td.text.strip()
        home_team  = home_td.find("a").text.strip() if home_td.find("a") else ""
        away_team  = away_td.find("a").text.strip() if away_td.find("a") else ""
        score      = score_td.text.strip() if score_td else ""
        fbref_link = "https://fbref.com" + report_td.find("a")["href"] if report_td and report_td.find("a") else ""

        if not home_team or not away_team:
            continue

        matches.append({
            "Title": f"{home_team} vs {away_team}",
            "Date": date,
            "Home Team": home_team,
            "Away Team": away_team,
            "Score": score,
            "Fbref_Link": fbref_link,
            "Rating (0-100)": ""
        })
        count += 1

    return matches

def extract_wc_matches():
    all_matches = []

    for file in FILES:
        print(f"Parsing {file['path']}...")
        with open(file["path"], "r", encoding="utf-8") as f:
            html = f.read()
        matches = parse_fixtures(html, file["limit"])
        print(f"Extracted {len(matches)} matches from {file['path']}")
        all_matches.extend(matches)

    df = pd.DataFrame(all_matches)
    df.to_excel("matches_to_rate.xlsx", index=False)
    print(f"Total: {len(df)} matches → matches_to_rate.xlsx")

if __name__ == "__main__":
    extract_wc_matches()