# extract/scrape_match.py
import asyncio
import os
import json
import pandas as pd
from utils.playwright_utils import capture_apis
from utils.logging_setup import setup_logger

logger = setup_logger("scrape_match", "logs/scrape_match.log")

API_PREFIX = "https://www.sofascore.com/api/v1"
RAW_FOLDER = "raw"

MATCH_ENDPOINTS = [
    "incidents",
    "lineups",
    "best-players/summary",
    "average-positions",
    "statistics",
    "shotmap",
    "graph",
    "highlights",
]

async def get_data_from_match(event_id: int, slug: str, custom_id: str) -> dict:
    print(f"Scraping match {event_id} — {slug}...")

    base_url = f"https://www.sofascore.com/fr/football/match/{slug}/{custom_id}"
    stats_url = f"{base_url}#id:{event_id},tab:statistics"
    shotmap_url = f"{base_url}#id:{event_id},tab:shotmap"

    urls_to_visit = [base_url, stats_url, shotmap_url]

    all_responses = []
    for url in urls_to_visit:
        responses = await capture_apis(url, API_PREFIX, headless=False, wait_time=10)
        all_responses.extend(responses)

    match_data = {"event_id": event_id}

    for r in all_responses:
        for endpoint in MATCH_ENDPOINTS:
            expected = f"{API_PREFIX}/event/{event_id}/{endpoint}"
            if r["api_link"] == expected:
                key = endpoint.replace("/", "_").replace("-", "_")
                if key not in match_data:
                    match_data[key] = json.dumps(r["json_response"])
                    print(f"  ✅ {endpoint}")

    return match_data

async def scrape_all_matches(date_str: str):
    input_path = os.path.join(RAW_FOLDER, f"{date_str}_sofascore.csv")

    if not os.path.exists(input_path):
        print(f"File not found: {input_path}")
        return

    df = pd.read_csv(input_path)
    print(f"Found {len(df)} matches to scrape")

    all_data = []

    for _, row in df.iterrows():
        match_data = await get_data_from_match(
            row["event_id"],
            row["slug"],
            row["custom_id"]
        )
        match_data["competition"]   = row["competition"]
        match_data["kickoff"]       = row["kickoff"]
        match_data["home_team"]     = row["home_team"]
        match_data["home_team_id"]  = row["home_team_id"]
        match_data["away_team"]     = row["away_team"]
        match_data["away_team_id"]  = row["away_team_id"]
        match_data["home_score"]    = row["home_score"]
        match_data["away_score"]    = row["away_score"]
        match_data["slug"] = row["slug"]
        match_data["custom_id"] = row["custom_id"]
        match_data["sofascore_link"] = row["sofascore_link"]
        
        all_data.append(match_data)

    output_df = pd.DataFrame(all_data)
    output_path = os.path.join(RAW_FOLDER, f"{date_str}_match_data.csv")
    output_df.to_csv(output_path, index=False)
    print(f"\nSaved → {output_path}")

if __name__ == "__main__":
    asyncio.run(scrape_all_matches("2026-06-17"))