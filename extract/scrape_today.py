# extract/scrape_today.py
import asyncio
import os
from datetime import date, datetime, timedelta
import pytz
import pandas as pd
from utils.playwright_utils import capture_apis

API_PREFIX = "https://www.sofascore.com/api/v1"
MOROCCO_TZ = pytz.timezone("Africa/Casablanca")
RAW_FOLDER = "raw"

TOURNAMENTS = {
    16:  "FIFA World Cup 2026",
    357: "FIFA Club World Cup",
    7:   "UEFA Champions League",
    679: "UEFA Europa League",
    17:  "Premier League",
    8:   "La Liga",
    35:  "Bundesliga",
    23:  "Serie A",
    34:  "Ligue 1"
}

def build_url(target_date: str) -> str:
    return f"https://www.sofascore.com/fr/football/{target_date}"

def get_window():
    now = datetime.now(MOROCCO_TZ)
    today = now.date()
    yesterday = today - timedelta(days=1)
    window_start = MOROCCO_TZ.localize(datetime(yesterday.year, yesterday.month, yesterday.day, 8, 0, 0))
    window_end = MOROCCO_TZ.localize(datetime(today.year, today.month, today.day, 8, 0, 0))
    return window_start, window_end, today, yesterday

def parse_event(event: dict, competition: str) -> dict:
    kickoff_ts = event.get("startTimestamp")
    kickoff = datetime.fromtimestamp(kickoff_ts, tz=MOROCCO_TZ).strftime("%Y-%m-%d %H:%M") if kickoff_ts else None
    home = event.get("homeTeam", {})
    away = event.get("awayTeam", {})
    home_score = event.get("homeScore", {})
    away_score = event.get("awayScore", {})
    status = event.get("status", {})
    slug = event.get("slug")
    custom_id = event.get("customId")
    event_id = event.get("id")

    return {
        "event_id":       event_id,
        "competition":    competition,
        "kickoff":        kickoff,
        "home_team":      home.get("name"),
        "home_team_id":   home.get("id"),
        "away_team":      away.get("name"),
        "away_team_id":   away.get("id"),
        "home_score":     home_score.get("current"),
        "away_score":     away_score.get("current"),
        "status":         status.get("description"),
        "slug":           slug,
        "custom_id":      custom_id,
        "sofascore_link": f"https://www.sofascore.com/fr/football/match/{slug}/{custom_id}",
    }

async def get_todays_matches(target_date: str = None) -> pd.DataFrame:
    window_start, window_end, today, yesterday = get_window()
    dates_to_fetch = [yesterday.isoformat(), today.isoformat()]
    if target_date:
        dates_to_fetch = [target_date]

    print(f"Window: {window_start.strftime('%Y-%m-%d %H:%M')} → {window_end.strftime('%Y-%m-%d %H:%M')} (Morocco)")

    seen = set()
    all_matches = []

    for fetch_date in dates_to_fetch:
        url = build_url(fetch_date)
        print(f"Fetching {fetch_date}...")
        responses = await capture_apis(url, API_PREFIX, headless=False, wait_time=10)

        for r in responses:
            for tournament_id, competition in TOURNAMENTS.items():
                endpoint = f"{API_PREFIX}/unique-tournament/{tournament_id}/scheduled-events/{fetch_date}"
                if r["api_link"] == endpoint:
                    events = r["json_response"].get("events", [])
                    for event in events:
                        kickoff_ts = event.get("startTimestamp")
                        if not kickoff_ts:
                            continue
                        kickoff = datetime.fromtimestamp(kickoff_ts, tz=MOROCCO_TZ)
                        if window_start <= kickoff <= window_end:
                            event_id = event.get("id")
                            if event_id not in seen:
                                parsed = parse_event(event, competition)
                                if parsed["status"] == "Ended":
                                    seen.add(event_id)
                                    all_matches.append(parsed)

    df = pd.DataFrame(all_matches)
    os.makedirs(RAW_FOLDER, exist_ok=True)
    output_date = yesterday.isoformat()
    output_path = os.path.join(RAW_FOLDER, f"{output_date}_sofascore.csv")
    df.to_csv(output_path, index=False)
    print(f"\nSaved {len(df)} matches → {output_path}")
    return df

if __name__ == "__main__":
    asyncio.run(get_todays_matches())