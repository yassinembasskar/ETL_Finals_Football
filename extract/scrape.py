import asyncio
import os
import json
from datetime import date, datetime, timedelta

import pytz
import pandas as pd

from utils.playwright_utils import capture_apis
from utils.logging_setup import setup_logger
from utils.pipeline_state import load_state, save_state, update_extract_state

logger = setup_logger("scrape", "logs/scrape.log")

API_PREFIX = "https://www.sofascore.com/api/v1"
MOROCCO_TZ = pytz.timezone("Africa/Casablanca")
RAW_FOLDER = "raw"

TOURNAMENTS = {
    16:  "FIFA World Cup",
    357: "FIFA Club World Cup",
    7:   "UEFA Champions League",
    679: "UEFA Europa League",
    17:  "Premier League",
    8:   "La Liga",
    35:  "Bundesliga",
    23:  "Serie A",
    34:  "Ligue 1"
}

MATCH_ENDPOINTS = [
    "incidents",
    "lineups",
    "average-positions",
    "statistics",
    "shotmap",
    "highlights",
]


# ---------------------------------------------------------------------------
# Stage 1: get the list of completed matches for a given date
# ---------------------------------------------------------------------------

def build_url(target_date: str) -> str:
    return f"https://www.sofascore.com/fr/football/{target_date}"


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


async def get_todays_matches(target_date: str = None, tournaments: dict = None) -> pd.DataFrame:

    tournaments_to_use = tournaments if tournaments is not None else TOURNAMENTS
    if target_date:
        fetch_date = target_date
    else:
        fetch_date = (datetime.now(MOROCCO_TZ).date() - timedelta(days=1)).isoformat()

    dates_to_fetch = [fetch_date]

    logger.info(f"get_todays_matches: fetching matches for {fetch_date}")
    logger.info(f"get_todays_matches: scraping tournaments: {tournaments_to_use}")

    seen = set()
    all_matches = []

    for fetch_date in dates_to_fetch:
        print(f"this is the dates to fitch: {dates_to_fetch} and this it the current date {fetch_date}")
        url = build_url(fetch_date)
        logger.info(f"get_todays_matches: fetching {fetch_date} from {url}")
        try:
            responses = await capture_apis(url, API_PREFIX, headless=False, wait_time=10)
        except Exception as e:
            logger.error(f"get_todays_matches: failed to capture APIs for {fetch_date} | {type(e).__name__}: {e}")
            continue

        for r in responses:
            for tournament_id, competition in tournaments_to_use.items():
                endpoint = f"{API_PREFIX}/unique-tournament/{tournament_id}/scheduled-events/{fetch_date}"
                if r["api_link"] == endpoint:
                    events = r["json_response"].get("events", [])
                    for event in events:
                        event_id = event.get("id")
                        if event_id not in seen:
                            parsed = parse_event(event, competition)
                            if parsed["status"] == "Ended":
                                seen.add(event_id)
                                all_matches.append(parsed)

    df = pd.DataFrame(all_matches)
    os.makedirs(RAW_FOLDER, exist_ok=True)
    output_date = fetch_date
    output_path = os.path.join(RAW_FOLDER, f"{output_date}_sofascore.csv")
    df.to_csv(output_path, index=False)
    logger.info(f"get_todays_matches: saved {len(df)} matches -> {output_path}")
    return df


# ---------------------------------------------------------------------------
# Stage 2: scrape full per-match detail data for every match in the list
# ---------------------------------------------------------------------------

async def get_data_from_match(event_id: int, slug: str, custom_id: str) -> dict:
    logger.info(f"get_data_from_match: scraping match event_id={event_id} ({slug})")

    base_url = f"https://www.sofascore.com/fr/football/match/{slug}/{custom_id}"
    stats_url = f"{base_url}#id:{event_id},tab:statistics"
    shotmap_url = f"{base_url}#id:{event_id},tab:shotmap"

    urls_to_visit = [base_url, stats_url, shotmap_url]

    all_responses = []
    for url in urls_to_visit:
        try:
            responses = await capture_apis(url, API_PREFIX, headless=False, wait_time=10)
            all_responses.extend(responses)
        except Exception as e:
            logger.error(f"get_data_from_match: failed to capture APIs for event_id={event_id} at {url} | {type(e).__name__}: {e}")
            continue

    match_data = {"event_id": event_id}
    pending_endpoints = set(MATCH_ENDPOINTS)

    for r in all_responses:
        if not pending_endpoints:
            break
        matched_endpoints = []
        for endpoint in pending_endpoints:
            expected = f"{API_PREFIX}/event/{event_id}/{endpoint}"
            if r["api_link"] == expected:
                matched_endpoints.append(endpoint)
                key = endpoint.replace("/", "_").replace("-", "_")
                if key not in match_data:
                    match_data[key] = json.dumps(r["json_response"])
                    logger.info(f"get_data_from_match: captured {endpoint} for event_id={event_id}")

        pending_endpoints.difference_update(matched_endpoints)
        

    missing = [ep for ep in pending_endpoints]
    if missing:
        logger.warning(f"get_data_from_match: event_id={event_id} missing endpoints: {missing}")

    return match_data

async def scrape_all_matches(date_str: str):
    input_path = os.path.join(RAW_FOLDER, f"{date_str}_sofascore.csv")

    if not os.path.exists(input_path):
        logger.error(f"scrape_all_matches: input file not found: {input_path}")
        return

    df = pd.read_csv(input_path)

    kickoff_date = df["kickoff"].astype(str).str.split(" ").str[0]
    mismatched_mask = kickoff_date != date_str
    if mismatched_mask.any():
        removed_event_ids = df.loc[mismatched_mask, "event_id"].tolist()
        df = df.loc[~mismatched_mask].reset_index(drop=True)
        df.to_csv(input_path, index=False)
        logger.info(
            f"scrape_all_matches: removed {len(removed_event_ids)} row(s) with mismatched "
            f"kickoff date for {date_str} | event_ids={removed_event_ids}"
        )

    logger.info(f"scrape_all_matches: found {len(df)} matches to scrape for {date_str}")

    state_df = load_state()

    all_data = []

    for _, row in df.iterrows():
        event_id = row["event_id"]
        try:
            match_data = await get_data_from_match(
                row["event_id"],
                row["slug"],
                row["custom_id"]
            )
            update_extract_state(state_df, event_id, status='success')
            logger.info(f"scrape_all_matches: succeeded for event_id={event_id}")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}"
            update_extract_state(state_df, event_id, status='failed', error_message=error_message)
            logger.error(f"scrape_all_matches: failed to scrape event_id={event_id} | {error_message}")
            continue

        match_data["competition"]   = row["competition"]
        match_data["kickoff"]       = row["kickoff"]
        match_data["home_team"]     = row["home_team"]
        match_data["home_team_id"]  = row["home_team_id"]
        match_data["away_team"]     = row["away_team"]
        match_data["away_team_id"]  = row["away_team_id"]
        match_data["home_score"]    = row["home_score"]
        match_data["away_score"]    = row["away_score"]
        match_data["slug"]          = row["slug"]
        match_data["custom_id"]     = row["custom_id"]
        match_data["sofascore_link"] = row["sofascore_link"]

        all_data.append(match_data)

    save_state(state_df)

    output_df = pd.DataFrame(all_data)
    output_path = os.path.join(RAW_FOLDER, f"{date_str}_match_data.csv")
    output_df.to_csv(output_path, index=False)
    logger.info(f"scrape_all_matches: saved {len(output_df)} matches -> {output_path}")

    
# ---------------------------------------------------------------------------
# Main: run both stages in sequence for a given date
# ---------------------------------------------------------------------------

async def main(date_str: str, tournaments: dict = None):
    logger.info(f"main: starting scrape run for date_str={date_str}")
    await get_todays_matches(target_date=date_str, tournaments=tournaments)
    await scrape_all_matches(date_str)
    logger.info(f"main: finished scrape run for date_str={date_str}")


def parse_tournament_args(args: list) -> dict:
    """
    Parses a flat list of alternating tournament_id/name pairs from the
    CLI (e.g. ['16', 'World Cup', '17', 'Premier League']) into a dict
    {16: 'FIFA World Cup', 17: 'Premier League'}.

    If tournament_id is already a known key in TOURNAMENTS, the canonical
    name from TOURNAMENTS is used instead of whatever name was typed on
    the CLI -- this keeps competition names consistent across the
    pipeline regardless of CLI shorthand/typos. The CLI-provided name is
    only used as a fallback for tournament_ids not already in TOURNAMENTS.

    Returns None if args is empty, so callers can default to the full
    TOURNAMENTS dict.
    """
    if not args:
        return None
    if len(args) % 2 != 0:
        raise ValueError(
            f"Expected an even number of tournament_id/name arguments, got {len(args)}: {args}"
        )
    tournaments = {}
    for i in range(0, len(args), 2):
        tournament_id = int(args[i])
        cli_name = args[i + 1]
        if tournament_id in TOURNAMENTS:
            canonical_name = TOURNAMENTS[tournament_id]
            if canonical_name != cli_name:
                logger.warning(
                    f"parse_tournament_args: tournament_id={tournament_id} was given as '{cli_name}' "
                    f"on the CLI but is already registered as '{canonical_name}' -- using '{canonical_name}'"
                )
            tournaments[tournament_id] = canonical_name
        else:
            tournaments[tournament_id] = cli_name
    return tournaments


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m extract.scrape YYYY-MM-DD [tournament_id name [tournament_id name ...]]")
        sys.exit(1)

    date_str = sys.argv[1]
    tournament_args = sys.argv[2:]
    tournaments = parse_tournament_args(tournament_args)

    asyncio.run(main(date_str, tournaments=tournaments))