# extract/scrape_links.py
import asyncio
import os
import csv
from datetime import date
from utils.connection import get_connection
from utils.logging_setup import setup_logger
from scrape_utils import scrape_sofascore, scrape_fbref 
from utils.csv_utils import write_csv_row


logger = setup_logger("scraping", "logs/scraping.log")

RAW_FOLDER = "raw"
os.makedirs(RAW_FOLDER, exist_ok=True)

async def scrape_pending_matches():
    """
    Scrape all matches in the 'matches' table where etl_status = 'pending'
    and store results in raw/ as CSV files:
    - %today%_sofascore.csv
    - %today%_fbref.csv
    """
    today_str = date.today().isoformat()  # e.g., "2026-01-05"
    sofascore_file = os.path.join(RAW_FOLDER, f"{today_str}_sofascore.csv")
    fbref_file = os.path.join(RAW_FOLDER, f"{today_str}_fbref.csv")


    sofascore_headers = ["new_id", "oracle_id", "api_link", "json_response"]
    fbref_headers = ["oracle_id", "scraped_html"]

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT id, sofascore_link, fbref_link FROM matches WHERE etl_status = 'pending'")
        pending_rows = cursor.fetchall()
        logger.info(f"Found {len(pending_rows)} pending rows to scrape")
        tasks = []

        for r in pending_rows:
            try:
                tasks.append(scrape_matches_async(
                    conn, cursor, r[0], r[1], r[2], sofascore_file, fbref_file, sofascore_headers, fbref_headers
                ))
            
                logger.info(f"✅ Scraped and updated match id {r[0]} successfully.")

                if len(tasks) >= 5:
                    await asyncio.gather(*tasks)
                    tasks = []

            except Exception as e:
                logger.error(f"Error scraping match id {r[0]}: {e}")
            

        if tasks:
            await asyncio.gather(*tasks)    

    except Exception as e:
        logger.error(f"Error querying pending matches: {e}")

    finally:
        cursor.close()
        conn.close()
        logger.info(f"Connection closed after scraping pending matches, CSVs saved in {RAW_FOLDER}")


async def scrape_matches_async(
    conn,
    cursor,
    match_id,
    sofascore_link,
    fbref_link,
    sofascore_file,
    fbref_file,
    sofascore_headers,
    fbref_headers
):
    try:
        print(f"Scraping match ID {match_id}...")
        print(f"Sofascore Link: {sofascore_link}")
        sofascore_data = await scrape_sofascore(sofascore_link)

        print(f"Fbref Link: {fbref_link}")
        fbref_data = await scrape_fbref(fbref_link)
        counter_id = 0
        for item in sofascore_data:
            write_csv_row(
                sofascore_file,
                sofascore_headers,
                [counter_id, match_id, item["api_link"], item["json_response"]]
            )
            counter_id += 1

        write_csv_row(
            fbref_file,
            fbref_headers,
            [match_id, fbref_data]
        )

        cursor.execute("UPDATE matches SET etl_status = 'scraped', last_modified = SYSDATE WHERE id = :1", (match_id,))
        conn.commit()

    except Exception as e:
        logger.error(f"Error scraping match {match_id}: {e}")
        print(f"Error scraping match {match_id}: {e}")
