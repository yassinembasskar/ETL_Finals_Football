# extract/scrape_links.py
import os
import csv
from datetime import date
from utils.connection import get_connection
from utils.logging_setup import setup_logger
from scrape_utils import scrape_sofascore, scrape_fbref 
from utils.csv_utils import write_csv_row


logger = setup_logger("scraping", "logs/scraping.log")

RAW_FOLDER = "../raw"
os.makedirs(RAW_FOLDER, exist_ok=True)

def scrape_pending_matches():
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

        new_id_counter = 1  

        for row_id, sofascore_link, fbref_link in pending_rows:
            try:
                sofascore_data = scrape_sofascore(sofascore_link)
                fbref_data = scrape_fbref(fbref_link)

                # Write to CSV
                for item in sofascore_data:
                    api_link = item["api_link"]
                    json_response = item["json_response"]
                    write_csv_row(sofascore_file, sofascore_headers, [new_id_counter, row_id, api_link, json_response])
                    new_id_counter += 1

                write_csv_row(fbref_file, fbref_headers, [row_id, fbref_data])
                

                # Update ETL status in DB
                cursor.execute("UPDATE matches SET etl_status = 'scraped', last_modified = SYSDATE WHERE id = :1", (row_id,))
                conn.commit()
                logger.info(f"✅ Scraped and updated match id {row_id}")

            except Exception as e:
                logger.error(f"Error scraping match id {row_id}: {e}")

    except Exception as e:
        logger.error(f"Error querying pending matches: {e}")

    finally:
        cursor.close()
        conn.close()
        logger.info(f"Connection closed after scraping pending matches, CSVs saved in {RAW_FOLDER}")

