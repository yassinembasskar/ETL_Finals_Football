# load/load_daily_matches.py
import csv
from utils.connection import get_connection
from utils.logging_setup import setup_logger

logger = setup_logger("load", "logs/load.log")

def load_daily_matches(csv_file="../matches.csv"):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        with open(csv_file, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = []
            batch_size = 500

            for row in reader:
                rows.append((row["Title"], row["Sofascore_Link"], row["Fbref_Link"]))
                if len(rows) >= batch_size:
                    cursor.executemany(
                        "INSERT INTO matches (title, sofascore_link, fbref_link) VALUES (:1, :2, :3)",
                        rows
                    )
                    conn.commit()
                    logger.info(f"Inserted batch of {len(rows)} rows")
                    rows = []

            if rows:
                cursor.executemany(
                    "INSERT INTO matches (title, sofascore_link, fbref_link) VALUES (:1, :2, :3)",
                    rows
                )
                conn.commit()
                logger.info(f"Inserted final batch of {len(rows)} rows")
                
        open(csv_file, 'w').close()
        logger.info(f"Cleared {csv_file} after successful insert")

    except Exception as e:
        logger.error(f"Error loading daily matches: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.info("Connection closed after loading daily matches")
