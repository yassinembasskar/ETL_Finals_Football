# extract/test_scraping.py
import os
import csv
from datetime import date
from utils.logging_setup import setup_logger
from extract.scrape_utils import scrape_sofascore, scrape_fbref 
from utils.csv_utils import write_csv_row, read_csv_as_dict
import asyncio


logger = setup_logger("scraping", "logs/scraping.log")

RAW_FOLDER = "raw"
os.makedirs(RAW_FOLDER, exist_ok=True)

async def scrape_pending_matches():
    today_str = date.today().isoformat()  # e.g., "2026-01-05"
    sofascore_file = os.path.join(RAW_FOLDER, f"{today_str}_sofascore.csv")
    fbref_file = os.path.join(RAW_FOLDER, f"{today_str}_fbref.csv")

    dictionnary = read_csv_as_dict("matches.csv")
    sofascore_headers = ["new_id", "oracle_id", "api_link", "json_response"]
    fbref_headers = ["oracle_id", "scraped_html"]

    try:
        tasks = []
        new_id_counter = 1
        # Create all tasks
        for r in dictionnary:
            tasks.append(scrape_matches_async(
                r, new_id_counter, sofascore_file, fbref_file, sofascore_headers, fbref_headers
            ))
            new_id_counter += 1

            if len(tasks) >= 5:
                await asyncio.gather(*tasks)
                tasks = []

        if tasks:
            await asyncio.gather(*tasks)

        print("Scraping completed.")

    except Exception as e:
        logger.error(f"Error scraping matches: {e}")
        print(f"Error scraping matches: {e}")

    finally:
        logger.info(f"Connection closed after scraping pending matches, CSVs saved in {RAW_FOLDER}")
        print(f"Connection closed after scraping pending matches, CSVs saved in {RAW_FOLDER}")


async def scrape_matches_async(
        r, new_id_counter, sofascore_file, fbref_file, sofascore_headers, fbref_headers
):
    try:
        print(f"Scraping match ID {r['Title']}...")
        #print(f"Sofascore Link: {r['Sofascore_Link']}")
        #sofascore_data = await scrape_sofascore(r["Sofascore_Link"])
        print(f"Fbref Link: {r['Fbref_Link']}")
        fbref_data = await scrape_fbref(r["Fbref_Link"])
        '''
        for item in sofascore_data:
            write_csv_row(sofascore_file, sofascore_headers,
                          [new_id_counter, new_id_counter, item["api_link"], item["json_response"]])
            new_id_counter += 1 '''
        
        fbref_link = "raw/fbref/" + f"{new_id_counter}_fbref.html"
        with open(fbref_link, "w", encoding="utf-8") as f:
            f.write(fbref_data)
        write_csv_row(fbref_file, fbref_headers, [new_id_counter, fbref_link]) 
    except Exception as e:
        logger.error(f"Error scraping match {r['Title']}: {e}")
        print(f"Error scraping match {r['Title']}: {e}")

if __name__ == "__main__":
    asyncio.run(scrape_pending_matches())