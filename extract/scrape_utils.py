# extract/scrape_utils.py
from typing import List, Dict
from utils.logging_setup import setup_logger
from utils.playwright_utils import capture_apis, scrape_html

# Setup a logger specifically for scraping utilities
logger = setup_logger("scraping", "logs/scraping.log")

async def scrape_sofascore(link: str) -> List[Dict[str, str]]:
    """
    Scrape data from a Sofascore API or webpage.

    Args:
        link (str): The Sofascore link for the match.

    Returns:
        List[Dict[str, str]]: A list of dictionaries with keys:
            - "api_link": the API link used
            - "json_response": the JSON response as a string
    """
    SOFASCORE_API_PREFIX = "https://www.sofascore.com/api/v1"

    try:
        logger.info(f"Starting Sofascore scrape for link: {link}")

        responses = await capture_apis(match_url=link, api_prefix=SOFASCORE_API_PREFIX)

        logger.info(f"Sofascore scrape completed successfully for link: {link}")
        return responses

    except Exception as e:
        logger.error(f"Error scraping Sofascore link {link}: {e}")
        return []


async def scrape_fbref(link: str) -> str:
    """
    Scrape HTML content from a Fbref link.

    Args:
        link (str): The Fbref link for the match.

    Returns:
        str: The scraped HTML as a string.
    """
    try:
        logger.info(f"Starting Fbref scrape for link: {link}")

        html_content = await scrape_html(url=link)

        logger.info(f"Fbref scrape completed successfully for link: {link}")
        return html_content

    except Exception as e:
        logger.error(f"Error scraping Fbref link {link}: {e}")
        return ""