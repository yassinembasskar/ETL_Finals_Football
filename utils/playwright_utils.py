# utils/playwright_utils.py
import asyncio
from playwright.async_api import async_playwright
from typing import List, Dict
from utils.logging_setup import setup_logger

logger = setup_logger("playwright_utils", "logs/playwright_utils.log")


async def capture_apis(match_url: str, api_prefix: str , headless: bool = True, wait_time: int = 20) -> List[Dict]:
    """
    Launches a Playwright browser, navigates to match_url, and captures API requests/responses.

    Args:
        match_url (str): Sofascore match URL to visit.
        headless (bool): Whether to run browser headless.
        wait_time (int): Time in seconds to wait for page to load API requests.

    Returns:
        List[Dict]: List of captured responses with keys: "api_link", "json_response"
    """
    match_requests = []
    match_responses = []


    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context()
            page = await context.new_page()

            def handle_request(request):
                if request.url.startswith(api_prefix):
                    match_requests.append(request.url)

            async def handle_response(response):
                if response.url.startswith(api_prefix) and response.status == 200:
                    try:
                        json_data = await response.json()
                        match_responses.append({
                            "api_link": response.url,
                            "json_response": json_data
                        })
                    except:
                        pass

            page.on("request", handle_request)
            page.on("response", handle_response)


            await page.goto(match_url, timeout=100000)
            await page.wait_for_timeout(wait_time * 1000)  


            page.remove_listener("request", handle_request)
            page.remove_listener("response", handle_response)

            await browser.close()
    except Exception as e:  
        print(f"Error capturing APIs for {match_url}: {e}")

    return match_responses



async def scrape_html(url: str, headless: bool = False, wait_time: int = 20) -> str:
    """
    Navigate to a URL using Playwright and return the page HTML.

    Args:
        url (str): The webpage URL to scrape
        headless (bool): Whether to run browser headless
        wait_time (int): Seconds to wait after page load

    Returns:
        str: The HTML content of the page
    """
    html_content = ""

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(viewport={"width": 1280, "height": 800})
            page = await context.new_page()

            await page.goto(url, timeout=100000)
            await page.wait_for_timeout(wait_time * 1000)  

            html_content = await page.content()
            await browser.close()

    except Exception as e:
        logger.error(f"Error scraping HTML for {url}: {e}")

    return html_content
