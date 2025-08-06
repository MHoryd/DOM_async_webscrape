import random
import json
import httpx
from typing import Dict
from bs4 import BeautifulSoup
from prefect import get_run_logger, task, flow

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/115.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36"
]


@task
def get_random_headers() -> Dict[str, str]:
    return {
        'user-agent': random.choice(USER_AGENTS),
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.5',
    }


@task
def get_pages_count(property_type: str) -> int:# -> Any:# -> Any:
    url = f'https://www.otodom.pl/pl/wyniki/sprzedaz/{property_type}/cala-polska?page=1'
    r = httpx.get(url, headers=get_random_headers())
    soup = BeautifulSoup(r.text, 'html.parser')
    script_tag = soup.find('script', attrs={'id': '__NEXT_DATA__'})
    json_data = json.loads(script_tag.text) # type: ignore
    return json_data['props']['pageProps']['data']['searchAds']['pagination']['totalPages']

@flow(retries=5, retry_delay_seconds=30)
def perform_initial_scrape(property_type: str):
    logger = get_run_logger()
    seen_investment = set()
    total_pages_num = get_pages_count(property_type=property_type)
    logger.info(f"Found {total_pages_num} pages for {property_type}")