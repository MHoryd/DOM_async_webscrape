import random
import json
from typing import Dict
import httpx
from prefect import flow, task, get_run_logger
from prefect.deployments import run_deployment
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/115.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36",
]


@task
def get_random_headers() -> Dict[str, str]:
    return {
        "user-agent": random.choice(USER_AGENTS),
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.5",
    }


@task(retries=3, retry_delay_seconds=40)
def fetch_url_content(url: str):
    raw_response = httpx.get(url, headers=get_random_headers())
    soup = BeautifulSoup(raw_response.text, "html.parser")
    data = soup.find("script", id="__NEXT_DATA__")
    payload: Dict[str, Union[str, int]] = json.loads(data.text)["props"]["pageProps"]["ad"]  # type: ignore
    return payload


@flow(log_prints=True)
def perform_scrape_of_offer_details(offer_url: str):
    logger = get_run_logger()
    json_data = fetch_url_content(url=offer_url)
    logger.info(f"Fetched json data len: {len(json_data)}")
