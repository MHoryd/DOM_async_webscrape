import random
import json
import httpx
import time
import asyncio
import logging
from typing import Dict, Set
from bs4 import BeautifulSoup
from prefect import get_run_logger, task, flow
from prefect.deployments import run_deployment

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/115.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36"
]


@task
async def get_random_headers():
    return {
        'user-agent': random.choice(USER_AGENTS),
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.5',
    }


@task(retries=5, retry_delay_seconds=30, log_prints=True)
async def get_pages_count(property_type: str) -> int:
    headers = await get_random_headers()
    async with httpx.AsyncClient() as client:
        r = await client.get(f'https://www.otodom.pl/pl/wyniki/sprzedaz/{property_type}/cala-polska?page=1',
                             headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    script_tag = soup.find('script', attrs={'id': '__NEXT_DATA__'})
    json_data = json.loads(script_tag.text) # type: ignore
    return json_data['props']['pageProps']['data']['searchAds']['pagination']['totalPages']


@task(retries=5, retry_delay_seconds=30, log_prints=True)
async def process_data(content: str, seen_investments: Set, logger: logging.Logger):
    soup = BeautifulSoup(content, 'html.parser')
    script_tag = soup.find('script', attrs={'id': '__NEXT_DATA__'})
    if not script_tag:
        logger.warning("No data script found. Sleep for 20s")
        await asyncio.sleep(20)
        return
    try:
        json_data = json.loads(script_tag.text)
        offers_list = json_data['props']['pageProps']['data']['searchAds']['items']
        for item in offers_list:
            offer_type = item.get('estate')
            offer_url = item.get('href')
            if offer_url:
                formatted_url = offer_url.replace('[lang]/ad', 'https://www.otodom.pl/pl/oferta').replace('hpr/', '')
                investment_url = 'https://www.otodom.pl/pl/oferta/' + item.get('slug').replace('hpr/', '')
                if offer_type == 'HOUSE':
                    flow_run = await run_deployment('perform-scrape-of-offer-details/details_scrape',
                                        parameters={"offer_url": formatted_url},
                                        timeout=0,
                                        as_subflow=False)
                    logger.info(f"Triggered flow run {flow_run}")
                elif offer_type == 'FLAT':
                    # testing
                    logger.info("Trigger FLAT deployment")
                elif offer_type == 'INVESTMENT' and investment_url not in seen_investments:
                    # testing
                    logger.info("Trigger INVESTMENT deployment")

    except Exception as e:
        logger.error(f"Failed to process data: {e}")

@flow(log_prints=True)
async def perform_initial_scrape(property_type: str):
    logger = get_run_logger()
    seen_investments = set()
    total_pages_num = await get_pages_count(property_type=property_type)
    logger.info(f"Found {total_pages_num} pages for {property_type}")
    for i in range(1, total_pages_num + 1):
        headers = await get_random_headers()
        url = f'https://www.otodom.pl/pl/wyniki/sprzedaz/{property_type}/cala-polska?page={i}'
        logger.info(f"Fetching page {i}: {url}")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url=url, headers=headers, timeout=10)
            await process_data(response.text, seen_investments, logger=logger) # type: ignore
            await asyncio.sleep(random.uniform(5, 10))
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
        await asyncio.sleep(1)