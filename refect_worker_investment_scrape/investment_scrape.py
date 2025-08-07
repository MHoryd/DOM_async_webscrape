from playwright.sync_api import sync_playwright
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment


HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
}


@flow(log_prints=True)
def handle_url(start_url: str):
    logger = get_run_logger()
    logger.info(f"Processing URL: {start_url}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=HEADERS["user-agent"],
            locale="pl-PL",
            viewport={"width": 1280, "height": 720},
            device_scale_factor=1,
            is_mobile=False,
        )
        page = context.new_page()
        page.goto(start_url, wait_until="load")

        try:
            consent_button = page.locator("//button[@id='onetrust-accept-btn-handler']")
            consent_button.click()
        except Exception:
            pass

        all_hrefs = set()
        naxt_page_avaiable = True

        while naxt_page_avaiable:
            try:
                hrefs = page.locator(
                    "ul[data-cy='adverts-list-container'] li a"
                ).evaluate_all(
                    "elements => elements.map(el => el.getAttribute('href'))"
                )
                all_hrefs.update(hrefs)
            except Exception as e:
                logger.error(f"Failed to extract hrefs: {e}")
                break

            next_btn = page.locator(
                "//ul[@data-cy='adverts-pagination']/li[@title='Go to next Page']"
            )
            if not next_btn.is_visible():
                logger.info("No pagination available (single page)")
                break

            aria_disabled = next_btn.get_attribute("aria-disabled")
            is_disabled_attr = next_btn.get_attribute("disabled")

            if next_btn.is_visible() and (
                aria_disabled != "true" and is_disabled_attr is None
            ):
                next_btn.click()
                page.wait_for_timeout(2000)
            else:
                naxt_page_avaiable = False

        browser.close()

        for href in sorted(all_hrefs):
            if href:
                run_deployment(
                    name="perform-scrape-of-offer-details/details_scrape",
                    as_subflow=False,
                    parameters={"offer_url": "https://www.otodom.pl" + href},
                    timeout=0,
                )
