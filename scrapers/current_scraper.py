import logging
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from db_manager import insert_current_queue_records

LOGGER = logging.getLogger(__name__)

CURRENT_URL = "https://gpk.gov.by/situation-at-the-border/"

# Default checkpoints that are important for this project.
DEFAULT_CHECKPOINTS = [
    "Брест",
    "Брузги",
    "Каменный Лог",
    "Бенякони",
    "Козловичи",
    "Берестовица",
]


def fetch_current_page_html(
    url: str = CURRENT_URL,
    retries: int = 3,
    timeout: int = 20,
    backoff_seconds: int = 3,
) -> str:
    """
    Download current queue HTML with basic retry logic.
    """
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            LOGGER.info("Fetching current queue page, attempt %s/%s", attempt, retries)
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except Exception as error:  # noqa: BLE001
            last_error = error
            LOGGER.warning("Attempt %s failed: %s", attempt, error)
            if attempt < retries:
                time.sleep(backoff_seconds * attempt)

    raise RuntimeError(f"Could not fetch current queue page: {last_error}") from last_error


def _to_int(value: str) -> int:
    only_digits = re.sub(r"[^\d]", "", value or "")
    return int(only_digits) if only_digits else 0


def _extract_transport_values(block_text: str) -> tuple[int, int, int]:
    """
    Extract cars/trucks/buses from a text block.
    The parser tries label-based regex first and then falls back
    to generic number extraction.
    """
    cars = trucks = buses = 0

    car_match = re.search(r"легков\w*[^\d]{0,20}(\d+)", block_text, re.IGNORECASE)
    truck_match = re.search(r"грузов\w*[^\d]{0,20}(\d+)", block_text, re.IGNORECASE)
    bus_match = re.search(r"автобус\w*[^\d]{0,20}(\d+)", block_text, re.IGNORECASE)

    if car_match:
        cars = _to_int(car_match.group(1))
    if truck_match:
        trucks = _to_int(truck_match.group(1))
    if bus_match:
        buses = _to_int(bus_match.group(1))

    # Fallback for unknown layout: use first numeric values in block.
    if cars == 0 and trucks == 0 and buses == 0:
        numbers = re.findall(r"\d+", block_text)
        if numbers:
            cars = _to_int(numbers[0]) if len(numbers) > 0 else 0
            trucks = _to_int(numbers[1]) if len(numbers) > 1 else 0
            buses = _to_int(numbers[2]) if len(numbers) > 2 else 0

    return cars, trucks, buses


def parse_current_queue(
    html: str,
    checkpoints: list[str] | None = None,
) -> list[dict]:
    """
    Parse current queue page and return checkpoint records.
    """
    checkpoints = checkpoints or DEFAULT_CHECKPOINTS
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)
    if "Очереди в автодорожных пунктах пропуска" not in page_text:
        LOGGER.warning("Unexpected page content. Parsing may be inaccurate.")

    records: list[dict] = []
    scrape_time = datetime.now().replace(second=0, microsecond=0).isoformat()

    # Candidate elements where queue values are usually placed.
    candidate_elements = soup.select("tr, li, div, article, section")

    for checkpoint in checkpoints:
        checkpoint_block_text = ""

        # Find the first meaningful block containing checkpoint name.
        for element in candidate_elements:
            text = element.get_text(" ", strip=True)
            if checkpoint.lower() in text.lower() and len(text) > len(checkpoint):
                checkpoint_block_text = text
                break

        # Fallback to full page text if block was not found.
        if not checkpoint_block_text:
            checkpoint_block_text = page_text

        cars, trucks, buses = _extract_transport_values(checkpoint_block_text)
        records.append(
            {
                "checkpoint": checkpoint,
                "cars_out": cars,
                "trucks_out": trucks,
                "buses_out": buses,
                "timestamp": scrape_time,
            }
        )

    return records


def scrape_and_store_current_queue() -> int:
    """
    Full pipeline:
    1) download current page,
    2) parse checkpoint values,
    3) store results in database.
    """
    html = fetch_current_page_html()
    records = parse_current_queue(html)
    inserted_count = insert_current_queue_records(records)
    LOGGER.info("Current queue scrape done. Inserted rows: %s", inserted_count)
    return inserted_count
