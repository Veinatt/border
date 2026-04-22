import logging
import re
import time
from datetime import date, datetime, timedelta

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from db_manager import init_db, insert_archive_records

LOGGER = logging.getLogger(__name__)

ARCHIVE_URL = "https://gpk.gov.by/situation-at-the-border/arkhiv-ocheredey/"

CHECKPOINTS = [
    "Брест",
    "Брузги",
    "Каменный Лог",
    "Бенякони",
    "Козловичи",
    "Берестовица",
]


def _build_driver(headless: bool = True) -> webdriver.Chrome:
    """
    Build Chrome WebDriver with webdriver-manager.
    """
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def _safe_extract_int(text: str) -> int:
    match = re.search(r"\d+", text or "")
    return int(match.group(0)) if match else 0


def _select_checkpoint(driver: webdriver.Chrome, checkpoint: str) -> None:
    """
    Select checkpoint in archive UI.
    IMPORTANT: CSS selectors may require adjustment if website layout changes.
    """
    wait = WebDriverWait(driver, 20)

    # First try native <select> (most stable case).
    try:
        select_element = wait.until(
            ec.presence_of_element_located((By.CSS_SELECTOR, "select"))
        )
        Select(select_element).select_by_visible_text(checkpoint)
        return
    except Exception:  # noqa: BLE001
        pass

    # Fallback for custom dropdown controls.
    dropdown_candidates = driver.find_elements(
        By.CSS_SELECTOR,
        ".select, .dropdown, [class*='select'], [class*='dropdown']",
    )
    for dropdown in dropdown_candidates:
        try:
            dropdown.click()
            option = wait.until(
                ec.element_to_be_clickable(
                    (By.XPATH, f"//*[contains(text(), '{checkpoint}')]")
                )
            )
            option.click()
            return
        except Exception:  # noqa: BLE001
            continue

    raise RuntimeError(f"Could not select checkpoint: {checkpoint}")


def _set_date(driver: webdriver.Chrome, target_date: date) -> None:
    """
    Set date in archive UI.
    IMPORTANT: date picker selector may require adjustment.
    """
    wait = WebDriverWait(driver, 20)
    date_value = target_date.strftime("%d.%m.%Y")

    # Try native date/text input.
    date_inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='date'], input")
    for date_input in date_inputs:
        input_name = (date_input.get_attribute("name") or "").lower()
        input_id = (date_input.get_attribute("id") or "").lower()
        if "date" in input_name or "date" in input_id or "calendar" in input_name:
            date_input.clear()
            date_input.send_keys(date_value)
            date_input.send_keys("\n")
            return

    # Fallback: click by visible text around calendar widgets.
    try:
        calendar_trigger = wait.until(
            ec.element_to_be_clickable(
                (By.CSS_SELECTOR, ".calendar, [class*='calendar'], [class*='date']")
            )
        )
        calendar_trigger.click()
        # If calendar popup is custom and not keyboard-editable,
        # this part must be adapted manually after first inspection.
    except TimeoutException as error:
        raise RuntimeError("Date picker was not found on archive page.") from error


def _parse_archive_table(driver: webdriver.Chrome, checkpoint: str, target_date: date) -> list[dict]:
    """
    Parse current table values from page source and map
    transport labels into normalized records.
    """
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.select_one("table")
    if not table:
        LOGGER.warning("No table found for %s on %s", checkpoint, target_date)
        return []

    # Labels we map to required DB transport_type values.
    transport_map = {
        "легковые": "cars",
        "грузовые": "trucks",
        "автобусы": "buses",
    }

    rows = table.select("tr")
    values = {"cars": 0, "trucks": 0, "buses": 0}

    for row in rows:
        row_text = row.get_text(" ", strip=True).lower()
        for source_label, normalized_type in transport_map.items():
            if source_label in row_text:
                values[normalized_type] = _safe_extract_int(row_text)

    scraped_at = datetime.now().isoformat()
    date_iso = target_date.isoformat()
    return [
        {
            "checkpoint": checkpoint,
            "date": date_iso,
            "transport_type": "cars",
            "queue_length": values["cars"],
            "scraped_at": scraped_at,
        },
        {
            "checkpoint": checkpoint,
            "date": date_iso,
            "transport_type": "trucks",
            "queue_length": values["trucks"],
            "scraped_at": scraped_at,
        },
        {
            "checkpoint": checkpoint,
            "date": date_iso,
            "transport_type": "buses",
            "queue_length": values["buses"],
            "scraped_at": scraped_at,
        },
    ]


def scrape_archive_last_days(days: int = 60, headless: bool = True) -> int:
    """
    Loop through checkpoints and last N days, then store parsed records.
    """
    init_db()
    total_inserted = 0
    driver = _build_driver(headless=headless)

    try:
        driver.get(ARCHIVE_URL)
        WebDriverWait(driver, 30).until(ec.presence_of_element_located((By.TAG_NAME, "body")))

        for checkpoint in CHECKPOINTS:
            LOGGER.info("Processing checkpoint: %s", checkpoint)
            for offset in range(days):
                target_date = date.today() - timedelta(days=offset)
                try:
                    _select_checkpoint(driver, checkpoint)
                    _set_date(driver, target_date)

                    # Wait a bit for AJAX table refresh.
                    time.sleep(1.5)

                    records = _parse_archive_table(driver, checkpoint, target_date)
                    inserted = insert_archive_records(records)
                    total_inserted += inserted
                    LOGGER.info(
                        "Saved %s archive records for %s on %s",
                        inserted,
                        checkpoint,
                        target_date.isoformat(),
                    )
                except Exception as error:  # noqa: BLE001
                    LOGGER.exception(
                        "Archive scrape failed for %s on %s: %s",
                        checkpoint,
                        target_date.isoformat(),
                        error,
                    )

        return total_inserted
    finally:
        driver.quit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    inserted_count = scrape_archive_last_days(days=60, headless=True)
    print(f"Archive scraping finished. New rows inserted: {inserted_count}")
