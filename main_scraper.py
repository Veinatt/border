import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from db_manager import init_db
from scrapers.current_scraper import scrape_and_store_current_queue
from utils.logger import setup_logging

LOGGER = logging.getLogger(__name__)


async def run_current_scrape_job() -> None:
    """
    Async wrapper for current queue scrape job.
    """
    try:
        scrape_and_store_current_queue()
    except Exception as error:  # noqa: BLE001
        LOGGER.exception("Current scrape job failed: %s", error)


async def main() -> None:
    """
    Initialize DB, run one scrape immediately,
    then continue scraping every 2 hours.
    """
    setup_logging("scraper.log")
    init_db()

    LOGGER.info("Running initial current queue scrape...")
    await run_current_scrape_job()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_current_scrape_job,
        trigger=IntervalTrigger(hours=2),
        id="current_queue_every_2_hours",
        replace_existing=True,
    )
    scheduler.start()
    LOGGER.info("Scheduler started. Current queue scrape interval: every 2 hours.")

    # Keep process alive forever.
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
