import logging
from pathlib import Path

from utils.paths import LOGS_DIR


def setup_logging(log_file_name: str = "app.log") -> None:
    """
    Configure shared logging for all scripts.
    Logs are written both to console and to logs/<log_file_name>.
    """
    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
    log_file_path = LOGS_DIR / log_file_name

    # Clear existing handlers to avoid duplicated logs
    # when setup_logging is called more than once.
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.handlers.clear()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
