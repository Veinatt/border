from pathlib import Path

# Absolute path to the project root directory.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Folder where SQLite database file is stored.
DATA_DIR = PROJECT_ROOT / "data"

# Folder where log files are stored.
LOGS_DIR = PROJECT_ROOT / "logs"

# Main SQLite file path.
DB_PATH = DATA_DIR / "border_queue.db"
