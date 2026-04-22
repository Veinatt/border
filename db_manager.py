import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from utils.paths import DATA_DIR, DB_PATH


def get_connection() -> sqlite3.Connection:
    """
    Return a SQLite connection with dict-like row access.
    """
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    """
    Create required tables and indexes if they do not exist.
    """
    with closing(get_connection()) as connection:
        cursor = connection.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS current_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                checkpoint TEXT NOT NULL,
                cars_out INTEGER NOT NULL DEFAULT 0,
                trucks_out INTEGER NOT NULL DEFAULT 0,
                buses_out INTEGER NOT NULL DEFAULT 0,
                timestamp TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS archive_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                checkpoint TEXT NOT NULL,
                date TEXT NOT NULL,
                transport_type TEXT NOT NULL,
                queue_length INTEGER NOT NULL,
                scraped_at TEXT NOT NULL,
                UNIQUE (checkpoint, date, transport_type)
            )
            """
        )

        # Indexes speed up frequently used bot queries.
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_current_checkpoint_time "
            "ON current_queue (checkpoint, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_archive_checkpoint_date "
            "ON archive_queue (checkpoint, date)"
        )

        connection.commit()


def insert_current_queue_records(records: Iterable[dict]) -> int:
    """
    Insert a batch of current queue records.
    Returns number of inserted rows.
    """
    rows = [
        (
            item["checkpoint"],
            int(item.get("cars_out", 0)),
            int(item.get("trucks_out", 0)),
            int(item.get("buses_out", 0)),
            item["timestamp"],
        )
        for item in records
    ]
    if not rows:
        return 0

    with closing(get_connection()) as connection:
        connection.executemany(
            """
            INSERT INTO current_queue (
                checkpoint, cars_out, trucks_out, buses_out, timestamp
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.commit()
    return len(rows)


def insert_archive_records(records: Iterable[dict]) -> int:
    """
    Insert archive records with duplicate protection.
    Uses INSERT OR IGNORE because of UNIQUE constraint.
    Returns amount of newly inserted rows.
    """
    rows = [
        (
            item["checkpoint"],
            item["date"],
            item["transport_type"],
            int(item["queue_length"]),
            item["scraped_at"],
        )
        for item in records
    ]
    if not rows:
        return 0

    with closing(get_connection()) as connection:
        cursor = connection.cursor()
        cursor.executemany(
            """
            INSERT OR IGNORE INTO archive_queue (
                checkpoint, date, transport_type, queue_length, scraped_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.commit()
        return cursor.rowcount if cursor.rowcount != -1 else 0


def get_latest_current_timestamp() -> str | None:
    with closing(get_connection()) as connection:
        row = connection.execute(
            "SELECT MAX(timestamp) AS latest FROM current_queue"
        ).fetchone()
    return row["latest"] if row and row["latest"] else None


def get_latest_current_snapshot() -> list[sqlite3.Row]:
    """
    Return all checkpoints from the most recent scrape timestamp.
    """
    latest = get_latest_current_timestamp()
    if not latest:
        return []

    with closing(get_connection()) as connection:
        rows = connection.execute(
            """
            SELECT checkpoint, cars_out, trucks_out, buses_out, timestamp
            FROM current_queue
            WHERE timestamp = ?
            ORDER BY cars_out DESC, checkpoint ASC
            """,
            (latest,),
        ).fetchall()
    return rows


def get_archive_average(checkpoint: str, days: int = 7) -> list[sqlite3.Row]:
    """
    Return average queue length per transport type for checkpoint over N days.
    """
    date_from = (datetime.now() - timedelta(days=days)).date().isoformat()
    with closing(get_connection()) as connection:
        rows = connection.execute(
            """
            SELECT transport_type, ROUND(AVG(queue_length), 2) AS avg_queue
            FROM archive_queue
            WHERE checkpoint = ? AND date >= ?
            GROUP BY transport_type
            ORDER BY transport_type
            """,
            (checkpoint, date_from),
        ).fetchall()
    return rows


def get_current_trend(checkpoint: str, days: int = 7) -> list[sqlite3.Row]:
    """
    Return time series from current_queue for chart building.
    """
    date_from = (datetime.now() - timedelta(days=days)).isoformat()
    with closing(get_connection()) as connection:
        rows = connection.execute(
            """
            SELECT timestamp, cars_out, trucks_out, buses_out
            FROM current_queue
            WHERE checkpoint = ? AND timestamp >= ?
            ORDER BY timestamp ASC
            """,
            (checkpoint, date_from),
        ).fetchall()
    return rows


def get_daily_top3_from_latest() -> list[sqlite3.Row]:
    """
    Return top 3 checkpoints by cars_out for the latest timestamp.
    """
    latest = get_latest_current_timestamp()
    if not latest:
        return []

    with closing(get_connection()) as connection:
        rows = connection.execute(
            """
            SELECT checkpoint, cars_out, trucks_out, buses_out, timestamp
            FROM current_queue
            WHERE timestamp = ?
            ORDER BY cars_out DESC
            LIMIT 3
            """,
            (latest,),
        ).fetchall()
    return rows


if __name__ == "__main__":
    init_db()
    print(f"Database initialized: {DB_PATH}")
