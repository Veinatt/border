import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from utils.paths import DATA_DIR, DB_PATH

# Increment when schema or one-time migration logic changes.
SCHEMA_USER_VERSION = 2


def get_connection() -> sqlite3.Connection:
    """
    Return a SQLite connection with dict-like row access.
    """
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def _create_unified_daily_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS unified_daily_queue (
            checkpoint TEXT NOT NULL,
            day TEXT NOT NULL,
            archive_cars INTEGER,
            archive_trucks INTEGER,
            archive_buses INTEGER,
            last_archive_scraped_at TEXT,
            live_cars INTEGER,
            live_trucks INTEGER,
            live_buses INTEGER,
            last_live_timestamp TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (checkpoint, day)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_unified_checkpoint_day "
        "ON unified_daily_queue (checkpoint, day)"
    )


def _create_unified_daily_view(cursor: sqlite3.Cursor, replace: bool) -> None:
    if replace:
        cursor.execute("DROP VIEW IF EXISTS v_unified_daily_effective")
    cursor.execute(
        """
        CREATE VIEW IF NOT EXISTS v_unified_daily_effective AS
        SELECT
            checkpoint,
            day,
            COALESCE(archive_cars, live_cars) AS effective_cars,
            COALESCE(archive_trucks, live_trucks) AS effective_trucks,
            COALESCE(archive_buses, live_buses) AS effective_buses
        FROM unified_daily_queue
        """
    )


def _backfill_unified_from_archive(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT INTO unified_daily_queue (
            checkpoint, day,
            archive_cars, archive_trucks, archive_buses,
            last_archive_scraped_at,
            live_cars, live_trucks, live_buses, last_live_timestamp,
            updated_at
        )
        SELECT
            checkpoint,
            date AS day,
            MAX(CASE WHEN transport_type = 'cars' THEN queue_length END),
            MAX(CASE WHEN transport_type = 'trucks' THEN queue_length END),
            MAX(CASE WHEN transport_type = 'buses' THEN queue_length END),
            MAX(scraped_at),
            NULL, NULL, NULL, NULL,
            datetime('now')
        FROM archive_queue
        GROUP BY checkpoint, date
        ON CONFLICT(checkpoint, day) DO UPDATE SET
            archive_cars = excluded.archive_cars,
            archive_trucks = excluded.archive_trucks,
            archive_buses = excluded.archive_buses,
            last_archive_scraped_at = excluded.last_archive_scraped_at,
            updated_at = excluded.updated_at
        """
    )


def _backfill_unified_from_current(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT INTO unified_daily_queue (
            checkpoint, day,
            archive_cars, archive_trucks, archive_buses, last_archive_scraped_at,
            live_cars, live_trucks, live_buses, last_live_timestamp,
            updated_at
        )
        SELECT
            t.checkpoint,
            substr(t.timestamp, 1, 10) AS day,
            NULL, NULL, NULL, NULL,
            t.cars_out, t.trucks_out, t.buses_out, t.timestamp,
            datetime('now')
        FROM current_queue AS t
        INNER JOIN (
            SELECT checkpoint, substr(timestamp, 1, 10) AS d, MAX(id) AS max_id
            FROM current_queue
            GROUP BY checkpoint, substr(timestamp, 1, 10)
        ) AS m ON t.id = m.max_id
        ON CONFLICT(checkpoint, day) DO UPDATE SET
            live_cars = excluded.live_cars,
            live_trucks = excluded.live_trucks,
            live_buses = excluded.live_buses,
            last_live_timestamp = excluded.last_live_timestamp,
            updated_at = excluded.updated_at
        """
    )


def _run_schema_migrations(connection: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    row = connection.execute("PRAGMA user_version").fetchone()
    current = int(row[0]) if row else 0
    if current >= SCHEMA_USER_VERSION:
        return
    _backfill_unified_from_archive(connection)
    _backfill_unified_from_current(connection)
    _create_unified_daily_view(cursor, replace=True)
    connection.execute(f"PRAGMA user_version = {SCHEMA_USER_VERSION}")


def _pivot_archive_row(
    connection: sqlite3.Connection, checkpoint: str, day: str
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT
            checkpoint,
            date AS day,
            MAX(CASE WHEN transport_type = 'cars' THEN queue_length END) AS ac,
            MAX(CASE WHEN transport_type = 'trucks' THEN queue_length END) AS at,
            MAX(CASE WHEN transport_type = 'buses' THEN queue_length END) AS ab,
            MAX(scraped_at) AS scraped_at
        FROM archive_queue
        WHERE checkpoint = ? AND date = ?
        GROUP BY checkpoint, date
        """,
        (checkpoint, day),
    ).fetchone()


def _sync_unified_from_archive_keys(
    connection: sqlite3.Connection, keys: Iterable[tuple[str, str]]
) -> None:
    seen: set[tuple[str, str]] = set()
    for checkpoint, day in keys:
        if (checkpoint, day) in seen:
            continue
        seen.add((checkpoint, day))
        row = _pivot_archive_row(connection, checkpoint, day)
        if not row or (
            row["ac"] is None and row["at"] is None and row["ab"] is None
        ):
            continue
        connection.execute(
            """
            INSERT INTO unified_daily_queue (
                checkpoint, day,
                archive_cars, archive_trucks, archive_buses,
                last_archive_scraped_at,
                live_cars, live_trucks, live_buses, last_live_timestamp,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, datetime('now'))
            ON CONFLICT(checkpoint, day) DO UPDATE SET
                archive_cars = excluded.archive_cars,
                archive_trucks = excluded.archive_trucks,
                archive_buses = excluded.archive_buses,
                last_archive_scraped_at = excluded.last_archive_scraped_at,
                updated_at = excluded.updated_at
            """,
            (
                checkpoint,
                day,
                row["ac"],
                row["at"],
                row["ab"],
                row["scraped_at"],
            ),
        )


def _sync_unified_from_live_batch(
    connection: sqlite3.Connection, records: list[dict]
) -> None:
    now = datetime.now().isoformat()
    for item in records:
        checkpoint = item["checkpoint"]
        ts = item["timestamp"]
        day = ts[:10] if len(ts) >= 10 else datetime.now().date().isoformat()
        connection.execute(
            """
            INSERT INTO unified_daily_queue (
                checkpoint, day,
                archive_cars, archive_trucks, archive_buses, last_archive_scraped_at,
                live_cars, live_trucks, live_buses, last_live_timestamp,
                updated_at
            )
            VALUES (?, ?, NULL, NULL, NULL, NULL, ?, ?, ?, ?, ?)
            ON CONFLICT(checkpoint, day) DO UPDATE SET
                live_cars = excluded.live_cars,
                live_trucks = excluded.live_trucks,
                live_buses = excluded.live_buses,
                last_live_timestamp = excluded.last_live_timestamp,
                updated_at = excluded.updated_at
            """,
            (
                checkpoint,
                day,
                int(item.get("cars_out", 0)),
                int(item.get("trucks_out", 0)),
                int(item.get("buses_out", 0)),
                ts,
                now,
            ),
        )


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

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_current_checkpoint_time "
            "ON current_queue (checkpoint, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_archive_checkpoint_date "
            "ON archive_queue (checkpoint, date)"
        )

        _create_unified_daily_table(cursor)
        _run_schema_migrations(connection, cursor)
        _create_unified_daily_view(cursor, replace=False)

        connection.commit()


def insert_current_queue_records(records: Iterable[dict]) -> int:
    """
    Insert a batch of current queue records.
    Returns number of inserted rows.
    """
    record_list = list(records)
    rows = [
        (
            item["checkpoint"],
            int(item.get("cars_out", 0)),
            int(item.get("trucks_out", 0)),
            int(item.get("buses_out", 0)),
            item["timestamp"],
        )
        for item in record_list
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
        _sync_unified_from_live_batch(connection, record_list)
        connection.commit()
    return len(rows)


def insert_archive_records(records: Iterable[dict]) -> int:
    """
    Insert archive records with duplicate protection.
    Uses INSERT OR IGNORE because of UNIQUE constraint.
    Returns amount of newly inserted rows.
    """
    record_list = list(records)
    rows = [
        (
            item["checkpoint"],
            item["date"],
            item["transport_type"],
            int(item["queue_length"]),
            item["scraped_at"],
        )
        for item in record_list
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
        keys = {(item["checkpoint"], item["date"]) for item in record_list}
        _sync_unified_from_archive_keys(connection, keys)
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
    Return average effective daily queue per transport type over N days
    (archive when present, otherwise live snapshots), from unified view.
    """
    date_from = (datetime.now() - timedelta(days=days)).date().isoformat()
    with closing(get_connection()) as connection:
        rows = connection.execute(
            """
            SELECT transport_type, ROUND(AVG(queue_len), 2) AS avg_queue
            FROM (
                SELECT 'buses' AS transport_type, effective_buses AS queue_len
                FROM v_unified_daily_effective
                WHERE checkpoint = ? AND day >= ?
                  AND effective_buses IS NOT NULL
                UNION ALL
                SELECT 'cars' AS transport_type, effective_cars AS queue_len
                FROM v_unified_daily_effective
                WHERE checkpoint = ? AND day >= ?
                  AND effective_cars IS NOT NULL
                UNION ALL
                SELECT 'trucks' AS transport_type, effective_trucks AS queue_len
                FROM v_unified_daily_effective
                WHERE checkpoint = ? AND day >= ?
                  AND effective_trucks IS NOT NULL
            ) AS u
            GROUP BY transport_type
            ORDER BY transport_type
            """,
            (checkpoint, date_from, checkpoint, date_from, checkpoint, date_from),
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


def get_current_queue_range(
    checkpoint: str,
    start_date: str,
    end_date: str,
    time_from: str | None = None,
    time_to: str | None = None,
) -> list[sqlite3.Row]:
    """
    Return current_queue snapshots for a checkpoint within inclusive calendar dates.

    ``start_date`` / ``end_date`` must be ``YYYY-MM-DD`` (ISO calendar date).
    Stored ``timestamp`` values are assumed to be ISO-like strings where
    ``substr(timestamp, 1, 10)`` is the calendar date and ``substr(timestamp, 12, 5)``
    is ``HH:MM`` (naive local times as produced by the scraper).

    When ``time_from`` and ``time_to`` are set (``HH:MM``), only rows whose
    time-of-day falls in that inclusive window are returned (string compare).
    """
    sql = """
        SELECT timestamp, cars_out, trucks_out, buses_out
        FROM current_queue
        WHERE checkpoint = ?
          AND substr(timestamp, 1, 10) >= ?
          AND substr(timestamp, 1, 10) <= ?
    """
    params: list[str] = [checkpoint, start_date, end_date]
    if time_from is not None and time_to is not None:
        sql += """
          AND substr(timestamp, 12, 5) >= ?
          AND substr(timestamp, 12, 5) <= ?
        """
        params.extend([time_from, time_to])
    sql += " ORDER BY timestamp ASC"
    with closing(get_connection()) as connection:
        rows = connection.execute(sql, params).fetchall()
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
