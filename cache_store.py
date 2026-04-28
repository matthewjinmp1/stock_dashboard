import datetime
import json
import os
import sqlite3


def init_cache_db(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ticker_cache (
            ticker TEXT PRIMARY KEY,
            data_date TEXT NOT NULL,
            pulled_at TEXT,
            payload_version INTEGER,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ticker_cache_data_date ON ticker_cache(data_date)"
    )


def load_legacy_cache(legacy_cache_file):
    if os.path.exists(legacy_cache_file):
        try:
            with open(legacy_cache_file, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def migrate_legacy_cache_if_needed(conn, legacy_cache_file):
    try:
        row = conn.execute("SELECT COUNT(*) FROM ticker_cache").fetchone()
        if row and row[0]:
            return
        legacy_cache = load_legacy_cache(legacy_cache_file)
        if legacy_cache:
            write_cache_rows(conn, legacy_cache)
    except Exception:
        pass


def write_cache_rows(conn, cache_data):
    now = datetime.datetime.now().isoformat()
    rows = []
    for ticker, entry in (cache_data or {}).items():
        if not isinstance(entry, dict):
            continue
        payload = entry.get("data", {})
        rows.append(
            (
                str(ticker).upper(),
                entry.get("date") or datetime.date.today().isoformat(),
                entry.get("pulledAt"),
                payload.get("payloadVersion") if isinstance(payload, dict) else None,
                json.dumps(payload),
                now,
            )
        )

    conn.executemany(
        """
        INSERT OR REPLACE INTO ticker_cache
            (ticker, data_date, pulled_at, payload_version, payload_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def load_cache(cache_db_file, legacy_cache_file):
    try:
        with sqlite3.connect(cache_db_file) as conn:
            init_cache_db(conn)
            migrate_legacy_cache_if_needed(conn, legacy_cache_file)
            cache = {}
            for ticker, data_date, pulled_at, payload_json in conn.execute(
                """
                SELECT ticker, data_date, pulled_at, payload_json
                FROM ticker_cache
                ORDER BY ticker
                """
            ):
                try:
                    payload = json.loads(payload_json)
                except Exception:
                    payload = {}
                cache[ticker] = {
                    "date": data_date,
                    "pulledAt": pulled_at,
                    "data": payload,
                }
            return cache
    except Exception as exc:
        print(f"Cache DB read failed: {exc}")
        return {}


def save_cache(cache_db_file, cache_data):
    try:
        with sqlite3.connect(cache_db_file) as conn:
            init_cache_db(conn)
            conn.execute("DELETE FROM ticker_cache")
            write_cache_rows(conn, cache_data)
    except Exception as exc:
        print(f"Cache DB write failed: {exc}")
