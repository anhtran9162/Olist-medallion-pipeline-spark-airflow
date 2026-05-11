"""Database connection for the Olist SQLite API."""

import sqlite3
from pathlib import Path

DB_PATH = Path("/app/data/olist_api.db")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn
