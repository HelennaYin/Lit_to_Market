"""SQLite connection and schema initialization helpers."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "litmarket.db"
SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def get_db_path() -> Path:
    """Return the configured database path."""
    return Path(os.environ.get("LITMARKET_DB_PATH", DEFAULT_DB_PATH))


def get_connection(db_path: str | os.PathLike[str] | None = None) -> sqlite3.Connection:
    """Open a SQLite connection with row dictionaries and FK checks enabled."""
    path = Path(db_path) if db_path is not None else get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(db_path: str | os.PathLike[str] | None = None) -> Path:
    """Create or update the SQLite database schema."""
    path = Path(db_path) if db_path is not None else get_db_path()
    schema = SCHEMA_PATH.read_text(encoding="utf-8")

    with get_connection(path) as conn:
        conn.executescript(schema)

    return path


def list_tables(db_path: str | os.PathLike[str] | None = None) -> list[str]:
    """Return user-created table names, mainly for verification."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
    return [row["name"] for row in rows]


if __name__ == "__main__":
    created_path = init_db()
    print(f"Initialized database: {created_path}")
    print("Tables:")
    for table in list_tables(created_path):
        print(f"  - {table}")
