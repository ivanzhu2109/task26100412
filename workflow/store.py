"""SQLite 持久化层：schema 与连接。"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    dag_json TEXT NOT NULL,
    state TEXT NOT NULL,
    last_time INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS steps (
    run_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    ready_at INTEGER,
    succeeded_at INTEGER,
    failed_at INTEGER,
    next_retry_at INTEGER,
    output TEXT,
    PRIMARY KEY (run_id, step_id)
);
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    claimed_at INTEGER NOT NULL,
    deadline INTEGER NOT NULL,
    outcome TEXT,
    resolved_at INTEGER
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    return conn
