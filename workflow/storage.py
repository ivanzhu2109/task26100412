"""SQLite 持久化层。"""
from __future__ import annotations

import json
import sqlite3

from .models import Dag, StepKind, StepSpec

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    dag_json    TEXT NOT NULL,
    status      TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    last_time   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS steps (
    run_id            INTEGER NOT NULL,
    step_id           TEXT NOT NULL,
    kind              TEXT NOT NULL,
    delay_seconds     INTEGER NOT NULL,
    deps_json         TEXT NOT NULL,
    status            TEXT NOT NULL,
    ready_at          INTEGER,
    due_at            INTEGER,
    succeeded_at      INTEGER,
    failed_at         INTEGER,
    output            TEXT,
    attempts          INTEGER NOT NULL DEFAULT 0,
    next_eligible_at  INTEGER,
    PRIMARY KEY (run_id, step_id)
);
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       INTEGER NOT NULL,
    step_id      TEXT NOT NULL,
    attempt_no   INTEGER NOT NULL,
    claimed_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL,
    status       TEXT NOT NULL,
    resolved_at  INTEGER
);
"""


def dag_to_json(dag: Dag) -> str:
    return json.dumps(
        [
            {
                "step_id": s.step_id,
                "kind": s.kind.value,
                "deps": list(s.deps),
                "delay_seconds": s.delay_seconds,
            }
            for s in dag.steps
        ]
    )


def dag_from_json(text: str) -> Dag:
    return Dag(
        steps=tuple(
            StepSpec(
                step_id=item["step_id"],
                kind=StepKind(item["kind"]),
                deps=tuple(item["deps"]),
                delay_seconds=item["delay_seconds"],
            )
            for item in json.loads(text)
        )
    )


class Storage:
    """持有 SQLite 连接，负责建表。具体读写由引擎完成。"""

    def __init__(self, db_path: str) -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        with self.conn:
            self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()
