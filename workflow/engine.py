"""工作流引擎：创建运行、领取尝试、回报结果、查看状态。"""
from __future__ import annotations

import json
import sqlite3
import uuid

from . import store
from .model import (
    ATTEMPT_TTL_SECONDS,
    MAX_ATTEMPTS,
    RETRY_BACKOFF_SECONDS,
    Claim,
    Dag,
    RunState,
    RunStatus,
    StepKind,
    StepSpec,
    StepState,
    StepStatus,
)

_TERMINAL_STEP_STATES = (StepState.SUCCEEDED.value, StepState.CANCELLED.value)


def _dag_to_json(dag: Dag) -> str:
    return json.dumps({
        "steps": [
            {
                "step_id": s.step_id,
                "kind": s.kind.value,
                "predecessors": list(s.predecessors),
                "delay_seconds": s.delay_seconds,
            }
            for s in dag.steps
        ]
    })


def _dag_from_json(payload: str) -> Dag:
    data = json.loads(payload)
    return Dag(steps=tuple(
        StepSpec(
            step_id=s["step_id"],
            kind=StepKind(s["kind"]),
            predecessors=tuple(s["predecessors"]),
            delay_seconds=s["delay_seconds"],
        )
        for s in data["steps"]
    ))


class Engine:
    def __init__(self, db_path: str) -> None:
        self._conn = store.connect(db_path)

    def close(self) -> None:
        self._conn.close()

    # ---------- 对外操作 ----------

    def create_run(self, dag: Dag, now: int) -> str:
        run_id = uuid.uuid4().hex
        with self._tx():
            self._conn.execute(
                "INSERT INTO runs (run_id, dag_json, state, last_time) VALUES (?,?,?,?)",
                (run_id, _dag_to_json(dag), RunState.RUNNING.value, now),
            )
            for spec in dag.steps:
                is_root = not spec.predecessors
                self._conn.execute(
                    "INSERT INTO steps (run_id, step_id, kind, state, ready_at)"
                    " VALUES (?,?,?,?,?)",
                    (
                        run_id,
                        spec.step_id,
                        spec.kind.value,
                        StepState.READY.value if is_root else StepState.PENDING.value,
                        now if is_root else None,
                    ),
                )
            self._advance(run_id, now)
        return run_id

    def claim(self, run_id: str, now: int) -> list[Claim]:
        with self._tx():
            self._begin_mutation(run_id, now)
            run = self._get_run(run_id)
            claims: list[Claim] = []
            if run["state"] == RunState.RUNNING.value:
                rows = self._conn.execute(
                    "SELECT * FROM steps WHERE run_id=? AND kind=? AND state=?",
                    (run_id, StepKind.EXEC.value, StepState.READY.value),
                ).fetchall()
                for row in rows:
                    if row["ready_at"] is None or row["ready_at"] > now:
                        continue
                    if row["next_retry_at"] is not None and row["next_retry_at"] > now:
                        continue
                    attempt_no = row["attempts"] + 1
                    attempt_id = uuid.uuid4().hex
                    deadline = now + ATTEMPT_TTL_SECONDS
                    self._conn.execute(
                        "INSERT INTO attempts"
                        " (attempt_id, run_id, step_id, attempt_no, claimed_at, deadline)"
                        " VALUES (?,?,?,?,?,?)",
                        (attempt_id, run_id, row["step_id"], attempt_no, now, deadline),
                    )
                    self._conn.execute(
                        "UPDATE steps SET attempts=?, state=? WHERE run_id=? AND step_id=?",
                        (attempt_no, StepState.RUNNING.value, run_id, row["step_id"]),
                    )
                    claims.append(Claim(
                        run_id=run_id,
                        step_id=row["step_id"],
                        attempt_id=attempt_id,
                        attempt_no=attempt_no,
                        deadline=deadline,
                    ))
            self._finish_mutation(run_id, now)
            return claims

    def report(self, run_id: str, attempt_id: str, succeeded: bool,
               output: str | None, now: int) -> None:
        with self._tx():
            self._begin_mutation(run_id, now)
            run = self._get_run(run_id)
            attempt = self._conn.execute(
                "SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)
            ).fetchone()
            valid = (
                attempt is not None
                and attempt["run_id"] == run_id
                and attempt["outcome"] is None
                and run["state"] == RunState.RUNNING.value
                and now < attempt["deadline"]
            )
            if valid:
                if succeeded:
                    self._conn.execute(
                        "UPDATE attempts SET outcome='success', resolved_at=?"
                        " WHERE attempt_id=?",
                        (now, attempt_id),
                    )
                    self._conn.execute(
                        "UPDATE steps SET state=?, succeeded_at=?, output=?"
                        " WHERE run_id=? AND step_id=?",
                        (StepState.SUCCEEDED.value, now, output,
                         run_id, attempt["step_id"]),
                    )
                    self._activate_successors(run_id, attempt["step_id"])
                    self._check_run_succeeded(run_id)
                else:
                    self._conn.execute(
                        "UPDATE attempts SET outcome='failed', resolved_at=?"
                        " WHERE attempt_id=?",
                        (now, attempt_id),
                    )
                    self._fail_step_attempt(run_id, attempt["step_id"],
                                            attempt["attempt_no"], now)
            self._finish_mutation(run_id, now)

    def status(self, run_id: str) -> RunStatus:
        run = self._get_run(run_id)
        rows = self._conn.execute(
            "SELECT * FROM steps WHERE run_id=? ORDER BY rowid", (run_id,)
        ).fetchall()
        return RunStatus(
            run_id=run_id,
            state=RunState(run["state"]),
            steps=tuple(
                StepStatus(
                    step_id=r["step_id"],
                    kind=StepKind(r["kind"]),
                    state=StepState(r["state"]),
                    attempts=r["attempts"],
                    ready_at=r["ready_at"],
                    succeeded_at=r["succeeded_at"],
                    output=r["output"],
                )
                for r in rows
            ),
        )

    # ---------- 内部：事务与时间 ----------

    def _tx(self):
        return _Transaction(self._conn)

    def _get_run(self, run_id: str) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT * FROM runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown run_id: {run_id}")
        return row

    def _begin_mutation(self, run_id: str, now: int) -> None:
        run = self._get_run(run_id)
        if now < run["last_time"]:
            raise ValueError(
                f"time went backwards for run {run_id}: {now} < {run['last_time']}"
            )
        self._advance(run_id, now)

    def _finish_mutation(self, run_id: str, now: int) -> None:
        self._conn.execute(
            "UPDATE runs SET last_time=? WHERE run_id=?", (now, run_id)
        )

    # ---------- 内部：状态推进 ----------

    def _advance(self, run_id: str, now: int) -> None:
        """按事件应发生的时刻，把 <= now 的到期事件逐个应用。"""
        while True:
            run = self._get_run(run_id)
            if run["state"] != RunState.RUNNING.value:
                return
            due_delay = None
            delay_rows = self._conn.execute(
                "SELECT * FROM steps WHERE run_id=? AND kind=? AND state=?",
                (run_id, StepKind.DELAY.value, StepState.READY.value),
            ).fetchall()
            dag = _dag_from_json(run["dag_json"])
            delays = {s.step_id: s.delay_seconds for s in dag.steps}
            for r in delay_rows:
                due_at = r["ready_at"] + delays[r["step_id"]]
                if due_at <= now and (due_delay is None or due_at < due_delay[0]):
                    due_delay = (due_at, r)
            timeout_row = self._conn.execute(
                "SELECT * FROM attempts"
                " WHERE run_id=? AND outcome IS NULL AND deadline<=?"
                " ORDER BY deadline LIMIT 1",
                (run_id, now),
            ).fetchone()
            if due_delay is None and timeout_row is None:
                return
            if due_delay is not None and (
                timeout_row is None or due_delay[0] <= timeout_row["deadline"]
            ):
                due_at, row = due_delay
                self._conn.execute(
                    "UPDATE steps SET state=?, succeeded_at=?"
                    " WHERE run_id=? AND step_id=?",
                    (StepState.SUCCEEDED.value, due_at, run_id, row["step_id"]),
                )
                self._activate_successors(run_id, row["step_id"])
                self._check_run_succeeded(run_id)
            else:
                self._conn.execute(
                    "UPDATE attempts SET outcome='timeout', resolved_at=?"
                    " WHERE attempt_id=?",
                    (timeout_row["deadline"], timeout_row["attempt_id"]),
                )
                self._fail_step_attempt(run_id, timeout_row["step_id"],
                                        timeout_row["attempt_no"],
                                        timeout_row["deadline"])

    def _activate_successors(self, run_id: str, step_id: str) -> None:
        run = self._get_run(run_id)
        dag = _dag_from_json(run["dag_json"])
        for spec in dag.steps:
            if step_id not in spec.predecessors:
                continue
            row = self._conn.execute(
                "SELECT * FROM steps WHERE run_id=? AND step_id=?",
                (run_id, spec.step_id),
            ).fetchone()
            if row["state"] != StepState.PENDING.value:
                continue
            pred_times = []
            for pred in spec.predecessors:
                pred_row = self._conn.execute(
                    "SELECT succeeded_at FROM steps WHERE run_id=? AND step_id=?",
                    (run_id, pred),
                ).fetchone()
                if pred_row["succeeded_at"] is None:
                    pred_times = None
                    break
                pred_times.append(pred_row["succeeded_at"])
            if pred_times is None:
                continue
            ready_at = max(pred_times) if pred_times else None
            self._conn.execute(
                "UPDATE steps SET state=?, ready_at=? WHERE run_id=? AND step_id=?",
                (StepState.READY.value, ready_at, run_id, spec.step_id),
            )

    def _fail_step_attempt(self, run_id: str, step_id: str,
                           attempt_no: int, failed_at: int) -> None:
        if attempt_no >= MAX_ATTEMPTS:
            self._fail_run(run_id)
            return
        backoff = RETRY_BACKOFF_SECONDS[attempt_no - 1]
        self._conn.execute(
            "UPDATE steps SET state=?, failed_at=?, next_retry_at=?"
            " WHERE run_id=? AND step_id=?",
            (StepState.READY.value, failed_at, failed_at + backoff,
             run_id, step_id),
        )

    def _fail_run(self, run_id: str) -> None:
        self._conn.execute(
            "UPDATE runs SET state=? WHERE run_id=?",
            (RunState.FAILED.value, run_id),
        )
        self._conn.execute(
            "UPDATE steps SET state=? WHERE run_id=? AND state NOT IN (?,?)",
            (StepState.CANCELLED.value, run_id, *_TERMINAL_STEP_STATES),
        )
        self._conn.execute(
            "UPDATE attempts SET outcome='cancelled', resolved_at=NULL"
            " WHERE run_id=? AND outcome IS NULL",
            (run_id,),
        )

    def _check_run_succeeded(self, run_id: str) -> None:
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM steps WHERE run_id=? AND state!=?",
            (run_id, StepState.SUCCEEDED.value),
        ).fetchone()
        if row["n"] == 0:
            self._conn.execute(
                "UPDATE runs SET state=? WHERE run_id=?",
                (RunState.SUCCEEDED.value, run_id),
            )


class _Transaction:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def __enter__(self):
        self._conn.execute("BEGIN IMMEDIATE")
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self._conn.execute("COMMIT")
        else:
            self._conn.execute("ROLLBACK")
        return False
