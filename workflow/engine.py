"""工作流引擎：创建运行、领取尝试、回报结果、查看状态。

时间语义：
- 每个变更调用携带不倒退的整数秒时间 now。
- 处理操作前，先把该运行中应发生的事件（尝试超时、延迟步骤到期）
  按各自原定时刻推进，再处理本次操作。
- 尝试有效期为领取时刻起 30 秒；截止时刻先发生超时，
  即 expires_at <= now 时尝试已失效。
"""
from __future__ import annotations

import json
from typing import Optional

from .models import (
    ATTEMPT_TTL_SECONDS,
    MAX_ATTEMPTS,
    RETRY_BACKOFFS,
    AttemptStatus,
    ClaimedAttempt,
    Dag,
    RunStatus,
    RunView,
    StepKind,
    StepStatus,
    StepView,
)
from .storage import Storage, dag_from_json, dag_to_json

_TERMINAL_STEP = (StepStatus.SUCCEEDED.value, StepStatus.CANCELLED.value)


class Engine:
    def __init__(self, db_path: str) -> None:
        self._storage = Storage(db_path)

    def close(self) -> None:
        self._storage.close()

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # 内部 helpers
    # ------------------------------------------------------------------
    @property
    def _conn(self):
        return self._storage.conn

    def _load_run(self, run_id: int):
        row = self._conn.execute(
            "SELECT * FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"run {run_id} not found")
        return row

    def _check_time(self, run, now: int) -> None:
        if now < run["last_time"]:
            raise ValueError(
                f"time went backwards for run {run['run_id']}: "
                f"{now} < {run['last_time']}"
            )

    def _fail_run(self, run_id: int, at: int) -> None:
        """第 6 次尝试失败后：运行失败，取消未完成步骤与活动尝试。"""
        self._conn.execute(
            "UPDATE runs SET status = ? WHERE run_id = ?",
            (RunStatus.FAILED.value, run_id),
        )
        self._conn.execute(
            "UPDATE steps SET status = ? WHERE run_id = ? AND status NOT IN (?, ?)",
            (StepStatus.CANCELLED.value, run_id, *_TERMINAL_STEP),
        )
        self._conn.execute(
            "UPDATE attempts SET status = ?, resolved_at = ? "
            "WHERE run_id = ? AND status = ?",
            (AttemptStatus.CANCELLED.value, at, run_id, AttemptStatus.ACTIVE.value),
        )

    def _expire_attempts(self, run_id: int, now: int) -> None:
        """把 expires_at <= now 的活动尝试判为超时（失败时刻取原截止时刻）。"""
        rows = self._conn.execute(
            "SELECT * FROM attempts WHERE run_id = ? AND status = ? AND expires_at <= ?",
            (run_id, AttemptStatus.ACTIVE.value, now),
        ).fetchall()
        for att in rows:
            self._conn.execute(
                "UPDATE attempts SET status = ?, resolved_at = ? WHERE attempt_id = ?",
                (AttemptStatus.FAILED_TIMEOUT.value, att["expires_at"], att["attempt_id"]),
            )
            step = self._conn.execute(
                "SELECT * FROM steps WHERE run_id = ? AND step_id = ?",
                (run_id, att["step_id"]),
            ).fetchone()
            if step["status"] == StepStatus.CANCELLED.value:
                continue
            self._conn.execute(
                "UPDATE steps SET failed_at = ? WHERE run_id = ? AND step_id = ?",
                (att["expires_at"], run_id, att["step_id"]),
            )
            self._after_attempt_failure(run_id, step, att["expires_at"])

    def _after_attempt_failure(self, run_id: int, step, failed_at: int) -> None:
        """尝试失败后的公共处理：满 6 次则运行失败，否则按退避重新就绪。"""
        if step["attempts"] >= MAX_ATTEMPTS:
            self._fail_run(run_id, failed_at)
            return
        backoff = RETRY_BACKOFFS[step["attempts"] - 1]
        self._conn.execute(
            "UPDATE steps SET status = ?, next_eligible_at = ? "
            "WHERE run_id = ? AND step_id = ?",
            (
                StepStatus.READY.value,
                failed_at + backoff,
                run_id,
                step["step_id"],
            ),
        )

    def _propagate(self, run_id: int) -> bool:
        """让所有前驱均已成功的 PENDING 步骤就绪；返回是否有变化。"""
        changed = False
        pending = self._conn.execute(
            "SELECT * FROM steps WHERE run_id = ? AND status = ?",
            (run_id, StepStatus.PENDING.value),
        ).fetchall()
        for step in pending:
            deps = self._conn.execute(
                "SELECT status, succeeded_at FROM steps "
                "WHERE run_id = ? AND step_id IN "
                f"(SELECT json_each.value FROM json_each(?))",
                (run_id, step["deps_json"]),
            ).fetchall()
            if len(deps) == 0:
                continue
            if any(d["status"] != StepStatus.SUCCEEDED.value for d in deps):
                continue
            ready_at = max(d["succeeded_at"] for d in deps)
            if step["kind"] == StepKind.DELAY.value:
                self._conn.execute(
                    "UPDATE steps SET status = ?, ready_at = ?, due_at = ? "
                    "WHERE run_id = ? AND step_id = ?",
                    (
                        StepStatus.READY.value,
                        ready_at,
                        ready_at + step["delay_seconds"],
                        run_id,
                        step["step_id"],
                    ),
                )
            else:
                self._conn.execute(
                    "UPDATE steps SET status = ?, ready_at = ?, next_eligible_at = ? "
                    "WHERE run_id = ? AND step_id = ?",
                    (StepStatus.READY.value, ready_at, ready_at, run_id, step["step_id"]),
                )
            changed = True
        return changed

    def _complete_due_delays(self, run_id: int, now: int) -> bool:
        """到期的延迟步骤按原定到期时刻成功；返回是否有变化。"""
        rows = self._conn.execute(
            "SELECT * FROM steps WHERE run_id = ? AND kind = ? AND status = ? "
            "AND due_at <= ? ORDER BY due_at",
            (run_id, StepKind.DELAY.value, StepStatus.READY.value, now),
        ).fetchall()
        for step in rows:
            self._conn.execute(
                "UPDATE steps SET status = ?, succeeded_at = ? "
                "WHERE run_id = ? AND step_id = ?",
                (StepStatus.SUCCEEDED.value, step["due_at"], run_id, step["step_id"]),
            )
        return bool(rows)

    def _advance(self, run_id: int, now: int) -> None:
        """按事件原定时刻推进到期状态，直到不动点，再更新 last_time。"""
        run = self._load_run(run_id)
        if run["status"] == RunStatus.RUNNING.value:
            while True:
                self._expire_attempts(run_id, now)
                run = self._load_run(run_id)
                if run["status"] != RunStatus.RUNNING.value:
                    break
                progressed = self._complete_due_delays(run_id, now)
                progressed |= self._propagate(run_id)
                if not progressed:
                    break
            remaining = self._conn.execute(
                "SELECT COUNT(*) AS c FROM steps WHERE run_id = ? AND status != ?",
                (run_id, StepStatus.SUCCEEDED.value),
            ).fetchone()["c"]
            run = self._load_run(run_id)
            if run["status"] == RunStatus.RUNNING.value and remaining == 0:
                self._conn.execute(
                    "UPDATE runs SET status = ? WHERE run_id = ?",
                    (RunStatus.SUCCEEDED.value, run_id),
                )
        self._conn.execute(
            "UPDATE runs SET last_time = ? WHERE run_id = ?", (now, run_id)
        )

    # ------------------------------------------------------------------
    # 四类操作
    # ------------------------------------------------------------------
    def create_run(self, dag: Dag, now: int) -> int:
        """创建运行；根步骤立即就绪。返回 run_id。"""
        with self._conn:
            cur = self._conn.execute(
                "INSERT INTO runs (dag_json, status, created_at, last_time) "
                "VALUES (?, ?, ?, ?)",
                (dag_to_json(dag), RunStatus.RUNNING.value, now, now),
            )
            run_id = cur.lastrowid
            for spec in dag.steps:
                if spec.deps:
                    status, ready_at, due_at, eligible = (
                        StepStatus.PENDING.value, None, None, None,
                    )
                else:
                    status, ready_at = StepStatus.READY.value, now
                    if spec.kind is StepKind.DELAY:
                        due_at, eligible = now + spec.delay_seconds, None
                    else:
                        due_at, eligible = None, now
                self._conn.execute(
                    "INSERT INTO steps (run_id, step_id, kind, delay_seconds, "
                    "deps_json, status, ready_at, due_at, next_eligible_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        spec.step_id,
                        spec.kind.value,
                        spec.delay_seconds,
                        json.dumps(list(spec.deps)),
                        status,
                        ready_at,
                        due_at,
                        eligible,
                    ),
                )
            self._advance(run_id, now)
        return run_id

    def claim(self, run_id: int, now: int) -> list[ClaimedAttempt]:
        """推进到期状态后，为所有当前具备资格的执行步骤签发新尝试。"""
        with self._conn:
            run = self._load_run(run_id)
            self._check_time(run, now)
            self._advance(run_id, now)
            run = self._load_run(run_id)
            if run["status"] != RunStatus.RUNNING.value:
                return []
            rows = self._conn.execute(
                "SELECT * FROM steps WHERE run_id = ? AND kind = ? AND status = ? "
                "AND next_eligible_at <= ? ORDER BY step_id",
                (run_id, StepKind.EXEC.value, StepStatus.READY.value, now),
            ).fetchall()
            claimed: list[ClaimedAttempt] = []
            for step in rows:
                attempt_no = step["attempts"] + 1
                cur = self._conn.execute(
                    "INSERT INTO attempts (run_id, step_id, attempt_no, "
                    "claimed_at, expires_at, status) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        step["step_id"],
                        attempt_no,
                        now,
                        now + ATTEMPT_TTL_SECONDS,
                        AttemptStatus.ACTIVE.value,
                    ),
                )
                self._conn.execute(
                    "UPDATE steps SET status = ?, attempts = ? "
                    "WHERE run_id = ? AND step_id = ?",
                    (StepStatus.ACTIVE.value, attempt_no, run_id, step["step_id"]),
                )
                claimed.append(
                    ClaimedAttempt(
                        attempt_id=cur.lastrowid,
                        run_id=run_id,
                        step_id=step["step_id"],
                        attempt_no=attempt_no,
                        claimed_at=now,
                        expires_at=now + ATTEMPT_TTL_SECONDS,
                    )
                )
            return claimed

    def report(
        self,
        attempt_id: int,
        success: bool,
        output: Optional[str] = None,
        *,
        now: int,
    ) -> bool:
        """回报尝试结果。返回本次回报是否生效（重复/过期/终止运行均返回 False）。"""
        with self._conn:
            att = self._conn.execute(
                "SELECT * FROM attempts WHERE attempt_id = ?", (attempt_id,)
            ).fetchone()
            if att is None:
                raise KeyError(f"attempt {attempt_id} not found")
            run_id = att["run_id"]
            run = self._load_run(run_id)
            self._check_time(run, now)
            self._advance(run_id, now)
            run = self._load_run(run_id)
            att = self._conn.execute(
                "SELECT * FROM attempts WHERE attempt_id = ?", (attempt_id,)
            ).fetchone()
            if run["status"] != RunStatus.RUNNING.value:
                return False
            if att["status"] != AttemptStatus.ACTIVE.value:
                return False
            step = self._conn.execute(
                "SELECT * FROM steps WHERE run_id = ? AND step_id = ?",
                (run_id, att["step_id"]),
            ).fetchone()
            if success:
                self._conn.execute(
                    "UPDATE attempts SET status = ?, resolved_at = ? "
                    "WHERE attempt_id = ?",
                    (AttemptStatus.SUCCEEDED.value, now, attempt_id),
                )
                self._conn.execute(
                    "UPDATE steps SET status = ?, succeeded_at = ?, output = ? "
                    "WHERE run_id = ? AND step_id = ?",
                    (StepStatus.SUCCEEDED.value, now, output, run_id, step["step_id"]),
                )
            else:
                self._conn.execute(
                    "UPDATE attempts SET status = ?, resolved_at = ? "
                    "WHERE attempt_id = ?",
                    (AttemptStatus.FAILED_REPORTED.value, now, attempt_id),
                )
                self._conn.execute(
                    "UPDATE steps SET failed_at = ? WHERE run_id = ? AND step_id = ?",
                    (now, run_id, step["step_id"]),
                )
                self._after_attempt_failure(run_id, step, now)
            self._advance(run_id, now)
            return True

    def status(self, run_id: int) -> RunView:
        """只读查看，不推进时间。"""
        run = self._load_run(run_id)
        steps = self._conn.execute(
            "SELECT * FROM steps WHERE run_id = ? ORDER BY rowid", (run_id,)
        ).fetchall()
        return RunView(
            run_id=run_id,
            status=RunStatus(run["status"]),
            created_at=run["created_at"],
            last_time=run["last_time"],
            steps=tuple(
                StepView(
                    step_id=s["step_id"],
                    kind=StepKind(s["kind"]),
                    status=StepStatus(s["status"]),
                    ready_at=s["ready_at"],
                    succeeded_at=s["succeeded_at"],
                    failed_at=s["failed_at"],
                    output=s["output"],
                    attempts=s["attempts"],
                    next_eligible_at=s["next_eligible_at"],
                )
                for s in steps
            ),
        )
