"""类型定义：DAG 描述、枚举与常量。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


ATTEMPT_TTL_SECONDS = 30
MAX_ATTEMPTS = 6
RETRY_BACKOFFS = (10, 20, 40, 80, 160)


class StepKind(str, Enum):
    EXEC = "exec"
    DELAY = "delay"


class StepStatus(str, Enum):
    PENDING = "pending"        # 等待前驱
    READY = "ready"            # 可领取（或延迟步骤等待到期）
    ACTIVE = "active"          # 存在有效尝试
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"    # 运行失败后被取消


class RunStatus(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class AttemptStatus(str, Enum):
    ACTIVE = "active"
    SUCCEEDED = "succeeded"
    FAILED_REPORTED = "failed_reported"
    FAILED_TIMEOUT = "failed_timeout"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class StepSpec:
    """DAG 中的一个步骤。delay 步骤必须给出非负 delay_seconds。"""
    step_id: str
    kind: StepKind
    deps: tuple[str, ...] = ()
    delay_seconds: int = 0


@dataclass(frozen=True)
class Dag:
    """已验证的不可变 DAG，仅含执行步骤与延迟步骤。"""
    steps: tuple[StepSpec, ...]

    def __post_init__(self) -> None:
        ids = [s.step_id for s in self.steps]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate step_id")
        known = set(ids)
        for s in self.steps:
            for d in s.deps:
                if d not in known:
                    raise ValueError(f"unknown dep {d!r} on step {s.step_id!r}")
            if s.kind is StepKind.DELAY and s.delay_seconds < 0:
                raise ValueError(f"negative delay on step {s.step_id!r}")


@dataclass(frozen=True)
class ClaimedAttempt:
    """一次领取结果。"""
    attempt_id: int
    run_id: int
    step_id: str
    attempt_no: int
    claimed_at: int
    expires_at: int


@dataclass(frozen=True)
class StepView:
    step_id: str
    kind: StepKind
    status: StepStatus
    ready_at: Optional[int]
    succeeded_at: Optional[int]
    failed_at: Optional[int]
    output: Optional[str]
    attempts: int
    next_eligible_at: Optional[int]


@dataclass(frozen=True)
class RunView:
    run_id: int
    status: RunStatus
    created_at: int
    last_time: int
    steps: tuple[StepView, ...]
