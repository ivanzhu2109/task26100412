"""数据模型与常量定义。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

ATTEMPT_TTL_SECONDS = 30
MAX_ATTEMPTS = 6
RETRY_BACKOFF_SECONDS = (10, 20, 40, 80, 160)


class StepKind(str, Enum):
    EXEC = "exec"
    DELAY = "delay"


class StepState(str, Enum):
    PENDING = "pending"      # 前驱未全部成功
    READY = "ready"          # 可领取（执行步骤）或等待到期（延迟步骤）
    RUNNING = "running"      # 存在未到期尝试
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"  # 运行失败时被取消


class RunState(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True)
class StepSpec:
    """DAG 中的单个步骤定义（调用方保证已验证、不可变）。"""
    step_id: str
    kind: StepKind
    predecessors: tuple[str, ...] = ()
    delay_seconds: int = 0  # 仅 DELAY 步骤使用


@dataclass(frozen=True)
class Dag:
    steps: tuple[StepSpec, ...]


@dataclass(frozen=True)
class Claim:
    """一次领取结果：一个可执行尝试。"""
    run_id: str
    step_id: str
    attempt_id: str
    attempt_no: int
    deadline: int


@dataclass(frozen=True)
class StepStatus:
    step_id: str
    kind: StepKind
    state: StepState
    attempts: int
    ready_at: int | None
    succeeded_at: int | None
    output: str | None


@dataclass(frozen=True)
class RunStatus:
    run_id: str
    state: RunState
    steps: tuple[StepStatus, ...]
