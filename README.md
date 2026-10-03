# task26100412-b：可恢复的工作流执行库

基于 Python + SQLite 的工作流执行库。运行状态与步骤输出全部持久化到 SQLite，
进程重启后可继续。步骤的实际执行由本机调用方完成，引擎只负责调度与状态机。

## 安装与运行

无第三方依赖，要求 Python 3.10+。

```bash
python3 -m unittest discover -s tests   # 运行测试
```

## 数据模型

```python
from workflow import Dag, StepSpec, StepKind

dag = Dag(steps=(
    StepSpec("extract", StepKind.EXEC),
    StepSpec("wait",    StepKind.DELAY, predecessors=("extract",), delay_seconds=60),
    StepSpec("load",    StepKind.EXEC,  predecessors=("wait",)),
))
```

- DAG 由调用方保证已验证且不可变，仅含执行步骤（`EXEC`）与延迟步骤（`DELAY`）。
- 根步骤在运行创建时就绪；其他步骤在全部直接前驱成功后就绪，
  就绪时刻为前驱成功时刻的最大值。

## 四类操作

```python
from workflow import Engine

engine = Engine("runs.sqlite")

# 1. 创建运行（now 为整数秒时间，同一运行内不倒退）
run_id = engine.create_run(dag, now=0)

# 2. 领取可执行尝试：返回 Claim 列表，每个含持久唯一的 attempt_id，
#    有效期为领取时刻起 30 秒（deadline = now + 30）
claims = engine.claim(run_id, now=10)

# 3. 回报尝试结果：成功保存输出；失败按 10/20/40/80/160 秒退避后允许重试
engine.report(run_id, claims[0].attempt_id, succeeded=True, output='{"rows": 3}', now=25)

# 4. 查看状态：只读，不推进时间
status = engine.status(run_id)   # RunStatus(state, steps=[StepStatus...])
```

## 语义要点

- **时间推进**：每次变更调用（创建/领取/回报）先按事件应发生的时刻把该运行
  推进到 `now`（处理延迟步骤到期、尝试超时），再处理本次操作；`status` 只读不推进。
- **延迟步骤**：从就绪时刻起等待指定时长即成功；即使到期后很久才再次调用引擎，
  成功时刻仍为原定到期时刻，串联延迟按此连续推进。
- **尝试**：每次领取生成不复用的 `attempt_id`，30 秒有效期；同一步步不能同时有
  两个有效尝试；重复领取不会重发未到期尝试。截止时刻超时先发生，恰在截止秒
  送达的回报无效。重复回报、过期尝试回报、已终止运行的回报均被忽略。
- **重试**：每个执行步骤最多 6 次尝试（首次 + 5 次重试），退避 10/20/40/80/160 秒。
  明确失败以回报时刻计时，超时失败以原截止时刻计时。只有实际领取才开始新尝试，
  时间跳跃不会补算从未发出的尝试。
- **终止**：第 6 次尝试失败后运行立即失败，取消所有未完成步骤（已成功步骤及输出
  保留）；全部步骤成功后运行成功。

## 文件结构

- `workflow/model.py` — 类型定义与常量（DAG、状态枚举、领取/状态结果）
- `workflow/store.py` — SQLite schema 与连接
- `workflow/engine.py` — 引擎：时间推进、领取、回报、状态查询
- `tests/test_engine.py` — 自动化测试（并行重试+延迟链汇合、截止秒/迟到/重复回报、
  6 次尝试上限、时间语义）
