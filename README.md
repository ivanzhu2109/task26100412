# workflow：可恢复的工作流执行库

纯 Python + SQLite，无第三方依赖。运行状态与步骤输出全部持久化到 SQLite，
进程重启后用同一个数据库文件重建 `Engine` 即可继续。步骤的实际执行由本机
调用方完成，引擎只负责调度、计时与状态持久化。

## 四类操作

| 操作 | 方法 | 说明 |
| --- | --- | --- |
| 创建运行 | `create_run(dag, now) -> run_id` | 传入已验证的不可变 DAG（仅执行/延迟步骤），根步骤立即就绪 |
| 领取尝试 | `claim(run_id, now) -> list[ClaimedAttempt]` | 为所有当前具备资格的执行步骤签发新尝试 |
| 回报结果 | `report(attempt_id, success, output=None, *, now) -> bool` | 返回本次回报是否生效 |
| 查看状态 | `status(run_id) -> RunView` | 只读，不推进时间 |

## 快速开始

```python
from workflow import Engine, Dag, StepSpec, StepKind

dag = Dag(steps=(
    StepSpec("extract", StepKind.EXEC),
    StepSpec("wait",    StepKind.DELAY, deps=("extract",), delay_seconds=60),
    StepSpec("load",    StepKind.EXEC,  deps=("wait",)),
))

eng = Engine("workflow.db")          # 或 ":memory:"
run_id = eng.create_run(dag, now=1_000)

for att in eng.claim(run_id, now=1_000):
    # att.expires_at = 领取时刻 + 30 秒；在有效期内回报
    eng.report(att.attempt_id, True, output="rows=42", now=1_005)

# 延迟步骤到期即自动成功，成功时刻为原定到期时刻；
# 即使很久之后才再次调用引擎，时间也按原定时刻推进。
for att in eng.claim(run_id, now=1_100):
    ...

print(eng.status(run_id))
eng.close()
```

## 语义要点

- **时间**：每个变更调用（创建/领取/回报）携带不倒退的整数秒 `now`；
  引擎先把该运行中应发生的事件（尝试超时、延迟到期）按各自原定时刻推进，
  再处理本次操作。`now` 倒退会抛 `ValueError`。
- **就绪**：非根步骤在全部直接前驱成功后就绪，就绪时刻为前驱成功时刻的最大值；
  汇合步骤只就绪一次，不会因多个前驱分别完成而重复执行。
- **延迟步骤**：从就绪时刻起等待 `delay_seconds`，到期即成功，成功时刻为原定
  到期时刻；串联延迟按此规则连续推进。
- **尝试**：有效期为领取时刻起 30 秒；同一步步同时至多一个有效尝试，重复领取
  不会重发未到期的尝试。截止时刻先发生超时——恰在截止秒送达的回报无效。
- **回报**：有效期内的成功使步骤成功并保存输出；失败使本次尝试失败。重复回报、
  过期尝试回报、已终止运行的回报一律无效（返回 `False`），不改变状态、不覆盖输出。
- **重试**：每个执行步骤最多 6 次尝试，失败后分别在 10/20/40/80/160 秒后可再次
  领取。明确失败以回报时刻计时，超时失败以原截止时刻计时；只有实际领取才开始
  新尝试，时间跳跃不会补算从未发出的尝试。
- **终态**：第 6 次失败后运行立即失败，取消所有未完成步骤（已成功的步骤与输出
  保留）；全部步骤成功后运行成功。终态运行的后续回报不能复活它。

## 运行测试

```bash
python3 -m unittest discover -s tests -v
```

## 文件结构

- `workflow/models.py` — DAG 描述、枚举、视图类型与常量
- `workflow/storage.py` — SQLite 建表与 DAG 序列化
- `workflow/engine.py` — 引擎：时间推进、领取、回报、查看
- `tests/test_workflow.py` — 自动化测试
