import os
import tempfile
import unittest

from workflow import (
    Engine,
    Dag,
    RunStatus,
    StepKind,
    StepSpec,
    StepStatus,
)


def make_engine():
    return Engine(":memory:")


class ParallelBranchJoinTest(unittest.TestCase):
    """场景一：并行分支中一支重试，另一支串联延迟且时间大幅跳跃，
    汇合步骤只在正确时刻变为可领取，且只执行一次。"""

    def setUp(self):
        self.eng = make_engine()
        dag = Dag(steps=(
            StepSpec("root", StepKind.EXEC),
            StepSpec("a1", StepKind.EXEC, deps=("root",)),
            StepSpec("d1", StepKind.DELAY, deps=("root",), delay_seconds=50),
            StepSpec("d2", StepKind.DELAY, deps=("d1",), delay_seconds=70),
            StepSpec("join", StepKind.EXEC, deps=("a1", "d2")),
        ))
        self.rid = self.eng.create_run(dag, now=0)

    def step(self, step_id):
        st = self.eng.status(self.rid)
        return {s.step_id: s for s in st.steps}[step_id]

    def test_join_claimed_only_at_correct_time(self):
        # t=0：root 就绪并被领取、成功
        claimed = self.eng.claim(self.rid, now=0)
        self.assertEqual([c.step_id for c in claimed], ["root"])
        self.assertTrue(self.eng.report(claimed[0].attempt_id, True, "r", now=0))

        # t=0：a1 第一次领取；d1 在后台等待到期
        claimed = self.eng.claim(self.rid, now=0)
        self.assertEqual([c.step_id for c in claimed], ["a1"])
        # t=5：a1 第一次失败，退避 10 秒，15 秒起可再次领取
        self.assertTrue(self.eng.report(claimed[0].attempt_id, False, now=5))
        self.assertEqual(self.step("a1").next_eligible_at, 15)

        # 时间大幅跳跃到 t=1000：d1 应于 50 成功，d2 应于 120 成功
        claimed = self.eng.claim(self.rid, now=1000)
        self.assertEqual([c.step_id for c in claimed], ["a1"])  # join 尚不可领取
        self.assertEqual(self.step("d1").succeeded_at, 50)
        self.assertEqual(self.step("d2").succeeded_at, 120)
        self.assertEqual(self.step("join").status, StepStatus.PENDING)

        # t=1010：a1 第二次尝试成功；join 的就绪时刻 = max(1010, 120) = 1010
        self.assertTrue(self.eng.report(claimed[0].attempt_id, True, "a1-out", now=1010))
        join = self.step("join")
        self.assertEqual(join.ready_at, 1010)
        self.assertEqual(join.next_eligible_at, 1010)

        # join 在 1010 被领取且只被领取一次
        claimed = self.eng.claim(self.rid, now=1010)
        self.assertEqual([c.step_id for c in claimed], ["join"])
        self.assertEqual(claimed[0].attempt_no, 1)
        self.assertEqual(self.eng.claim(self.rid, now=1010), [])
        self.assertTrue(self.eng.report(claimed[0].attempt_id, True, "j", now=1012))
        self.assertEqual(self.eng.status(self.rid).status, RunStatus.SUCCEEDED)
        self.assertEqual(self.eng.claim(self.rid, now=2000), [])


class AttemptLifecycleTest(unittest.TestCase):
    """场景二：截止秒回报、旧尝试迟到、重复回报；最多领取 6 次。"""

    def setUp(self):
        self.eng = make_engine()
        dag = Dag(steps=(StepSpec("s", StepKind.EXEC),))
        self.rid = self.eng.create_run(dag, now=0)

    def step(self):
        return self.eng.status(self.rid).steps[0]

    def test_deadline_report_late_report_duplicate_report(self):
        # 第 1 次尝试：t=0 领取，30 秒到期；恰在截止秒 t=30 的回报无效
        a1 = self.eng.claim(self.rid, now=0)[0]
        self.assertEqual(a1.expires_at, 30)
        self.assertFalse(self.eng.report(a1.attempt_id, True, "x", now=30))
        self.assertEqual(self.step().failed_at, 30)          # 超时按原截止时刻计
        self.assertEqual(self.step().next_eligible_at, 40)   # 30 + 10
        self.assertEqual(self.eng.claim(self.rid, now=39), [])
        self.assertIsNone(self.step().output)                # 无效回报未写入输出

        # 第 2 次尝试：t=40 领取，t=45 明确失败，退避 20 秒
        a2 = self.eng.claim(self.rid, now=40)[0]
        self.assertTrue(self.eng.report(a2.attempt_id, False, now=45))
        self.assertEqual(self.step().failed_at, 45)
        self.assertEqual(self.step().next_eligible_at, 65)

        # 第 3 次尝试：t=65 领取，95 到期无回报；t=100 时发现超时
        a3 = self.eng.claim(self.rid, now=65)[0]
        self.assertEqual(self.eng.claim(self.rid, now=100), [])
        self.assertEqual(self.step().failed_at, 95)          # 以原截止时刻计，而非发现时刻
        self.assertEqual(self.step().next_eligible_at, 135)  # 95 + 40

        # 旧尝试迟到：t=140 才对 a3 回报，无效
        a4 = self.eng.claim(self.rid, now=135)[0]
        self.assertFalse(self.eng.report(a3.attempt_id, True, "stale", now=140))
        self.assertEqual(self.step().attempts, 4)
        self.assertIsNone(self.step().output)

        # 第 4 次失败：t=140，退避 80 秒
        self.assertTrue(self.eng.report(a4.attempt_id, False, now=140))
        self.assertEqual(self.step().next_eligible_at, 220)

        # 第 5 次失败：t=225，退避 160 秒
        a5 = self.eng.claim(self.rid, now=220)[0]
        self.assertTrue(self.eng.report(a5.attempt_id, False, now=225))
        self.assertEqual(self.step().next_eligible_at, 385)

        # 第 6 次尝试：t=385 领取；重复回报第二次无效
        a6 = self.eng.claim(self.rid, now=385)[0]
        self.assertEqual(a6.attempt_no, 6)
        self.assertTrue(self.eng.report(a6.attempt_id, False, now=390))
        self.assertFalse(self.eng.report(a6.attempt_id, False, now=391))

        # 第 6 次失败后运行立即失败，之后不能再领取
        self.assertEqual(self.eng.status(self.rid).status, RunStatus.FAILED)
        self.assertEqual(self.step().attempts, 6)
        self.assertEqual(self.eng.claim(self.rid, now=1000), [])
        self.assertFalse(self.eng.report(a6.attempt_id, True, "revive", now=1001))


class RunFailureCancelsTest(unittest.TestCase):
    """运行失败取消未完成步骤，已成功的步骤与输出保留，后续回报不能复活。"""

    def test_cancel_on_sixth_failure(self):
        eng = make_engine()
        dag = Dag(steps=(
            StepSpec("bad", StepKind.EXEC),
            StepSpec("ok", StepKind.EXEC),
            StepSpec("down", StepKind.EXEC, deps=("bad",)),
        ))
        rid = eng.create_run(dag, now=0)
        claimed = {c.step_id: c for c in eng.claim(rid, now=0)}
        self.assertEqual(set(claimed), {"bad", "ok"})
        self.assertTrue(eng.report(claimed["ok"].attempt_id, True, "ok-out", now=1))

        # bad 第 1 次失败：t=2 明确失败，退避 10 秒
        self.assertTrue(eng.report(claimed["bad"].attempt_id, False, now=2))
        # 再失败 5 次（退避 20/40/80/160）
        t = 12
        for i in range(5):
            c = eng.claim(rid, now=t)
            self.assertEqual(len(c), 1, f"attempt {i + 2}")
            self.assertTrue(eng.report(c[0].attempt_id, False, now=t))
            t += (20, 40, 80, 160, 0)[i]

        st = eng.status(rid)
        self.assertEqual(st.status, RunStatus.FAILED)
        steps = {s.step_id: s for s in st.steps}
        self.assertEqual(steps["ok"].status, StepStatus.SUCCEEDED)
        self.assertEqual(steps["ok"].output, "ok-out")       # 输出保留
        self.assertEqual(steps["down"].status, StepStatus.CANCELLED)
        self.assertEqual(eng.claim(rid, now=t + 1000), [])


class TimeoutAndSuccessTest(unittest.TestCase):
    def test_success_report_and_output_persisted(self):
        eng = make_engine()
        rid = eng.create_run(Dag(steps=(StepSpec("s", StepKind.EXEC),)), now=10)
        a = eng.claim(rid, now=10)[0]
        self.assertTrue(eng.report(a.attempt_id, True, "done", now=20))
        st = eng.status(rid)
        self.assertEqual(st.status, RunStatus.SUCCEEDED)
        self.assertEqual(st.steps[0].output, "done")
        self.assertEqual(st.steps[0].succeeded_at, 20)
        # 已终止运行的回报无效
        self.assertFalse(eng.report(a.attempt_id, True, "other", now=21))

    def test_no_double_active_attempt(self):
        eng = make_engine()
        rid = eng.create_run(Dag(steps=(StepSpec("s", StepKind.EXEC),)), now=0)
        a1 = eng.claim(rid, now=0)[0]
        # 有效期内重复领取不会重新发出
        self.assertEqual(eng.claim(rid, now=10), [])
        self.assertEqual(eng.claim(rid, now=29), [])
        # 到期后（含截止秒）按超时处理，退避后可再领取
        self.assertEqual(eng.claim(rid, now=30), [])
        a2 = eng.claim(rid, now=40)[0]
        self.assertEqual(a2.attempt_no, 2)
        self.assertNotEqual(a2.attempt_id, a1.attempt_id)    # 尝试标识不复用

    def test_time_must_not_go_backwards(self):
        eng = make_engine()
        rid = eng.create_run(Dag(steps=(StepSpec("s", StepKind.EXEC),)), now=100)
        with self.assertRaises(ValueError):
            eng.claim(rid, now=99)


class DelayChainTest(unittest.TestCase):
    def test_chained_delays_advance_by_schedule(self):
        eng = make_engine()
        dag = Dag(steps=(
            StepSpec("d1", StepKind.DELAY, delay_seconds=10),
            StepSpec("d2", StepKind.DELAY, deps=("d1",), delay_seconds=20),
            StepSpec("d3", StepKind.DELAY, deps=("d2",), delay_seconds=0),
            StepSpec("tail", StepKind.EXEC, deps=("d3",)),
        ))
        rid = eng.create_run(dag, now=0)
        # 到期后很久才调用：成功时刻仍为原定到期时刻，串联连续推进
        claimed = eng.claim(rid, now=10000)
        self.assertEqual([c.step_id for c in claimed], ["tail"])
        steps = {s.step_id: s for s in eng.status(rid).steps}
        self.assertEqual(steps["d1"].succeeded_at, 10)
        self.assertEqual(steps["d2"].succeeded_at, 30)
        self.assertEqual(steps["d3"].succeeded_at, 30)
        self.assertEqual(steps["tail"].ready_at, 30)


class PersistenceTest(unittest.TestCase):
    def test_state_survives_reopen(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "wf.db")
            eng = Engine(path)
            dag = Dag(steps=(
                StepSpec("a", StepKind.EXEC),
                StepSpec("b", StepKind.EXEC, deps=("a",)),
            ))
            rid = eng.create_run(dag, now=0)
            a = eng.claim(rid, now=0)[0]
            eng.report(a.attempt_id, True, "A", now=1)
            eng.close()

            eng2 = Engine(path)
            st = eng2.status(rid)
            self.assertEqual(st.status, RunStatus.RUNNING)
            steps = {s.step_id: s for s in st.steps}
            self.assertEqual(steps["a"].output, "A")
            claimed = eng2.claim(rid, now=2)
            self.assertEqual([c.step_id for c in claimed], ["b"])
            eng2.close()

    def test_status_does_not_advance_time(self):
        eng = make_engine()
        rid = eng.create_run(
            Dag(steps=(StepSpec("d", StepKind.DELAY, delay_seconds=10),)), now=0
        )
        # 查看不推进时间：即使真实时间已过期，状态仍停留在 last_time=0
        st = eng.status(rid)
        self.assertEqual(st.steps[0].status, StepStatus.READY)
        self.assertEqual(st.last_time, 0)
        # 变更调用才推进
        eng.claim(rid, now=100)
        st = eng.status(rid)
        self.assertEqual(st.steps[0].status, StepStatus.SUCCEEDED)
        self.assertEqual(st.steps[0].succeeded_at, 10)


if __name__ == "__main__":
    unittest.main()
