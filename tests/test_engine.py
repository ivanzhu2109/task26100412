import os
import tempfile
import unittest

from workflow import Dag, Engine, RunState, StepKind, StepSpec, StepState


def exec_step(step_id, preds=()):
    return StepSpec(step_id=step_id, kind=StepKind.EXEC, predecessors=preds)


def delay_step(step_id, preds, seconds):
    return StepSpec(step_id=step_id, kind=StepKind.DELAY,
                    predecessors=preds, delay_seconds=seconds)


class EngineTestCase(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        os.unlink(self.db_path)
        self.engine = Engine(self.db_path)

    def tearDown(self):
        self.engine.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.db_path + suffix)
            except FileNotFoundError:
                pass

    def step(self, run_id, step_id):
        status = self.engine.status(run_id)
        return next(s for s in status.steps if s.step_id == step_id)


class TestParallelRetryAndDelayChain(EngineTestCase):
    """并行分支：一支重试，另一支串联延迟+时间大跳跃，汇合步骤只在正确时刻可领取。"""

    def test_join_ready_only_after_both_branches(self):
        dag = Dag(steps=(
            exec_step("a"),
            exec_step("b", ("a",)),
            delay_step("d1", ("a",), 100),
            delay_step("d2", ("d1",), 100),
            exec_step("join", ("b", "d2")),
        ))
        run_id = self.engine.create_run(dag, now=0)

        claims = self.engine.claim(run_id, now=0)
        self.assertEqual([c.step_id for c in claims], ["a"])
        self.engine.report(run_id, claims[0].attempt_id, True, "out-a", now=1)

        # a 成功 -> b 与 d1 就绪；d1 是延迟步骤不可领取
        claims = self.engine.claim(run_id, now=2)
        self.assertEqual([c.step_id for c in claims], ["b"])
        b_attempt1 = claims[0]

        # b 第一次失败，10 秒后可重试
        self.engine.report(run_id, b_attempt1.attempt_id, False, None, now=5)
        self.assertEqual(self.engine.claim(run_id, now=10), [])
        claims = self.engine.claim(run_id, now=15)
        self.assertEqual([c.step_id for c in claims], ["b"])
        self.assertEqual(claims[0].attempt_no, 2)
        self.assertNotEqual(claims[0].attempt_id, b_attempt1.attempt_id)
        self.engine.report(run_id, claims[0].attempt_id, True, "out-b", now=20)

        # d1 就绪于 1，到期 101；d2 就绪 101，到期 201
        # 时间跳到 150：d1 已成功但 d2 未到期，汇合步骤不可领取
        self.assertEqual(self.engine.claim(run_id, now=150), [])
        self.assertEqual(self.step(run_id, "d1").succeeded_at, 101)
        self.assertEqual(self.step(run_id, "join").state, StepState.PENDING)

        # 时间大幅跳跃到 1000：d2 在 201 成功，汇合步骤就绪时刻为 max(20, 201)=201
        claims = self.engine.claim(run_id, now=1000)
        self.assertEqual([c.step_id for c in claims], ["join"])
        join_step = self.step(run_id, "join")
        self.assertEqual(join_step.ready_at, 201)
        self.assertEqual(self.step(run_id, "d2").succeeded_at, 201)

        # 汇合步骤只发出一次：重复领取不会重复发出未到期尝试
        self.assertEqual(self.engine.claim(run_id, now=1000), [])
        self.engine.report(run_id, claims[0].attempt_id, True, "out-join", now=1001)
        status = self.engine.status(run_id)
        self.assertEqual(status.state, RunState.SUCCEEDED)
        self.assertEqual(self.step(run_id, "join").attempts, 1)
        self.assertEqual(self.step(run_id, "join").output, "out-join")


class TestDeadlineAndLateReports(EngineTestCase):
    """截止秒回报、旧尝试迟到、重复回报均无效。"""

    def test_deadline_second_report_and_stale_reports(self):
        dag = Dag(steps=(exec_step("s"),))
        run_id = self.engine.create_run(dag, now=0)

        claims = self.engine.claim(run_id, now=0)
        attempt1 = claims[0]
        self.assertEqual(attempt1.deadline, 30)

        # 恰在截止秒送达：超时先发生，回报无效
        self.engine.report(run_id, attempt1.attempt_id, True, "bad", now=30)
        step = self.step(run_id, "s")
        self.assertNotEqual(step.state, StepState.SUCCEEDED)
        self.assertIsNone(step.output)

        # 超时按原截止时刻 30 计时，30+10=40 可再次领取
        self.assertEqual(self.engine.claim(run_id, now=39), [])
        claims = self.engine.claim(run_id, now=40)
        self.assertEqual(len(claims), 1)
        attempt2 = claims[0]

        # 旧尝试迟到回报：无效，不影响进行中的尝试
        self.engine.report(run_id, attempt1.attempt_id, True, "stale", now=41)
        step = self.step(run_id, "s")
        self.assertEqual(step.state, StepState.RUNNING)
        self.assertIsNone(step.output)

        # 当前尝试明确失败，以回报时刻 42 计时，42+20=62 可重试
        self.engine.report(run_id, attempt2.attempt_id, False, None, now=42)
        self.assertEqual(self.engine.claim(run_id, now=61), [])
        claims = self.engine.claim(run_id, now=62)
        attempt3 = claims[0]

        # 有效期内成功回报生效
        self.engine.report(run_id, attempt3.attempt_id, True, "good", now=63)
        step = self.step(run_id, "s")
        self.assertEqual(step.state, StepState.SUCCEEDED)
        self.assertEqual(step.succeeded_at, 63)
        self.assertEqual(step.output, "good")
        self.assertEqual(self.engine.status(run_id).state, RunState.SUCCEEDED)

        # 重复回报不能改变状态或覆盖输出
        self.engine.report(run_id, attempt3.attempt_id, False, None, now=64)
        step = self.step(run_id, "s")
        self.assertEqual(step.state, StepState.SUCCEEDED)
        self.assertEqual(step.output, "good")
        self.assertEqual(self.engine.status(run_id).state, RunState.SUCCEEDED)


class TestMaxSixAttempts(EngineTestCase):
    """第 6 次尝试失败后运行立即失败，最多领取 6 次。"""

    def claim_step(self, run_id, step_id, now):
        return next((c for c in self.engine.claim(run_id, now)
                     if c.step_id == step_id), None)

    def test_sixth_failure_fails_run(self):
        dag = Dag(steps=(
            exec_step("s"),
            exec_step("other"),  # 无关步骤，运行失败时应被取消
        ))
        run_id = self.engine.create_run(dag, now=0)

        # 第 1 次：超时失败（截止 30，重试于 40）
        a1 = self.claim_step(run_id, "s", now=0)
        self.assertIsNotNone(a1)
        # 截止时刻 30 超时先发生；重试时刻 40 之前不可领取
        self.assertIsNone(self.claim_step(run_id, "s", now=30))
        self.assertIsNone(self.claim_step(run_id, "s", now=39))
        # 其余尝试：明确失败，按 10/20/40/80/160 退避
        a2 = self.claim_step(run_id, "s", now=40)
        self.engine.report(run_id, a2.attempt_id, False, None, now=41)   # +20 -> 61
        a3 = self.claim_step(run_id, "s", now=61)
        self.engine.report(run_id, a3.attempt_id, False, None, now=62)   # +40 -> 102
        a4 = self.claim_step(run_id, "s", now=102)
        # 第 4 次超时（截止 132），+80 -> 212
        self.assertIsNone(self.claim_step(run_id, "s", now=211))
        a5 = self.claim_step(run_id, "s", now=212)
        self.engine.report(run_id, a5.attempt_id, False, None, now=213)  # +160 -> 373
        a6 = self.claim_step(run_id, "s", now=373)
        self.assertEqual(a6.attempt_no, 6)

        # 第 6 次失败 -> 运行立即失败，未完成的 other 被取消
        self.engine.report(run_id, a6.attempt_id, False, None, now=374)
        status = self.engine.status(run_id)
        self.assertEqual(status.state, RunState.FAILED)
        self.assertEqual(self.step(run_id, "s").attempts, 6)
        self.assertEqual(self.step(run_id, "other").state, StepState.CANCELLED)

        # 不会再发出第 7 次尝试
        self.assertEqual(self.engine.claim(run_id, now=10000), [])
        self.assertEqual(self.step(run_id, "s").attempts, 6)

        # 已终止运行的回报不能复活运行
        self.engine.report(run_id, a6.attempt_id, True, "revive", now=10001)
        self.assertEqual(self.engine.status(run_id).state, RunState.FAILED)
        self.assertIsNone(self.step(run_id, "s").output)


class TestTimeSemantics(EngineTestCase):
    def test_state_survives_reopen(self):
        dag = Dag(steps=(exec_step("s"), delay_step("d", ("s",), 100)))
        run_id = self.engine.create_run(dag, now=0)
        attempt = self.engine.claim(run_id, now=0)[0]
        self.engine.report(run_id, attempt.attempt_id, True, "kept", now=5)

        # 模拟进程重启：关闭后用同一数据库文件重建引擎
        self.engine.close()
        self.engine = Engine(self.db_path)

        step = self.step(run_id, "s")
        self.assertEqual(step.state, StepState.SUCCEEDED)
        self.assertEqual(step.output, "kept")
        # 延迟步骤在 105 到期；重开后推进仍按原定时刻成功
        self.assertEqual(self.engine.claim(run_id, now=200), [])
        self.assertEqual(self.step(run_id, "d").succeeded_at, 105)
        self.assertEqual(self.engine.status(run_id).state, RunState.SUCCEEDED)

    def test_view_does_not_advance_time(self):
        dag = Dag(steps=(exec_step("s"),))
        run_id = self.engine.create_run(dag, now=0)
        attempt = self.engine.claim(run_id, now=0)[0]
        # 多次查看不影响尝试有效期
        for _ in range(3):
            self.engine.status(run_id)
        self.engine.report(run_id, attempt.attempt_id, True, "ok", now=29)
        self.assertEqual(self.step(run_id, "s").state, StepState.SUCCEEDED)

    def test_backwards_time_rejected(self):
        dag = Dag(steps=(exec_step("s"),))
        run_id = self.engine.create_run(dag, now=100)
        with self.assertRaises(ValueError):
            self.engine.claim(run_id, now=99)
        with self.assertRaises(ValueError):
            self.engine.report(run_id, "whatever", True, None, now=50)

    def test_delay_chain_success_times_kept_after_big_jump(self):
        dag = Dag(steps=(
            delay_step("d1", (), 60),
            delay_step("d2", ("d1",), 60),
        ))
        run_id = self.engine.create_run(dag, now=0)
        # 到期很久之后才再次调用引擎，成功时刻仍为原定到期时刻
        self.assertEqual(self.engine.claim(run_id, now=100000), [])
        self.assertEqual(self.step(run_id, "d1").succeeded_at, 60)
        self.assertEqual(self.step(run_id, "d2").succeeded_at, 120)
        self.assertEqual(self.engine.status(run_id).state, RunState.SUCCEEDED)

    def test_no_attempt_created_by_time_jump(self):
        dag = Dag(steps=(exec_step("s"),))
        run_id = self.engine.create_run(dag, now=0)
        # 时间跳跃本身不补算尝试
        self.engine.status(run_id)
        self.assertEqual(self.step(run_id, "s").attempts, 0)
        claims = self.engine.claim(run_id, now=5000)
        self.assertEqual(len(claims), 1)
        self.assertEqual(claims[0].attempt_no, 1)
        self.assertEqual(claims[0].deadline, 5030)


if __name__ == "__main__":
    unittest.main()
