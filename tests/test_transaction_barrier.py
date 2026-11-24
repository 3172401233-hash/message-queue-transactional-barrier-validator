import unittest

from tx_barrier import (
    Broker,
    Consumer,
    EntryKind,
    EventRunner,
    IsolationLevel,
    PartitionLog,
    TransactionConflictError,
    TxnState,
    UnknownTransactionError,
    Visibility,
    parse_event,
    parse_script,
)


class TransactionInterruptionTest(unittest.TestCase):
    """事务中断：未提交数据先写入，随后 abort。"""

    def build_broker(self):
        broker = Broker()
        broker.begin("T1", 0)
        broker.data("T1", 0, "a")
        broker.data("T1", 0, "b")
        return broker

    def test_pending_data_is_hidden_until_commit_or_abort(self):
        broker = self.build_broker()
        log = broker.partition(0)

        self.assertEqual(log.barrier, 0)
        self.assertEqual(
            [view.state for view in log.message_views()],
            [Visibility.PENDING, Visibility.PENDING],
        )
        self.assertEqual(
            Consumer("safe", IsolationLevel.READ_COMMITTED).poll(log),
            [],
        )

    def test_aborted_messages_are_never_visible(self):
        broker = self.build_broker()
        broker.abort("T1", 0)
        log = broker.partition(0)

        self.assertEqual(
            [view.state for view in log.message_views()],
            [Visibility.ABORTED, Visibility.ABORTED],
        )
        self.assertEqual(log.visible_offsets(), [])
        self.assertEqual(log.aborted_data_offsets(), [1, 2])
        log.verify_barrier()

    def test_recovery_rolls_back_even_when_abort_is_before_last_offset(self):
        broker = self.build_broker()
        log = broker.partition(0)

        # 未提交时，read_uncommitted 消费者越过屏障处理了两条消息。
        unsafe = Consumer("unsafe", IsolationLevel.READ_UNCOMMITTED)
        records = unsafe.poll(log)
        self.assertEqual([record.offset for record in records], [1, 2])
        paused_offset = unsafe.position(0)

        broker.abort("T1", 0)
        # abort 标记位于暂停偏移之后，但消息本身已经进入中止事务。
        self.assertEqual(log.entries[-1].kind, EntryKind.ABORT)
        self.assertGreater(log.entries[-1].offset, paused_offset - 1)

        report = broker.analyze_recovery(0, paused_offset)
        self.assertEqual(report.visible_messages, [])
        self.assertEqual(report.rollback_offset, 0)
        self.assertTrue(report.unsafe_ranges)
        self.assertTrue(report.warnings)


class CommitAndDuplicateTest(unittest.TestCase):
    def test_commit_releases_barrier_and_makes_data_visible(self):
        broker = Broker()
        broker.begin("T1", 0)
        broker.data("T1", 0, "a")
        broker.commit("T1", 0)
        log = broker.partition(0)

        self.assertEqual(log.barrier, log.log_end_offset)
        self.assertEqual(
            [view.state for view in log.message_views()],
            [Visibility.VISIBLE],
        )
        records = Consumer(
            "safe", IsolationLevel.READ_COMMITTED
        ).poll(log)
        self.assertEqual([record.offset for record in records], [1])

    def test_duplicate_begin_and_duplicate_commit_are_rejected(self):
        broker = Broker()
        broker.begin("T1", 0)
        broker.data("T1", 0, "a")
        broker.commit("T1", 0)

        with self.assertRaises(TransactionConflictError):
            broker.begin("T1", 0)
        with self.assertRaises(TransactionConflictError):
            broker.commit("T1", 0)
        with self.assertRaises(TransactionConflictError):
            broker.abort("T1", 0)

    def test_unknown_transaction_operations_fail(self):
        broker = Broker()
        with self.assertRaises(UnknownTransactionError):
            broker.data("ghost", 0, "x")
        with self.assertRaises(UnknownTransactionError):
            broker.commit("ghost", 0)

    def test_durable_commit_marker_survives_restart_replay(self):
        broker = Broker()
        broker.begin("T1", 0)
        broker.data("T1", 0, "a")
        broker.commit("T1", 0)

        broker.restart()
        log = broker.partition(0)
        self.assertEqual(
            log.transaction_state("T1"), TxnState.COMMITTED
        )
        self.assertEqual(log.visible_offsets(), [1])
        log.verify_barrier()


class ConsumerCrossingBarrierTest(unittest.TestCase):
    def test_open_transaction_blocks_later_committed_data(self):
        broker = Broker()
        broker.begin("T1", 0)
        broker.data("T1", 0, "open")
        broker.begin("T2", 0)
        broker.data("T2", 0, "committed-but-blocked")
        broker.commit("T2", 0)

        log = broker.partition(0)
        self.assertEqual(log.barrier, 0)
        self.assertEqual(
            [view.state for view in log.message_views()],
            [Visibility.PENDING, Visibility.PENDING],
        )

        broker.abort("T1", 0)
        views = {view.offset: view.state for view in log.message_views()}
        self.assertEqual(views[1], Visibility.ABORTED)
        self.assertEqual(views[3], Visibility.VISIBLE)
        self.assertEqual(log.visible_offsets(), [3])
        log.verify_barrier()

    def test_recovery_detects_consumer_crossed_barrier(self):
        broker = Broker()
        broker.begin("T1", 0)
        broker.data("T1", 0, "will-abort")
        broker.begin("T2", 0)
        broker.data("T2", 0, "will-commit")
        broker.commit("T2", 0)

        # 暂停队列只记录最后偏移，该偏移已经越过未完成事务 T1 的屏障。
        report = broker.analyze_recovery(0, consumer_offset=4)
        self.assertEqual(report.barrier, 0)
        self.assertEqual(report.rollback_offset, 0)
        self.assertEqual(report.unsafe_ranges[0].start, 0)

        broker.abort("T1", 0)
        report_after_abort = broker.analyze_recovery(0, consumer_offset=4)
        self.assertEqual(
            report_after_abort.rollback_offset, 0
        )
        self.assertEqual(
            [entry.offset for entry in report_after_abort.visible_messages],
            [3],
        )

    def test_safe_consumer_never_returns_aborted_records(self):
        broker = Broker()
        broker.begin("T1", 0)
        broker.data("T1", 0, "a")
        broker.abort("T1", 0)
        broker.begin("T2", 0)
        broker.data("T2", 0, "b")
        broker.commit("T2", 0)

        consumer = Consumer("safe", IsolationLevel.READ_COMMITTED)
        records = consumer.poll(broker.partition(0))
        self.assertEqual([record.offset for record in records], [4])
        self.assertEqual(consumer.position(0), 5)


class BrokerRestartTest(unittest.TestCase):
    def test_restart_rebuilds_log_and_keeps_open_transaction(self):
        broker = Broker()
        broker.begin("T1", 0)
        broker.data("T1", 0, "a")
        broker.begin("T2", 1)
        broker.data("T2", 1, "b")
        broker.commit("T2", 1)

        broker.restart()
        self.assertEqual(
            broker.partition(0).transaction_state("T1"),
            TxnState.OPEN,
        )
        self.assertEqual(
            broker.partition(1).transaction_state("T2"),
            TxnState.COMMITTED,
        )
        report = broker.analyze_recovery(0, consumer_offset=2)
        self.assertEqual(report.open_ranges[0].start, 0)
        self.assertEqual(report.rollback_offset, 0)

    def test_fence_aborts_orphan_transactions_after_restart(self):
        broker = Broker()
        broker.begin("T1", 0)
        broker.data("T1", 0, "orphan")
        broker.begin("T2", 0)
        broker.data("T2", 0, "stable")
        broker.commit("T2", 0)

        fenced = broker.restart(fence=True)
        self.assertEqual(fenced, ["T1"])
        log = broker.partition(0)
        self.assertEqual(
            log.transaction_state("T1"), TxnState.ABORTED
        )
        self.assertEqual(log.aborted_data_offsets(), [1])
        self.assertEqual(log.visible_offsets(), [3])
        log.verify_barrier()

    def test_replay_from_entries_is_equivalent(self):
        source = Broker()
        source.begin("T1", 0)
        source.data("T1", 0, "a")
        source.abort("T1", 0)

        rebuilt = PartitionLog.replay(0, source.partition(0).entries)
        self.assertEqual(
            [entry.offset for entry in rebuilt.entries],
            [0, 1, 2],
        )
        self.assertEqual(
            rebuilt.transaction_state("T1"), TxnState.ABORTED
        )


class EventScriptTest(unittest.TestCase):
    def test_parse_and_run_full_recovery_script(self):
        script = """
        begin T1 0
        data T1 0 hello
        abort T1 0
        begin T2 0
        data T2 0 world
        commit T2 0
        restart
        recover 0 3
        """
        events = parse_script(script)
        runner = EventRunner()
        outputs = runner.run(events)

        report = runner.broker.analyze_recovery(0, 3)
        self.assertEqual(
            [entry.offset for entry in report.visible_messages], [4]
        )
        self.assertEqual(report.rollback_offset, 0)
        self.assertTrue(any("recover partition=0" in line for line in outputs))

    def test_poll_and_seek_events(self):
        events = [
            parse_event("begin T1 0"),
            parse_event("data T1 0 a"),
            parse_event("commit T1 0"),
            parse_event("seek c1 0 0"),
            parse_event("poll c1 0"),
        ]
        runner = EventRunner()
        runner.run(events)
        consumer = runner.consumers["c1"]
        self.assertEqual(consumer.position(0), 2)

    def test_invalid_event_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_event("unknown T1 0")
        with self.assertRaises(ValueError):
            parse_event("begin T1")


if __name__ == "__main__":
    unittest.main()
