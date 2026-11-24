"""命令行演示：python -m tx_barrier

演示一个本地事务中断场景：
T1 未提交时已写入数据，read_uncommitted 消费者越过屏障处理；
随后 T1 abort，恢复时仅靠“最后偏移”会错误消费中止消息。
"""
from __future__ import annotations

from .broker import Broker
from .events import format_recovery_report
from .model import (
    Consumer,
    EntryKind,
    IsolationLevel,
    TxnState,
    Visibility,
)


def main() -> None:
    print("== 本地事务屏障演示（无网络、无外部 broker） ==")
    broker = Broker()
    partition = 0

    broker.begin("T1", partition)
    broker.data("T1", partition, "uncommitted-1")
    broker.data("T1", partition, "uncommitted-2")
    broker.begin("T2", partition)
    broker.data("T2", partition, "blocked-by-open-T1")

    log = broker.partition(partition)
    print("\n事务未结束时的消息状态：")
    for view in log.message_views():
        print(
            "  offset=%d txn=%s payload=%-20s state=%s"
            % (
                view.offset,
                view.transaction_id,
                view.payload,
                view.state.value,
            )
        )
    print("  read_committed barrier =", log.barrier)

    safe_consumer = Consumer("safe", IsolationLevel.READ_COMMITTED)
    unsafe_consumer = Consumer(
        "unsafe", IsolationLevel.READ_UNCOMMITTED
    )
    safe_records = safe_consumer.poll(log)
    unsafe_records = unsafe_consumer.poll(log)
    print("\nread_committed 消费偏移：", [r.offset for r in safe_records])
    print("read_uncommitted 消费偏移：", [r.offset for r in unsafe_records])
    print("不安全消费者暂停时记录的最后偏移：", unsafe_consumer.position(partition))

    broker.abort("T1", partition)
    broker.commit("T2", partition)

    print("\nT1 abort、T2 commit 后：")
    for view in log.message_views():
        print(
            "  offset=%d txn=%s payload=%-20s state=%s"
            % (
                view.offset,
                view.transaction_id,
                view.payload,
                view.state.value,
            )
        )
    print("  barrier =", log.barrier)
    log.verify_barrier()

    bad_offset = unsafe_consumer.position(partition)
    report = broker.analyze_recovery(partition, bad_offset)
    print("\n按暂停队列记录的最后偏移恢复：")
    print(format_recovery_report(report))

    print("\n重启回放（未 fence 的事务保持 OPEN）：")
    broker.restart()
    replayed = broker.partition(partition)
    print(
        "  T1 =",
        replayed.transaction_state("T1").value,
        "T2 =",
        replayed.transaction_state("T2").value,
    )

    broker.begin("T3", partition)
    broker.data("T3", partition, "lost-on-restart")
    fenced = broker.restart(fence=True)
    print("再次重启并 fence 未完成事务：", fenced)
    print(
        "  T3 =",
        broker.partition(partition).transaction_state("T3").value,
    )


if __name__ == "__main__":
    main()
