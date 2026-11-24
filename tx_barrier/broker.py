"""本地 broker：分区管理、重启回放与事务恢复分析。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .model import (
    EntryKind,
    IsolationLevel,
    LogEntry,
    OffsetRange,
    PartitionLog,
    TxnState,
    Visibility,
    merge_ranges,
)


@dataclass(frozen=True)
class RecoveryReport:
    """一次暂停/重启恢复的完整判定结果。"""

    partition: int
    barrier: int
    visible_messages: List[LogEntry]
    rollback_offset: Optional[int]
    consumer_offset: int
    unsafe_ranges: List[OffsetRange]
    open_ranges: List[OffsetRange]
    warnings: List[str] = field(default_factory=list)


class Broker:
    """进程内 broker，所有状态仅保存在内存与传入的本地日志中。"""

    def __init__(self) -> None:
        self._partitions: Dict[int, PartitionLog] = {}

    def partition(self, partition: int) -> PartitionLog:
        if partition not in self._partitions:
            self._partitions[partition] = PartitionLog(partition)
        return self._partitions[partition]

    @property
    def partitions(self) -> Dict[int, PartitionLog]:
        return dict(self._partitions)

    def begin(self, transaction_id: str, partition: int) -> LogEntry:
        return self.partition(partition).append_begin(transaction_id)

    def data(
        self, transaction_id: str, partition: int, payload: str
    ) -> LogEntry:
        return self.partition(partition).append_data(transaction_id, payload)

    def commit(self, transaction_id: str, partition: int) -> LogEntry:
        return self.partition(partition).append_finalize(
            transaction_id, EntryKind.COMMIT
        )

    def abort(self, transaction_id: str, partition: int) -> LogEntry:
        return self.partition(partition).append_finalize(
            transaction_id, EntryKind.ABORT
        )

    def fence_open_transactions(self, partition: int) -> List[str]:
        """broker 重启后，将没有结束标记的事务按超时围栏中止。"""

        log = self.partition(partition)
        aborted = []
        for transaction_id, info in log.transactions.items():
            if info.state is TxnState.OPEN:
                log.append_finalize(transaction_id, EntryKind.ABORT)
                aborted.append(transaction_id)
        return aborted

    def restart(self, fence: bool = False) -> List[str]:
        """模拟 broker 重启：从日志条目重建全部分区状态。"""

        rebuilt: Dict[int, PartitionLog] = {}
        fenced: List[str] = []
        for partition, old_log in self._partitions.items():
            new_log = PartitionLog.replay(partition, old_log.entries)
            rebuilt[partition] = new_log
        self._partitions = rebuilt
        if fence:
            for partition in sorted(self._partitions):
                fenced.extend(self.fence_open_transactions(partition))
        return fenced

    def analyze_recovery(
        self,
        partition: int,
        consumer_offset: int,
        isolation: IsolationLevel = IsolationLevel.READ_COMMITTED,
    ) -> RecoveryReport:
        """根据消费者暂停时记录的最后偏移计算恢复动作。

        关键点：仅记录“最后偏移”是不够的。若该偏移落在已中止事务或
        当前屏障之后，必须把消费位置回滚到安全边界，并报告无法仅凭
        本地偏移自动恢复的日志范围。
        """

        log = self.partition(partition)
        warnings: List[str] = []
        aborted_ranges = log.aborted_ranges()
        open_ranges = log.open_ranges()
        barrier = log.barrier

        unsafe = [
            OffsetRange(
                item.start,
                item.end,
                "consumer-crossed-barrier:%s" % item.reason,
            )
            for item in aborted_ranges + open_ranges
            if consumer_offset > item.start
        ]
        unsafe_ranges = merge_ranges(unsafe)

        rollback_offset: Optional[int] = None
        if consumer_offset > barrier:
            rollback_offset = barrier
            warnings.append(
                "consumer offset %d crosses barrier %d"
                % (consumer_offset, barrier)
            )
        else:
            for item in aborted_ranges:
                if item.contains(consumer_offset - 1) or (
                    consumer_offset > item.start
                    and consumer_offset <= item.end
                ):
                    rollback_offset = item.start
                    warnings.append(
                        "consumer offset %d overlaps aborted range [%d, %d)"
                        % (consumer_offset, item.start, item.end)
                    )
                    break

        visible = [
            entry
            for entry in log.data_entries()
            if log.visibility_of(entry) is Visibility.VISIBLE
        ]
        if isolation is IsolationLevel.READ_UNCOMMITTED:
            warnings.append(
                "read_uncommitted consumer can bypass the transaction barrier"
            )

        return RecoveryReport(
            partition=partition,
            barrier=barrier,
            visible_messages=visible,
            rollback_offset=rollback_offset,
            consumer_offset=consumer_offset,
            unsafe_ranges=unsafe_ranges,
            open_ranges=open_ranges,
            warnings=warnings,
        )

    def recover(
        self,
        partition: int,
        consumer_offset: int,
        isolation: IsolationLevel = IsolationLevel.READ_COMMITTED,
    ) -> RecoveryReport:
        """恢复分析的语义化别名，返回可见消息、回滚偏移和不安全范围。"""

        return self.analyze_recovery(partition, consumer_offset, isolation)
