"""本地事务消息模型。

所有消息流均在进程内模拟，不连接任何外部 broker。

一个分区是按追加顺序排列的日志，每条记录拥有单调递增的日志偏移：

* ``BEGIN`` / ``COMMIT`` / ``ABORT`` 是事务边界标记，同样占用日志偏移，
  但不会作为数据消息返回给消费者。
* ``DATA`` 是事务内的数据记录。

``read_committed`` 消费者只能越过已经结束（提交或中止）的事务；
屏障（barrier，即最老未完成事务的起始偏移）之前的数据才允许可见。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class EntryKind(str, Enum):
    BEGIN = "BEGIN"
    DATA = "DATA"
    COMMIT = "COMMIT"
    ABORT = "ABORT"


class TxnState(str, Enum):
    OPEN = "OPEN"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class IsolationLevel(str, Enum):
    READ_COMMITTED = "read_committed"
    READ_UNCOMMITTED = "read_uncommitted"


class Visibility(str, Enum):
    VISIBLE = "visible"        # 已提交且在消费者屏障之后
    ABORTED = "aborted"        # 事务已中止，永不允许消费
    PENDING = "pending"        # 事务尚未结束，被屏障拦截


class BarrierError(Exception):
    """事务屏障相关错误的基类。"""


class UnknownTransactionError(BarrierError):

    def __init__(self, transaction_id: str) -> None:
        super().__init__("unknown transaction: %s" % transaction_id)
        self.transaction_id = transaction_id


class TransactionConflictError(BarrierError):
    """同一个事务 ID 被重复开启，或重复写入结束标记。"""


@dataclass(frozen=True)
class LogEntry:
    """分区日志中的一条不可变记录。"""

    offset: int
    kind: EntryKind
    sequence: int
    transaction_id: Optional[str] = None
    payload: Optional[str] = None

    @property
    def is_data(self) -> bool:
        return self.kind is EntryKind.DATA

    @property
    def is_marker(self) -> bool:
        return self.kind in (EntryKind.BEGIN, EntryKind.COMMIT, EntryKind.ABORT)


@dataclass(frozen=True)
class MessageView:
    """一条数据消息的可见性判定结果。"""

    offset: int
    sequence: int
    transaction_id: str
    payload: str
    state: Visibility


@dataclass(frozen=True)
class OffsetRange:
    """半开区间 [start, end)，单位为日志偏移。"""

    start: int
    end: int
    reason: str = ""

    def contains(self, offset: int) -> bool:
        return self.start <= offset < self.end

    def as_tuple(self):
        return self.start, self.end


@dataclass
class TxnInfo:
    transaction_id: str
    state: TxnState
    begin_offset: int
    end_offset: Optional[int] = None
    data_offsets: List[int] = field(default_factory=list)


def merge_ranges(ranges: List[OffsetRange]) -> List[OffsetRange]:
    """合并相交或相邻的半开区间，并拼接原因。"""

    if not ranges:
        return []
    ordered = sorted(ranges, key=lambda item: (item.start, item.end))
    merged = [OffsetRange(ordered[0].start, ordered[0].end, ordered[0].reason)]
    for item in ordered[1:]:
        last = merged[-1]
        if item.start <= last.end:
            reason = last.reason
            if item.reason and item.reason not in reason.split("|"):
                reason = "%s|%s" % (reason, item.reason) if reason else item.reason
            merged[-1] = OffsetRange(last.start, max(last.end, item.end), reason)
        else:
            merged.append(item)
    return merged


class PartitionLog:
    """单个分区上的本地事务日志与屏障计算。"""

    def __init__(self, partition: int) -> None:
        self.partition = partition
        self._entries: List[LogEntry] = []
        self._transactions: Dict[str, TxnInfo] = {}

    @property
    def entries(self) -> List[LogEntry]:
        return list(self._entries)

    @property
    def transactions(self) -> Dict[str, TxnInfo]:
        return dict(self._transactions)

    @property
    def log_end_offset(self) -> int:
        """下一条记录将使用的日志偏移（已写入记录的上界）。"""

        return len(self._entries)

    @property
    def barrier(self) -> int:
        """read_committed 屏障（last stable offset 风格）。

        屏障之前的日志偏移都已稳定；最老的未完成事务从其 BEGIN 标记起
        拦截后续所有数据，即使这些数据属于另一个已提交事务。
        """

        open_begins = [
            info.begin_offset
            for info in self._transactions.values()
            if info.state is TxnState.OPEN
        ]
        return min(open_begins) if open_begins else self.log_end_offset

    def append_begin(self, transaction_id: str) -> LogEntry:
        if transaction_id in self._transactions:
            raise TransactionConflictError(
                "transaction %s already exists" % transaction_id
            )
        entry = self._append(EntryKind.BEGIN, transaction_id)
        self._transactions[transaction_id] = TxnInfo(
            transaction_id=transaction_id,
            state=TxnState.OPEN,
            begin_offset=entry.offset,
        )
        return entry

    def append_data(self, transaction_id: str, payload: str) -> LogEntry:
        info = self._require_open(transaction_id)
        entry = self._append(EntryKind.DATA, transaction_id, payload)
        info.data_offsets.append(entry.offset)
        return entry

    def append_finalize(
        self, transaction_id: str, kind: EntryKind
    ) -> LogEntry:
        if kind not in (EntryKind.COMMIT, EntryKind.ABORT):
            raise ValueError("finalize kind must be COMMIT or ABORT")
        info = self._require_open(transaction_id)
        entry = self._append(kind, transaction_id)
        info.state = (
            TxnState.COMMITTED
            if kind is EntryKind.COMMIT
            else TxnState.ABORTED
        )
        info.end_offset = entry.offset
        return entry

    def _append(
        self,
        kind: EntryKind,
        transaction_id: Optional[str],
        payload: Optional[str] = None,
    ) -> LogEntry:
        offset = len(self._entries)
        entry = LogEntry(
            offset=offset,
            kind=kind,
            sequence=offset,
            transaction_id=transaction_id,
            payload=payload,
        )
        self._entries.append(entry)
        return entry

    def _require_open(self, transaction_id: str) -> TxnInfo:
        info = self._transactions.get(transaction_id)
        if info is None:
            raise UnknownTransactionError(transaction_id)
        if info.state is not TxnState.OPEN:
            raise TransactionConflictError(
                "transaction %s is already %s"
                % (transaction_id, info.state.value)
            )
        return info

    @classmethod
    def replay(cls, partition: int, entries: List[LogEntry]) -> "PartitionLog":
        """从持久化日志条目重建内存状态，模拟 broker 重启恢复。"""

        log = cls(partition)
        for entry in entries:
            if entry.kind is EntryKind.BEGIN:
                log.append_begin(entry.transaction_id)
            elif entry.kind is EntryKind.DATA:
                log.append_data(entry.transaction_id, entry.payload or "")
            else:
                log.append_finalize(entry.transaction_id, entry.kind)
        return log

    def data_entries(self) -> List[LogEntry]:
        return [entry for entry in self._entries if entry.is_data]

    def transaction_state(self, transaction_id: str) -> TxnState:
        info = self._transactions.get(transaction_id)
        if info is None:
            raise UnknownTransactionError(transaction_id)
        return info.state

    def visibility_of(self, entry: LogEntry) -> Visibility:
        if not entry.is_data:
            raise ValueError("only DATA entries have consumer visibility")
        state = self.transaction_state(entry.transaction_id)
        if state is TxnState.OPEN:
            return Visibility.PENDING
        if state is TxnState.ABORTED:
            return Visibility.ABORTED
        if entry.offset < self.barrier:
            return Visibility.VISIBLE
        return Visibility.PENDING

    def message_views(self) -> List[MessageView]:
        views = []
        for entry in self.data_entries():
            views.append(
                MessageView(
                    offset=entry.offset,
                    sequence=entry.sequence,
                    transaction_id=entry.transaction_id,
                    payload=entry.payload or "",
                    state=self.visibility_of(entry),
                )
            )
        return views

    def visible_offsets(self) -> List[int]:
        return [
            view.offset
            for view in self.message_views()
            if view.state is Visibility.VISIBLE
        ]

    def aborted_ranges(self) -> List[OffsetRange]:
        """已中止事务覆盖的日志范围（含 BEGIN/ABORT 标记）。"""

        ranges = []
        for info in self._transactions.values():
            if info.state is TxnState.ABORTED:
                ranges.append(
                    OffsetRange(
                        info.begin_offset,
                        (info.end_offset + 1) if info.end_offset is not None
                        else self.log_end_offset,
                        "aborted:%s" % info.transaction_id,
                    )
                )
        return merge_ranges(ranges)

    def open_ranges(self) -> List[OffsetRange]:
        """恢复时仍无结束标记的事务范围，无法自动判定最终状态。"""

        ranges = []
        for info in self._transactions.values():
            if info.state is TxnState.OPEN:
                ranges.append(
                    OffsetRange(
                        info.begin_offset,
                        self.log_end_offset,
                        "open:%s" % info.transaction_id,
                    )
                )
        return merge_ranges(ranges)

    def aborted_data_offsets(self) -> List[int]:
        result = []
        for info in self._transactions.values():
            if info.state is TxnState.ABORTED:
                result.extend(info.data_offsets)
        return sorted(result)

    def verify_barrier(self) -> None:
        """屏障不变量：可见消息不得来自未提交或已中止事务。"""

        for view in self.message_views():
            if view.state is Visibility.VISIBLE:
                state = self.transaction_state(view.transaction_id)
                if state is not TxnState.COMMITTED:
                    raise AssertionError(
                        "barrier violated at offset %d (txn=%s state=%s)"
                        % (view.offset, view.transaction_id, state.value)
                    )


class Consumer:
    """本地消费者模型，维护每个分区的消费位置。"""

    def __init__(
        self,
        name: str,
        isolation: IsolationLevel = IsolationLevel.READ_COMMITTED,
    ) -> None:
        self.name = name
        self.isolation = isolation
        self._positions: Dict[int, int] = {}

    def position(self, partition: int) -> int:
        return self._positions.get(partition, 0)

    def seek(self, partition: int, offset: int) -> None:
        if offset < 0:
            raise ValueError("offset must be non-negative")
        self._positions[partition] = offset

    def poll(self, log: PartitionLog, max_records: Optional[int] = None):
        """返回当前隔离级别下允许消费的数据消息。

        ``read_committed`` 在屏障处停下，绝不返回 ABORTED/PENDING 数据。
        ``read_uncommitted`` 会越过屏障，用于复现消费者越界问题。
        """

        limit = log.log_end_offset
        if self.isolation is IsolationLevel.READ_COMMITTED:
            limit = log.barrier
        delivered = []
        position = self.position(log.partition)
        for entry in log.entries:
            if entry.offset < position or entry.offset >= limit:
                continue
            if not entry.is_data:
                continue
            if self.isolation is IsolationLevel.READ_COMMITTED:
                if log.visibility_of(entry) is not Visibility.VISIBLE:
                    continue
            delivered.append(entry)
            if max_records is not None and len(delivered) >= max_records:
                break
        if delivered:
            self._positions[log.partition] = delivered[-1].offset + 1
        return delivered
