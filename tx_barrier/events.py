"""文本事件解析与本地回放。

每行一条事件，字段以空白分隔，payload 取剩余文本：

    begin   <txn_id> <partition>
    data    <txn_id> <partition> <payload...>
    commit  <txn_id> <partition>
    abort   <txn_id> <partition>
    restart
    fence
    poll    <consumer> <partition> [read_uncommitted]
    seek    <consumer> <partition> <offset>
    recover <partition> <consumer_offset> [read_uncommitted]
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from .broker import Broker, RecoveryReport
from .model import Consumer, IsolationLevel, LogEntry


@dataclass(frozen=True)
class Event:
    kind: str
    transaction_id: Optional[str] = None
    partition: Optional[int] = None
    payload: Optional[str] = None
    consumer: Optional[str] = None
    offset: Optional[int] = None
    isolation: IsolationLevel = IsolationLevel.READ_COMMITTED


_SIMPLE_KINDS = ("restart", "fence")
_TXN_KINDS = ("begin", "data", "commit", "abort")


def parse_event(line: str) -> Event:
    text = line.strip()
    if not text or text.startswith("#"):
        raise ValueError("empty event line")
    parts = text.split()
    kind = parts[0].lower()

    if kind in _SIMPLE_KINDS:
        return Event(kind=kind)

    if kind in _TXN_KINDS:
        if len(parts) < 3:
            raise ValueError("event %r needs <txn_id> <partition>" % kind)
        payload = " ".join(parts[3:]) if kind == "data" else None
        return Event(
            kind=kind,
            transaction_id=parts[1],
            partition=int(parts[2]),
            payload=payload,
        )

    if kind in ("poll", "seek"):
        if len(parts) < 3:
            raise ValueError("event %r needs <consumer> <partition>" % kind)
        if kind == "seek":
            if len(parts) < 4:
                raise ValueError("seek needs <consumer> <partition> <offset>")
            offset = int(parts[3])
            isolation = _parse_isolation(parts[4:])
        else:
            offset = None
            isolation = _parse_isolation(parts[3:])
        return Event(
            kind=kind,
            consumer=parts[1],
            partition=int(parts[2]),
            offset=offset,
            isolation=isolation,
        )

    if kind == "recover":
        if len(parts) < 3:
            raise ValueError(
                "recover needs <partition> <consumer_offset>"
            )
        return Event(
            kind=kind,
            partition=int(parts[1]),
            offset=int(parts[2]),
            isolation=_parse_isolation(parts[3:]),
        )

    raise ValueError("unknown event kind: %s" % kind)


def _parse_isolation(extra: List[str]) -> IsolationLevel:
    if not extra:
        return IsolationLevel.READ_COMMITTED
    value = extra[0].lower()
    if value == "read_uncommitted":
        return IsolationLevel.READ_UNCOMMITTED
    if value == "read_committed":
        return IsolationLevel.READ_COMMITTED
    raise ValueError("unknown isolation level: %s" % value)


def parse_script(script: str) -> List[Event]:
    events = []
    for line in script.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        events.append(parse_event(line))
    return events


class EventRunner:
    """按顺序把事件应用到本地 broker，并收集可读结果。"""

    def __init__(self) -> None:
        self.broker = Broker()
        self.consumers = {}
        self.outputs = []

    def run(self, events: List[Event]) -> List[str]:
        for event in events:
            self.apply(event)
        return list(self.outputs)

    def apply(self, event: Event) -> None:
        if event.kind in _TXN_KINDS:
            entry = self._apply_transaction(event)
            self.outputs.append(
                "p%d off=%d %s txn=%s"
                % (
                    event.partition,
                    entry.offset,
                    entry.kind.value,
                    event.transaction_id,
                )
            )
            return

        if event.kind == "restart":
            fenced = self.broker.restart(fence=False)
            self.outputs.append(
                "broker restarted, replayed partitions=%s fenced=%s"
                % (sorted(self.broker.partitions), fenced)
            )
            return

        if event.kind == "fence":
            fenced = []
            for partition in sorted(self.broker.partitions):
                fenced.extend(self.broker.fence_open_transactions(partition))
            self.outputs.append("fenced open transactions=%s" % fenced)
            return

        if event.kind == "poll":
            consumer = self._get_consumer(event)
            records = consumer.poll(self.broker.partition(event.partition))
            self.outputs.append(
                "consumer=%s poll partition=%d offsets=%s"
                % (
                    consumer.name,
                    event.partition,
                    [record.offset for record in records],
                )
            )
            return

        if event.kind == "seek":
            consumer = self._get_consumer(event)
            consumer.seek(event.partition, event.offset)
            self.outputs.append(
                "consumer=%s seek partition=%d offset=%d"
                % (consumer.name, event.partition, event.offset)
            )
            return

        report = self.broker.analyze_recovery(
            event.partition, event.offset, event.isolation
        )
        self.outputs.append(format_recovery_report(report))

    def _apply_transaction(self, event: Event) -> LogEntry:
        if event.kind == "begin":
            return self.broker.begin(
                event.transaction_id, event.partition
            )
        if event.kind == "data":
            return self.broker.data(
                event.transaction_id, event.partition, event.payload or ""
            )
        if event.kind == "commit":
            return self.broker.commit(
                event.transaction_id, event.partition
            )
        return self.broker.abort(event.transaction_id, event.partition)

    def _get_consumer(self, event: Event) -> Consumer:
        if event.consumer not in self.consumers:
            self.consumers[event.consumer] = Consumer(
                event.consumer, event.isolation
            )
        return self.consumers[event.consumer]


def format_recovery_report(report: RecoveryReport) -> str:
    visible = [
        "off=%d(%s)" % (entry.offset, entry.payload)
        for entry in report.visible_messages
    ]
    unsafe = [
        "[%d,%d):%s" % (item.start, item.end, item.reason)
        for item in report.unsafe_ranges
    ]
    open_ranges = [
        "[%d,%d):%s" % (item.start, item.end, item.reason)
        for item in report.open_ranges
    ]
    lines = [
        "recover partition=%d consumer_offset=%d barrier=%d"
        % (report.partition, report.consumer_offset, report.barrier),
        "  visible=%s" % visible,
        "  rollback_offset=%s" % report.rollback_offset,
        "  unsafe_ranges=%s" % unsafe,
        "  open_ranges=%s" % open_ranges,
        "  warnings=%s" % report.warnings,
    ]
    return "\n".join(lines)
