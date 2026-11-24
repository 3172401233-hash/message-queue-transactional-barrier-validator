# 事务消息屏障验证器（transactional barrier validator）

用纯 Python 标准库验证事务消息的屏障语义，定位下面这类恢复缺陷：

> 生产者事务尚未提交时，broker 已经写入部分消息；消费者开始处理，
> 随后事务 abort。恢复时暂停队列只记录最后偏移，无法排除未提交消息。

## 本地模型声明

**所有消息流均为本地模型。** 本项目不启动真实 broker，不发起任何网络请求，
也不依赖 Kafka、RocketMQ 等外部系统。分区日志、事务标记、消费者偏移、
broker 重启回放和暂停恢复全部在单个 Python 进程内模拟，仅用于验证
事务边界、可见性和恢复算法。

## 环境

- Python 3.8.2+（仅使用标准库；代码兼容 3.8 语法）
- 无第三方依赖

## 运行

```bash
python -m unittest
python -m tx_barrier
```

## 核心概念

- 分区日志按追加顺序分配单调递增的日志偏移。
- `BEGIN`、`COMMIT`、`ABORT` 是事务边界标记，占用日志偏移但不投递给消费者。
- `DATA` 是事务内数据，其可见性分为：
  - `visible`：事务已提交，且偏移位于屏障之前。
  - `aborted`：事务已中止，任何隔离级别下的安全消费者都不得消费。
  - `pending`：事务尚未结束，被当前屏障拦截。
- 屏障（barrier）采用 last-stable-offset 风格，等于最老未完成事务的
  `BEGIN` 偏移；没有未完成事务时等于日志末尾。

### 屏障必须覆盖事务边界

只要存在未完成事务，屏障就停留在该事务的起始偏移。因此：

- 未提交事务之后的另一个已提交事务也必须等待，不能越过屏障。
- `abort` 消息不能因为位于消费者暂停记录的“最后偏移”之前就被消费；
  恢复时要按事务范围而不是单个偏移判定。
- `read_committed` 消费者在屏障处停止；`read_uncommitted` 仅用于复现
  消费者越过屏障的错误场景。

## 恢复输出

`Broker.analyze_recovery(partition, consumer_offset)` 返回 `RecoveryReport`：

- `visible_messages`：当前允许消费的已提交消息。
- `rollback_offset`：需要回滚到的安全偏移；为 `None` 表示无需回滚。
- `unsafe_ranges`：无法安全恢复的半开日志区间 `[start, end)`，覆盖已中止
  或仍处于 OPEN 状态、且被消费者偏移越过的事务范围。
- `open_ranges`：broker 重启后仍缺少结束标记的事务范围。
- `barrier`：当前事务屏障。
- `warnings`：越界、中止区间重叠等诊断信息。

判定规则：

1. 消费者偏移大于屏障：回滚到屏障。
2. 消费者偏移落在已中止事务范围内：回滚到该事务 `BEGIN` 偏移。
3. 仍为 OPEN 的事务没有结束标记，本地无法判断最终提交/中止，
   重启后通过 `fence_open_transactions()` 按超时围栏显式中止，再恢复。

## 事件脚本

可通过文本事件描述消息流，由 `tx_barrier.events` 解析和回放：

```text
begin   <txn_id> <partition>
data    <txn_id> <partition> <payload...>
commit  <txn_id> <partition>
abort   <txn_id> <partition>
restart
fence
poll    <consumer> <partition> [read_uncommitted]
seek    <consumer> <partition> <offset>
recover <partition> <consumer_offset> [read_uncommitted]
```

示例：

```python
from tx_barrier import EventRunner, parse_script

script = """
begin T1 0
data T1 0 uncommitted
abort T1 0
recover 0 2
"""
runner = EventRunner()
print("\n".join(runner.run(parse_script(script))))
```

## 测试覆盖

`tests/test_transaction_barrier.py` 共 16 个用例，覆盖：

- 事务中断：未提交数据隐藏、abort 后永不可见、暂停偏移位于 abort 标记
  之前时仍正确回滚。
- 重复提交/重复开启：同一事务 ID 重复 `BEGIN`、重复 `COMMIT/ABORT`
  抛出 `TransactionConflictError`；未知事务操作抛出
  `UnknownTransactionError`；提交标记可在重启回放后保留。
- 消费者越过屏障：未完成事务阻塞后续已提交数据；越界偏移被识别并回滚；
  `read_committed` 消费者绝不返回中止记录。
- broker 重启：从日志条目重建事务状态，OPEN 事务保持未完成；
  fence 后孤儿事务被中止，已提交数据保持可见。

## 目录

```text
tx_barrier/
  __init__.py     公共 API
  __main__.py     本地演示
  model.py        日志、事务、屏障、消费者模型
  broker.py       broker、重启回放、恢复报告
  events.py       文本事件解析与回放
tests/
  test_transaction_barrier.py
```
