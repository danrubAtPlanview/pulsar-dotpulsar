# 0007 - Ack path allocations: P1 (plain ack) and P2 (transactional ack) thresholds are not met

Status: **fixed and re-measured in WP10** in `Avoid allocating a result for a plain ack` (P1) and `Pool the command of a transactional ack` (P2), merged on `feature/transactions`. The benchmarks were re-run on the final tip in WP10 (last section): P1 and P2 are met, the plain-ack time is within the noise of the baseline.

Result (in-process, Release, .NET 10, medium job; allocated bytes are exact, times are indicative with about 20 ns standard deviation), direct / thread-pool completion:

| | baseline e8f2ba2 | before | after P1 | after P1+P2 |
|---|---|---|---|---|
| PlainAck | 104 B, 110 ns | 136 B, 154 / 182 ns | 104 B | 104 B, 110 / 103 ns |
| PlainAck_Receipt | n/a | 448 / 1560 B | 418 B | 416 / 1563 B |
| TxnAck_SteadyState | n/a | 984 / 2532 B | 952 / 2511 B | 424 / 1572 B (minus PlainAck_Receipt: 8 B / 9 B) |
| TxnAck_BatchIndex | n/a | 1072 / 2625 B | 1040 / 2606 B | 424 / 1575 B |

What changed: `ExecutionResult<T>` is a `readonly struct`; an ack that does not wait for a receipt calls the non-generic `Execute` again; single-id transactional acks (individual and cumulative) use `_commandAckPool` and reuse `MessageId[0]` incl. its ack set, returned in a `finally`; `SendOnce` returns a `ValueTask`, `InternalSendTransactional` is gone, the two single-id public methods are no longer `async`. The list form of the transactional ack stays unpooled. Chunked ids use the unpooled path (the merge with WP8 consumer, see `SubConsumer.Acknowledge(MessageId, ITransaction, ...)`). Behaviour risk: `SubConsumer.Acknowledge(MessageId, ITransaction)` and `AcknowledgeCumulative(MessageId, ITransaction)` now throw argument/`EnsureOpen` exceptions from the call, not from the returned `ValueTask` (callers inside `Consumer<T>` await them, so no difference). Measured by the agent in one process with two load contexts: plain ack +6% in the middle vs baseline, transactional ack 28% faster than before. Not done: state-passing `Execute` overloads (would remove the closure on the plain path but change `IExecute`).

## Original findings (kept for the record)
Found by the `AckBenchmarks` (commit `Add ack, ack set and lifecycle benchmarks`). Medium job, `--inProcess`, Release, .NET 10; allocated bytes are exact, times vary 10-20% between runs.

| Benchmark | Direct completion | Thread-pool completion |
|---|---|---|
| `PlainAck` (current) | 154 ns / 136 B | 182 ns / 136 B |
| `PlainAck` on master `e8f2ba2` (baseline harness) | 104-115 ns / 104 B | |
| `PlainAck_Receipt` (plain ack awaiting a receipt, the fair baseline) | 264 ns / 448 B | 2218 ns / 1560 B |
| `TxnAck_SteadyState` | 474 ns / 984 B | 3236 ns / 2532 B |
| `TxnAck_BatchIndex` | 519 ns / 1072 B | 2839 ns / 2625 B |
| `BuildAckSet_64`, `BuildAckSet_1000` (and the tracker variants) | 0 B | 0 B |

## 1. P1: the plain ack regressed by 32 B and about 35-45% time

`ExecutionResult<TResult>` is a `sealed class`. Since WP5/WP1 `InternalAcknowledge` returns the receipt, so the plain ack goes through the generic `Executor.Execute<T>`, which allocates it (32 B).
Fix options: make `ExecutionResult<TResult>` a `readonly struct`, or keep a non-generic path when no receipt is awaited. Time regression not profiled: likely the extra async layers and `PrepareAck`.
Acceptance: plain ack allocates 104 B again and is within noise (about 5%) of `e8f2ba2`.

## 2. P2: `TxnAck_SteadyState` is +848 B over `PlainAck`; the spec allows the awaiter (96 B) plus one continuation (64-112 B), about 210 B

Against `PlainAck_Receipt` (same awaiter and timeout) it is +536 B (+972 B on the thread pool):
- 344 B: a fresh `CommandAck` (152 B), `MessageIdData` (104 B) and `MessageId[]` (88 B) per ack. WP5 did not use `_commandAckPool`.
- 144 B: two extra `Task<BaseCommand>` results from the extra async layers (`ConsumerChannel.SendTransactional`, `InternalSendTransactional`, the `SendOnce` local function). 48 B: a second closure.
- `TxnAck_BatchIndex` adds 88 B for the `long[]` ack set (also fresh per ack; building the ack set itself is 0 B).

Proposed fix: pool the `CommandAck` for the single-id transactional ack (reuse `MessageId[0]`, reset `AckSet` and `BatchSize` in `ResetPooledAck`; return in `finally`, safe because the command is awaited inside the same method, as the plain path does);
make `InternalSendTransactional` return a `ValueTask`; replace the `SendOnce` closure by checking `ProtocolVersion` outside the executor. Spec 07-performance section 4.2 assumes the pooled command.
Acceptance: `TxnAck_SteadyState` minus `PlainAck_Receipt` at most one continuation (64-112 B).

## 3. Done already

An empty transaction allocated 4200 B (two `ConcurrentDictionary` instances with a lock per processor, boxed `TransactionState` in `StateManager`). Fixed in `Allocate less for a state change and a transaction`: 1.09 KB.
The `StateManager` fix also removes 48 B per final-state comparison on every producer/consumer/reader state change (pre-existing, on master).

## 4. Not covered by these benchmarks

The receipt is simulated with one task completion source, so the real connection's request-id handling and dictionary entry are not in the numbers. `TxnAck_BatchIndex` does not deliver a real batch (no `BatchHandler` tracker); it takes the
`data.BatchSize` path, which has the same allocations as the tracker path.

## Re-measured on the final tip (WP10, `339d734 (the ids of the history have changed since; the later commits only add a guard to `TransactionMetaStoreOp.TryBeginSend` and fix tests, which the benchmarks do not touch)`)

In-process (`--inProcess`), Release, .NET 10, default job, the machine was quiet (no brokers or test runs); allocated bytes are exact, times are indicative (the standard deviation of the ack benchmarks is 3 to 9 ns). Direct completion / thread-pool completion.

| Benchmark | Time | Allocated | Against |
|---|---|---|---|
| `PlainAck` | 84 ns / 82 ns | 104 B / 104 B | baseline e8f2ba2: 104 B, 104-115 ns. **P1 is met** (the allocation is the same and the time is within the noise; the +6% of the earlier measurement is gone) |
| `PlainAck_Receipt` | 216 ns / 1680 ns | 416 B / 1558 B | |
| `TxnAck_SteadyState` | 251 ns / 1779 ns | 424 B / 1562 B | +8 B / +4 B over `PlainAck_Receipt`. **P2 is met** (at most one continuation) |
| `TxnAck_BatchIndex` | 283 ns / 1829 ns | 424 B / 1565 B | |
| `PlainSend` | 3.07 us / 3.47 us | 2.88 KB / 2.93 KB | |
| `TxnSend_SteadyState` | 3.31 us / 3.54 us | 2.88 KB / 2.93 KB | same allocation as `PlainSend` (+8% / +2% time) |
| `TxnSend_FirstPerPartition` | 4.16 us / 4.50 us | 4.06 KB / 4.10 KB | +1.2 KB for the registration of the partition |
| `Send_NoBatching_1000` / `Send_Batching_1000` (per message) | 2.00 us / 1.29 us (thread pool: 2.00 / 1.35) | 4.09 KB / 1.22 KB (4.28 / 1.24) | batching: **1.55x** the throughput (ratio 0.65) and 30% of the allocation. The target of 3x is not met (not profiled: probably the per-message part of the send path, the queue and the completion of the receipt, dominates) |
| `EncryptedSend_100Bytes` | 3.85 us (plain 3.15 us) | 3.11 KB (plain 2.88 KB) | +229 B and +22% time |
| `ChunkingEnabled_Send_100Bytes` | 3.37 us (plain 3.21 us) | 3.43 KB (plain 2.88 KB) | chunking enabled, a message that is not chunked: +5% time, +0.55 KB |
| `ChunkingEnabled_Send_10Chunks` | 8.1 us | 13.2 KB | 10 chunks; with encryption 17.0 us / 29.3 KB |
| `CreateCommit_Empty` | 297 ns / 291 ns | 1.09 KB | the transaction itself |
| `BuildAckSet_64`, `BuildAckSet_1000`, tracker variants `_64`, `_1000` | 15 / 65 / 4 / 17 ns | 0 B | the threshold of 0 B is met |
| `CreateCommit_100Sends` vs `Plain_100Sends` | 326 us vs 339 us / 361 us vs 347 us | 288 KB vs 287 KB / 293 KB vs 292 KB | +1.1 KB for 100 transactional sends in one transaction |

The 3x target of batching stays open as a performance item. The benchmark measures the whole send path with a fake connection, where a send costs 2 us without batching; probably the part of it that batching does not remove (the queue, the receipt and the completion of every message) limits the gain, but that was not profiled. It is not a regression: the unbatched send is the same as before.

## Closing of the 3x threshold

Decided by the requester on 2026-11-01: the measured 1.55x throughput and 3.3x less allocation per message are accepted as the criterion of batching in this work. Nothing is left to do in this work for it. Meeting 3x would be a separate performance project on the per-message send path of the producer (about 3.5x cheaper per message), which is code of master.
