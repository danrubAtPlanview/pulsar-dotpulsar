/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Transactions;

using BenchmarkDotNet.Attributes;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using DotPulsar.Internal;
using DotPulsar.Internal.Transactions;
using System.Reflection;

/// <summary>
/// The acknowledgment benchmarks run in-process against a fake connection that answers every acknowledgment that asks for a receipt, and against a fake transaction coordinator client.
/// The fake allocates one task completion source for every receipt, which is the awaiter that the real connection needs too. The allocations of the fake connection are the same
/// for every benchmark that waits for a receipt, so the allocated bytes of those benchmarks can be compared with each other.
/// <para>
/// Thresholds (documented in the spec 07-performance.md):
/// PlainAck must not allocate more or run slower than it did before transactions were added. It does not wait for a receipt, which is the default of a consumer.
/// TxnAck_SteadyState minus PlainAck is at most the size of the AckResponse awaiter and one continuation. A transactional acknowledgment always waits for the receipt of the broker,
/// and a receipt needs an awaiter that the broker answers, which is unavoidable (Java allocates a CompletableFuture too). PlainAck_Receipt is the plain acknowledgment that waits
/// for the receipt too (AckReceiptEnabled), so TxnAck_SteadyState minus PlainAck_Receipt is what the transaction adds. TxnAck_BatchIndex adds the building of the ack set.
/// The awaiter of the fake is one task completion source and its task (96 B).
/// </para>
/// <para>
/// Measured (Medium job, in-process, Release, .NET 10, Intel Core Ultra 9 185H), nanoseconds and allocated bytes per acknowledgment, direct completion / thread-pool completion.
/// The allocated bytes do not change from run to run when the completion is direct, and change by about 10 B when it is on the thread pool. The times change by about 10-20%.
/// <code>
/// PlainAck              110 ns /  104 B     103 ns /  104 B
/// PlainAck_Receipt      307 ns /  416 B   1,985 ns / 1563 B
/// TxnAck_SteadyState    319 ns /  424 B   2,273 ns / 1572 B
/// TxnAck_BatchIndex     362 ns /  424 B   2,089 ns / 1575 B
/// </code>
/// Before followups/0007 was fixed, it was 154 ns / 136 B and 182 ns / 136 B for PlainAck, 264 ns / 448 B and 2,218 ns / 1560 B for PlainAck_Receipt, 474 ns / 984 B and 3,236 ns / 2532 B
/// for TxnAck_SteadyState, and 519 ns / 1072 B and 2,839 ns / 2625 B for TxnAck_BatchIndex.
/// </para>
/// <para>
/// P1 (PlainAck) is met for the allocations and nearly for the time.
/// PlainAck allocates 104 B again, which is what it allocated on the baseline commit e8f2ba2 (the same benchmark and fake connection without the transaction members: 104 B, 109.6 ns and 109.9 ns
/// in the same session). The result of the executor was a class (32 B), and the plain acknowledgment called the generic overload of Executor.Execute because it returned the (null) receipt.
/// The result is a struct now, and an acknowledgment that does not wait for a receipt calls the overload without a result again, which also drops a layer of async methods (SendAndAwaitReceipt) from that path.
/// BenchmarkDotNet cannot tell the time, because its standard deviation on this machine is about 20 ns of 110 ns (the runs above and the baseline run overlap).
/// Measured with the baseline build and this build in separate load contexts of one process (20,000 acknowledgments per slice, the two alternating, 5,000 to 12,000 slices per run, nine runs),
/// the 5th percentile of the slices was 3% below to 13% above the baseline (+6% in the middle), the minimum of the slices 2% below to 11% above (+9% in the middle), and the median of the slices 3% to 10% above.
/// The cost that is left is the work of the features that were added to the path, which is about 3 ns in ConsumerChannel.Send (the batch handling and the trackers, with a shortcut for the acknowledgment of one
/// entry that is not a batch) and about 1 ns for the reset of the pooled command. The first version of this fix, which only made the result of the executor a struct, was 40% above the baseline.
/// </para>
/// <para>
/// P2 (TxnAck) is met. TxnAck_SteadyState minus PlainAck_Receipt, which has the same awaiter, the same timeout (linked token source and timer, 144 B) and the same send, is 8 B (9 B on the thread pool),
/// and TxnAck_BatchIndex minus PlainAck_Receipt is 8 B (12 B on the thread pool), where one continuation (64-112 B) is allowed. It was 536 B (972 B on the thread pool).
/// What was done, in bytes per acknowledgment with direct completion:
/// the command of a transactional acknowledgment of one message id comes from the pool of the plain acknowledgments, with its MessageIdData and the array of its ack set
/// (a RepeatedField keeps its array when it is cleared), where it was a new CommandAck (152 B), MessageIdData (104 B), array of the message id list (88 B) and, for a message in a batch, ack set (88 B) for every acknowledgment (-344 B, -432 B for a message in a batch);
/// the local function SendOnce returns a value task and calls the channel itself, where the transactional path returned a task from each of ConsumerChannel.SendTransactional, InternalSendTransactional and SendOnce (-144 B);
/// the closure of the plain acknowledgment is created in its own method and not when Send is entered, which it was also for a transactional acknowledgment (-40 B);
/// ExecutionResult is a struct (-32 B, for every acknowledgment that waits for a receipt).
/// The 8 B that are left are the closure of SendOnce (48 B), which is 8 B bigger than the closure of a plain acknowledgment (40 B).
/// On the thread pool, where the receipt has not arrived when the acknowledgment waits for it, every async method that waits allocates its state machine, and what was left was the methods that the receipt does not need:
/// the acknowledgment is an operation of the transaction that begins and ends in the method of the acknowledgment (Transaction.BeginOp and EndOp), and no longer a task that is registered with a continuation
/// (ContinueWith, and a method of its own for the task), and the two acknowledgments of one message id are not async methods that await another. Together with the rest, they take the difference to PlainAck_Receipt on the thread pool from 972 B to 9 B.
/// The transactional acknowledgment is 28-30% faster than before this change in runs that alternate the two builds (direct completion, 5th percentile and median), which is more than its allocations alone explain.
/// The fake connection does not have the request id handling and the dictionary entry of the real one, which every acknowledgment that waits for a receipt needs (see followups/0007, section 4).
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AckBenchmarks
{
    private const int _operations = 1000;
    private const string _topic = "persistent://public/default/ack";
    private const int _batchSize = 64;

    private PulsarClient? _plainClient;
    private PulsarClient? _transactionClient;
    private IConsumer<byte[]>? _plainConsumer;
    private IConsumer<byte[]>? _receiptConsumer;
    private IConsumer<byte[]>? _transactionConsumer;
    private FakeTransactionCoordinatorClient? _transactionCoordinatorClient;
    private Transaction? _transaction;
    private MessageId? _messageId;
    private MessageId? _batchMessageId;

    [Params(false, true)]
    public bool CompleteOnThreadPool { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _transactionCoordinatorClient = new FakeTransactionCoordinatorClient();

        _plainClient = PulsarClientFactory.CreatePulsarClient(new FakeConnectionPool(new FakeConsumerConnection(CompleteOnThreadPool)), new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"));
        _plainConsumer = _plainClient.NewConsumer(Schema.ByteArray).SubscriptionName("plain").Topic(_topic).Create();
        _receiptConsumer = _plainClient.NewConsumer(Schema.ByteArray).SubscriptionName("receipt").Topic(_topic).AckReceiptEnabled(true).Create();

        _transactionClient = PulsarClientFactory.CreatePulsarClient(new FakeConnectionPool(new FakeConsumerConnection(CompleteOnThreadPool)), new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"), _transactionCoordinatorClient);
        _transactionConsumer = _transactionClient.NewConsumer(Schema.ByteArray).SubscriptionName("transaction").Topic(_topic).Create();

        await _plainConsumer.StateChangedTo(ConsumerState.Active);
        await _receiptConsumer.StateChangedTo(ConsumerState.Active);
        await _transactionConsumer.StateChangedTo(ConsumerState.Active);

        _messageId = new MessageId(1, 1, -1, -1, _topic);
        _batchMessageId = CreateBatchMessageId(1, 2, 5, _topic, _batchSize);

        // The subscription is registered with the transaction by the first acknowledgment, so the acknowledgments of the benchmark are steady state
        _transaction = new Transaction(_transactionCoordinatorClient, new TransactionId(0, 0), TimeSpan.FromHours(1));
        await _transactionConsumer.Acknowledge(_messageId, _transaction);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _plainConsumer!.DisposeAsync();
        await _receiptConsumer!.DisposeAsync();
        await _transactionConsumer!.DisposeAsync();
        await _plainClient!.DisposeAsync();
        await _transactionClient!.DisposeAsync();
        await _transaction!.Abort(CancellationToken.None);
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = _operations)]
    public async Task PlainAck()
    {
        for (var i = 0; i < _operations; ++i)
            await _plainConsumer!.Acknowledge(_messageId!);
    }

    // The plain acknowledgment that waits for the receipt of the broker, as a transactional acknowledgment always does
    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task PlainAck_Receipt()
    {
        for (var i = 0; i < _operations; ++i)
            await _receiptConsumer!.Acknowledge(_messageId!);
    }

    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task TxnAck_SteadyState()
    {
        for (var i = 0; i < _operations; ++i)
            await _transactionConsumer!.Acknowledge(_messageId!, _transaction!);
    }

    // The id of a message in a batch carries the batch size, as the ids that the consumer delivers do. It makes the channel build the ack set of the batch.
    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task TxnAck_BatchIndex()
    {
        for (var i = 0; i < _operations; ++i)
            await _transactionConsumer!.Acknowledge(_batchMessageId!, _transaction!);
    }

    // The batch size of a message id is internal, and the library has no InternalsVisibleTo. The id is created once, outside of the measurements.
    private static MessageId CreateBatchMessageId(ulong ledgerId, ulong entryId, int batchIndex, string topic, int batchSize)
        => (MessageId) Activator.CreateInstance(
            typeof(MessageId),
            BindingFlags.Instance | BindingFlags.NonPublic,
            null,
            [ledgerId, entryId, -1, batchIndex, topic, batchSize],
            null)!;
}
