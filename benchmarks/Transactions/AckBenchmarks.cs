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
/// PlainAck              119 ns /  104 B     113 ns /  104 B
/// PlainAck_Receipt      264 ns /  448 B   2,218 ns / 1560 B
/// TxnAck_SteadyState    474 ns /  984 B   3,236 ns / 2532 B
/// TxnAck_BatchIndex     519 ns / 1072 B   2,839 ns / 2625 B
/// </code>
/// PlainAck was 154 ns / 136 B before followups/0007 was fixed (the three other rows are from that time too). It allocates 104 B again, which is what it allocated on the baseline commit e8f2ba2
/// (the same benchmark and fake connection without the transaction members: 104 B, 109.6 ns and 109.9 ns in the same session). The allocations of P1 are therefore met. The result of the executor
/// was a class (32 B), and the plain acknowledgment called the generic overload of Executor.Execute because it returned the (null) receipt. It is a struct now, and an acknowledgment that does
/// not wait for a receipt calls the overload without a result again, which also drops a layer of async methods (SendAndAwaitReceipt) from that path.
/// The time of P1 is NOT met strictly. BenchmarkDotNet cannot tell, because its standard deviation on this machine is about 20 ns of 110 ns (the runs above and the baseline run overlap).
/// Measured with the baseline build and this build in separate load contexts of one process (20,000 acknowledgments per slice, the two alternating, 7,000 to 12,000 slices per run),
/// the 5th percentile of the slices was 4.5% to 13% above the baseline in six runs (+6% in the middle), the minimum of the slices 1% to 11% above (+8% in the middle) and the median of the slices 6% to 10% above.
/// The cost that is left is the work of the features that were added to the path, which is about 3 ns in ConsumerChannel.Send (the batch handling and the trackers, with a shortcut for the acknowledgment of one
/// entry that is not a batch) and about 1 ns for the reset of the pooled command. The first version of this fix, which only made the result of the executor a struct, was 40% above the baseline.
/// The threshold of TxnAck is NOT met. TxnAck_SteadyState minus PlainAck is 848 B (2396 B on the thread pool), where the awaiter (96 B) and a continuation (64-112 B) allow about 210 B.
/// Minus PlainAck_Receipt, which has the same awaiter, the same timeout (linked token source and timer, 144 B) and the same direct send, it is 536 B (972 B on the thread pool).
/// 344 B of the 536 B is the command: the transactional acknowledgment creates a new CommandAck (152 B), a new MessageIdData (104 B) and the array of its message id list (88 B)
/// for every acknowledgment, where the plain acknowledgment uses the CommandAck of its pool. The other 192 B are two more result tasks of 72 B (the transactional path returns a task from
/// each of ConsumerChannel.SendTransactional, InternalSendTransactional and the local function SendOnce, where the plain path returns one from ConsumerChannel.Send) and a second closure (48 B).
/// TxnAck_BatchIndex adds 88 B for the array of the ack set, which is also new for every acknowledgment, and 0 B for building the ack set itself (see AckSetBenchmarks).
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
