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

/// <summary>
/// The lifecycle benchmarks run in-process against a fake connection that answers every message with a receipt, and against a fake transaction coordinator client.
/// A transaction is created with the client, used and committed. The fake coordinator answers at once, so what is measured is the work and the allocations of the client.
/// <para>
/// Threshold (documented in the spec 07-performance.md): the allocations of a transaction are independent of the number of messages beyond the cost of the messages themselves.
/// CreateCommit_Empty is the cost of a transaction without messages (the transaction, its dictionaries, its timer, its state manager). CreateCommit_100Sends sends 100 messages in the
/// transaction, one after the other, and commits. Plain_100Sends sends the same 100 messages without a transaction. CreateCommit_100Sends minus CreateCommit_Empty minus Plain_100Sends
/// is what the transaction adds for its messages. That is the registration of the partition with the transaction, which is done once per transaction, and nothing for every message.
/// </para>
/// <para>
/// Measured (Medium job, in-process, Release, .NET 10, Intel Core Ultra 9 185H), per transaction, direct completion / thread-pool completion. The operation of CreateCommit_100Sends and of
/// Plain_100Sends is a series of 100 messages. The allocated bytes of the empty transaction do not change from run to run. The other ones change by about 100 B on the thread pool.
/// <code>
/// CreateCommit_Empty           0.7-0.8 us /   4200 B       0.8-0.9 us /   4200 B
/// CreateCommit_100Sends      352-354 us   / 296712 B     371-426 us   / 302219 B
/// Plain_100Sends             334-341 us   / 292517 B     422-430 us   / 298114 B
/// </code>
/// The threshold is met. CreateCommit_100Sends minus CreateCommit_Empty minus Plain_100Sends is -5 B (-95 B on the thread pool, which is noise): a transaction costs the same
/// 4200 B whether it has 0 or 100 messages, and the 100 messages cost what they cost without a transaction (2925 B each). The registration of the partition is not measurable.
/// The 4200 B of an empty transaction are more than the "about 10 allocations" of the spec. They are made of 2464 B for the two ConcurrentDictionary instances (1232 B each: their default
/// concurrency level is the number of processors, 22 here, so each has 22 lock objects), about 1.1 KB of boxed TransactionState values (StateManager.IsFinalState calls the non-generic
/// Equals of an unconstrained generic type, which boxes both values: 48 B for every final state it compares, and the state changes of a transaction call it about 23 times)
/// and about 0.6 KB for the transaction, its timer, its state manager and the task that commit waits on. This does not depend on the number of messages.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class TransactionLifecycleBenchmarks
{
    private const int _emptyTransactions = 1000;
    private const int _transactions = 10;
    private const int _messages = 100;

    private readonly byte[] _message = new byte[100];
    private readonly TransactionOptions _options = new() { Timeout = TimeSpan.FromHours(1) };
    private PulsarClient? _client;
    private IProducer<byte[]>? _producer;

    [Params(false, true)]
    public bool CompleteOnThreadPool { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var transactionCoordinatorClient = new FakeTransactionCoordinatorClient();
        _client = PulsarClientFactory.CreatePulsarClient(new FakeConnectionPool(new FakeConnection(CompleteOnThreadPool)), new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"), transactionCoordinatorClient);
        _producer = _client.NewProducer(Schema.ByteArray).Topic("persistent://public/default/lifecycle").Create();

        await _producer.StateChangedTo(ProducerState.Connected);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _producer!.DisposeAsync();
        await _client!.DisposeAsync();
    }

    // The operation is one transaction
    [Benchmark(Baseline = true, OperationsPerInvoke = _emptyTransactions)]
    public async Task CreateCommit_Empty()
    {
        for (var i = 0; i < _emptyTransactions; ++i)
        {
            var transaction = await _client!.CreateTransaction(_options);
            await transaction.Commit();
        }
    }

    // The operation is one transaction with 100 messages
    [Benchmark(OperationsPerInvoke = _transactions)]
    public async Task CreateCommit_100Sends()
    {
        for (var i = 0; i < _transactions; ++i)
        {
            var transaction = await _client!.CreateTransaction(_options);

            for (var j = 0; j < _messages; ++j)
                await Send(transaction);

            await transaction.Commit();
        }
    }

    // The operation is 100 messages without a transaction. It is the cost of the messages in CreateCommit_100Sends.
    [Benchmark(OperationsPerInvoke = _transactions)]
    public async Task Plain_100Sends()
    {
        for (var i = 0; i < _transactions; ++i)
        {
            for (var j = 0; j < _messages; ++j)
                await Send(null);
        }
    }

    // The same as the extension method that sends without metadata, with a transaction added
    private async Task Send(ITransaction? transaction)
    {
        var metadata = MessageMetadataObjectPool.Get();
        metadata.Transaction = transaction;

        try
        {
            _ = await _producer!.Send(metadata, _message, CancellationToken.None);
        }
        finally
        {
            MessageMetadataObjectPool.Return(metadata);
        }
    }
}
