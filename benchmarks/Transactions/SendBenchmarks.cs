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

/// <summary>
/// The send benchmarks run in-process against a fake connection that answers every message with a receipt, and against a fake transaction coordinator client.
/// The allocations of the fake connection are the same for every benchmark, so the allocated bytes of the benchmarks can be compared with each other.
/// <para>
/// Thresholds (documented in the spec 07-performance.md):
/// PlainSend must not allocate more or run slower than it did before transactions were added.
/// TxnSend_SteadyState minus PlainSend_Metadata, which sends in exactly the same way without a transaction, is at most the size of one continuation object
/// (about 64-112 bytes depending on the runtime). The producer allocates no continuation for the transaction, so the expected difference is 0 bytes.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class SendBenchmarks
{
    private const int _operations = 1000;

    private readonly byte[] _message = new byte[100];
    private PulsarClient? _plainClient;
    private PulsarClient? _transactionClient;
    private IProducer<byte[]>? _plainProducer;
    private IProducer<byte[]>? _transactionProducer;
    private FakeTransactionCoordinatorClient? _transactionCoordinatorClient;
    private Transaction? _transaction;

    [Params(false, true)]
    public bool CompleteOnThreadPool { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _transactionCoordinatorClient = new FakeTransactionCoordinatorClient();

        _plainClient = PulsarClientFactory.CreatePulsarClient(new FakeConnectionPool(new FakeConnection(CompleteOnThreadPool)), new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"));
        _plainProducer = _plainClient.NewProducer(Schema.ByteArray).Topic("persistent://public/default/plain").Create();

        _transactionClient = PulsarClientFactory.CreatePulsarClient(new FakeConnectionPool(new FakeConnection(CompleteOnThreadPool)), new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"), _transactionCoordinatorClient);
        _transactionProducer = _transactionClient.NewProducer(Schema.ByteArray).Topic("persistent://public/default/transaction").Create();

        await _plainProducer.StateChangedTo(ProducerState.Connected);
        await _transactionProducer.StateChangedTo(ProducerState.Connected);

        // The partition is registered with the transaction by the first send, so the sends of the benchmark are steady state
        _transaction = new Transaction(_transactionCoordinatorClient, new TransactionId(0, 0), TimeSpan.FromHours(1));
        await Send(_transactionProducer, _transaction);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _plainProducer!.DisposeAsync();
        await _transactionProducer!.DisposeAsync();
        await _plainClient!.DisposeAsync();
        await _transactionClient!.DisposeAsync();
        await _transaction!.Abort(CancellationToken.None);
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = _operations)]
    public async Task PlainSend()
    {
        for (var i = 0; i < _operations; ++i)
            _ = await _plainProducer!.Send(_message);
    }

    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task PlainSend_Metadata()
    {
        for (var i = 0; i < _operations; ++i)
            await Send(_plainProducer!, null);
    }

    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task TxnSend_SteadyState()
    {
        for (var i = 0; i < _operations; ++i)
            await Send(_transactionProducer!, _transaction);
    }

    // Every send is the first send of a new transaction, so it includes the creation of the transaction, the registration of the partition and the commit
    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task TxnSend_FirstPerPartition()
    {
        for (var i = 0; i < _operations; ++i)
        {
            var transaction = new Transaction(_transactionCoordinatorClient!, new TransactionId(0, (ulong) i), TimeSpan.FromHours(1));
            await Send(_transactionProducer!, transaction);
            await transaction.Commit(CancellationToken.None);
        }
    }

    // The same as the extension method that sends without metadata, with a transaction added
    private async Task Send(IProducer<byte[]> producer, Transaction? transaction)
    {
        var metadata = MessageMetadataObjectPool.Get();
        metadata.Transaction = transaction;

        try
        {
            _ = await producer.Send(metadata, _message, CancellationToken.None);
        }
        finally
        {
            MessageMetadataObjectPool.Return(metadata);
        }
    }
}
