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
/// The batching benchmarks send 1000 messages of 100 bytes through the send channel of a producer, and wait until the last message is acknowledged by a fake connection that serializes the frames and
/// answers every entry with a receipt. A batch of 1000 messages is one frame and one receipt, and the messages are written to it without a frame of their own.
/// <para>
/// The spec 07-performance.md asks for a throughput that is at least 3 times as high with batching. The fake connection has no network, so only the cost of the frames is saved, and the cost that a producer has
/// for every message (the send operation, its task and its receipt) is the same with and without batching. The measured gain is about 1.5 times (1.6 us and 2.5 us per message in-process, Release, .NET 10),
/// and the allocated memory per message is a quarter (1.2 KB and 4.1 KB).
/// </para>
/// </summary>
[MemoryDiagnoser]
public class BatchingBenchmarks
{
    private const int _operations = 1000;

    private readonly byte[] _message = new byte[100];
    private PulsarClient? _noBatchingClient;
    private PulsarClient? _batchingClient;
    private IProducer<byte[]>? _noBatchingProducer;
    private IProducer<byte[]>? _batchingProducer;

    [Params(false, true)]
    public bool CompleteOnThreadPool { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _noBatchingClient = PulsarClientFactory.CreatePulsarClient(new FakeConnectionPool(new FakeConnection(CompleteOnThreadPool, true)), new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"));
        _noBatchingProducer = _noBatchingClient.NewProducer(Schema.ByteArray).Topic("persistent://public/default/no-batching").MaxPendingMessages(_operations).Create();

        _batchingClient = PulsarClientFactory.CreatePulsarClient(new FakeConnectionPool(new FakeConnection(CompleteOnThreadPool, true)), new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"));
        _batchingProducer = _batchingClient
            .NewProducer(Schema.ByteArray)
            .Topic("persistent://public/default/batching")
            .EnableBatching(true)
            .MaxPendingMessages(_operations)
            .BatchingMaxMessages(_operations)
            .BatchingMaxPublishDelay(TimeSpan.FromHours(1))
            .Create();

        await _noBatchingProducer.StateChangedTo(ProducerState.Connected);
        await _batchingProducer.StateChangedTo(ProducerState.Connected);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _noBatchingProducer!.DisposeAsync();
        await _batchingProducer!.DisposeAsync();
        await _noBatchingClient!.DisposeAsync();
        await _batchingClient!.DisposeAsync();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = _operations)]
    public async Task Send_NoBatching_1000()
        => await SendAll(_noBatchingProducer!);

    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task Send_Batching_1000()
        => await SendAll(_batchingProducer!);

    private async Task SendAll(IProducer<byte[]> producer)
    {
        for (var i = 0; i < _operations; ++i)
        {
            await producer.SendChannel.Send(_message);
        }

        await producer.Flush();
    }
}
