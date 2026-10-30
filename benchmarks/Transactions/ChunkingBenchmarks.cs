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
/// The chunking benchmarks send messages through a fake connection that answers every chunk with a receipt. The maximum message size of the connection is 2048 bytes.
/// A producer that does not have chunking enabled is not changed by chunking: PlainSend allocates the same as before chunking was added.
/// A producer that has chunking enabled copies the metadata and plans the chunks of every message, also when the message is not split, which is what ChunkingEnabled_Send_100Bytes costs more than PlainSend.
/// The message of ChunkingEnabled_Send_10Chunks is split in 10 chunks.
/// </summary>
[MemoryDiagnoser]
public class ChunkingBenchmarks
{
    private const int _operations = 1000;

    private readonly byte[] _smallMessage = new byte[100];
    private readonly byte[] _largeMessage = new byte[10 * 1900];
    private PulsarClient? _plainClient;
    private PulsarClient? _chunkingClient;
    private IProducer<byte[]>? _plainProducer;
    private IProducer<byte[]>? _chunkingProducer;

    [GlobalSetup]
    public async Task Setup()
    {
        _plainClient = PulsarClientFactory.CreatePulsarClient(new FakeConnectionPool(new FakeConnection(false, false, 2048)), new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"));
        _plainProducer = _plainClient.NewProducer(Schema.ByteArray).Topic("persistent://public/default/plain").Create();

        _chunkingClient = PulsarClientFactory.CreatePulsarClient(new FakeConnectionPool(new FakeConnection(false, false, 2048)), new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"));
        _chunkingProducer = _chunkingClient.NewProducer(Schema.ByteArray).Topic("persistent://public/default/chunking").EnableChunking(true).Create();

        await _plainProducer.StateChangedTo(ProducerState.Connected);
        await _chunkingProducer.StateChangedTo(ProducerState.Connected);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _plainProducer!.DisposeAsync();
        await _chunkingProducer!.DisposeAsync();
        await _plainClient!.DisposeAsync();
        await _chunkingClient!.DisposeAsync();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = _operations)]
    public async Task PlainSend_100Bytes()
    {
        for (var i = 0; i < _operations; ++i)
            _ = await _plainProducer!.Send(_smallMessage);
    }

    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task ChunkingEnabled_Send_100Bytes()
    {
        for (var i = 0; i < _operations; ++i)
            _ = await _chunkingProducer!.Send(_smallMessage);
    }

    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task ChunkingEnabled_Send_10Chunks()
    {
        for (var i = 0; i < _operations; ++i)
            _ = await _chunkingProducer!.Send(_largeMessage);
    }
}
