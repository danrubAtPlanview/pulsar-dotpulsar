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
using System.Text;

/// <summary>
/// The encryption benchmarks send messages through a fake connection that answers every message and every chunk with a receipt. The maximum message size of the connection is 2048 bytes.
/// A producer without encryption keys is not changed by encryption: PlainSend_100Bytes allocates the same as before encryption was added.
/// The encrypted producer wraps the data key (RSA) when it is created, and not when it sends, so what EncryptedSend_100Bytes costs more than PlainSend_100Bytes is the AES-GCM encryption,
/// the output buffer (the message plus the 16 bytes of the tag), the IV and the metadata that has to be reset.
/// The message of EncryptedChunkingEnabled_Send_10Chunks is split in 10 chunks, which are encrypted one by one.
/// </summary>
[MemoryDiagnoser]
public class EncryptionBenchmarks
{
    private const int _operations = 1000;

    private readonly byte[] _smallMessage = new byte[100];
    private readonly byte[] _largeMessage = new byte[10 * 1500];
    private PulsarClient? _plainClient;
    private PulsarClient? _encryptedClient;
    private PulsarClient? _plainChunkingClient;
    private PulsarClient? _encryptedChunkingClient;
    private IProducer<byte[]>? _plainProducer;
    private IProducer<byte[]>? _encryptedProducer;
    private IProducer<byte[]>? _plainChunkingProducer;
    private IProducer<byte[]>? _encryptedChunkingProducer;

    [GlobalSetup]
    public async Task Setup()
    {
        _plainClient = CreateClient();
        _plainProducer = _plainClient.NewProducer(Schema.ByteArray).Topic("persistent://public/default/plain").Create();

        _encryptedClient = CreateClient();
        _encryptedProducer = CreateEncryptedProducer(_encryptedClient, "persistent://public/default/encrypted", false);

        _plainChunkingClient = CreateClient();
        _plainChunkingProducer = _plainChunkingClient.NewProducer(Schema.ByteArray).Topic("persistent://public/default/plain-chunking").EnableChunking(true).Create();

        _encryptedChunkingClient = CreateClient();
        _encryptedChunkingProducer = CreateEncryptedProducer(_encryptedChunkingClient, "persistent://public/default/encrypted-chunking", true);

        await _plainProducer.StateChangedTo(ProducerState.Connected);
        await _encryptedProducer.StateChangedTo(ProducerState.Connected);
        await _plainChunkingProducer.StateChangedTo(ProducerState.Connected);
        await _encryptedChunkingProducer.StateChangedTo(ProducerState.Connected);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _plainProducer!.DisposeAsync();
        await _encryptedProducer!.DisposeAsync();
        await _plainChunkingProducer!.DisposeAsync();
        await _encryptedChunkingProducer!.DisposeAsync();
        await _plainClient!.DisposeAsync();
        await _encryptedClient!.DisposeAsync();
        await _plainChunkingClient!.DisposeAsync();
        await _encryptedChunkingClient!.DisposeAsync();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = _operations)]
    public async Task PlainSend_100Bytes()
    {
        for (var i = 0; i < _operations; ++i)
            _ = await _plainProducer!.Send(_smallMessage);
    }

    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task EncryptedSend_100Bytes()
    {
        for (var i = 0; i < _operations; ++i)
            _ = await _encryptedProducer!.Send(_smallMessage);
    }

    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task ChunkingEnabled_Send_10Chunks()
    {
        for (var i = 0; i < _operations; ++i)
            _ = await _plainChunkingProducer!.Send(_largeMessage);
    }

    [Benchmark(OperationsPerInvoke = _operations)]
    public async Task EncryptedChunkingEnabled_Send_10Chunks()
    {
        for (var i = 0; i < _operations; ++i)
            _ = await _encryptedChunkingProducer!.Send(_largeMessage);
    }

    private static PulsarClient CreateClient()
        => PulsarClientFactory.CreatePulsarClient(new FakeConnectionPool(new FakeConnection(false, false, 2048)), new DefaultExceptionHandler(), new Uri("pulsar://localhost:6650"));

    private static IProducer<byte[]> CreateEncryptedProducer(PulsarClient client, string topic, bool enableChunking)
        => client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .EnableChunking(enableChunking)
            .EncryptionKey("benchmark-key")
            .CryptoKeyReader(new PublicKeyReader())
            .Create();

    // The public key of the RSA test key pair of the Java client (public-key.client-rsa.pem), which is licensed under the Apache License, Version 2.0
    private sealed class PublicKeyReader : ICryptoKeyReader
    {
        private const string _publicKey = """
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtKWwgqdnTYrOCv+j1MkT
WfSH0wCsHZZca9wAW3qP4uuhlBvnb10JcFf5ZjzP9BSXK+tHmI8uoN368vEv6yhU
RHM4yuXqzCxzuAwkQSo39rzX8PGC7qdjCN7LDJ3MnqiBIrUsSaEP1wrNsB1kI+o9
ER1e5O/uEPAotP933hHQ0J2hMEekHqL7sBlJ98h6NmsicEaUkardk0TOXrlkjC+c
Md8ZbGScPqI9M38bmn3OLxFTn1vthpvnXLvCmG4M+6xtYtD+npcVPZw1i1R90fMs
7ppZnRbv8Hc/DFdOKVQIgam6CDdnNKgW7c7IBMrP0AEm37HTu0LSOjP2OHXlvvlQ
GQIDAQAB
-----END PUBLIC KEY-----
""";

        public EncryptionKeyInfo? GetPublicKey(string keyName, IReadOnlyDictionary<string, string> metadata)
            => new(Encoding.UTF8.GetBytes(_publicKey));

        public EncryptionKeyInfo? GetPrivateKey(string keyName, IReadOnlyDictionary<string, string> metadata)
            => null;
    }
}
