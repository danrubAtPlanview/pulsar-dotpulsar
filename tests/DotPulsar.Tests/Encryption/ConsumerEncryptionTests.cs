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

namespace DotPulsar.Tests.Encryption;

using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal;
using DotPulsar.Tests.Internal.Crypto;
using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

// The consumer and reader side of the encryption tests of the Java client (SimpleProducerConsumerTest). The messages are encrypted and produced by the command line tools of the broker image,
// which are the Java client. TODO(WP9-producer): produce the messages with an IProducer that encrypts them, and port the tests that need the producer, which are testEncryptionFailure (step 1,
// the producer that cannot load its key), testEncryptionRequired and ProducerEncryptionKeyFailureTest.
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class ConsumerEncryptionTests : IDisposable
{
    private const string RsaKeyName = "client-rsa.pem";
    private const string RsaPkcs8KeyName = "client-rsa-pkcs8.pem";
    private const string EcdsaKeyName = "client-ecdsa.pem";

    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public ConsumerEncryptionTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(4));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Upstream: SimpleProducerConsumerTest#testRSAEncryption
    [Fact]
    public async Task Receive_GivenRsaEncryptedMessagesFromThreeProducers_ShouldDecryptInOrder()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        var cryptoKeyReader = new DefaultCryptoKeyReader(null, null, null, new Dictionary<string, string>
        {
            [RsaKeyName] = CreateDataUri(CryptoTestKeys.Private("ClientRsa")),
            [RsaPkcs8KeyName] = CreateDataUri(CryptoTestKeys.Private("ClientRsaPkcs8"))
        });
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, subscription, cryptoKeyReader);
        await using var consumerWithoutKeys = CreateConsumer(client, topic, CreateSubscriptionName());
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var expected = new List<string>();
        await CopyPublicKeys();

        //Act
        foreach (var (keyName, keySet) in new[] { (RsaKeyName, "client-rsa"), (RsaKeyName, "client-rsa"), (RsaPkcs8KeyName, "client-rsa-pkcs8") })
        {
            var messages = Enumerable.Range(0, 10).Select(i => $"my-message-{i}").ToArray();
            expected.AddRange(messages);
            await ProduceUsingClient(topic, keyName, $"public-key.{keySet}.pem", messages);
        }

        //Assert
        var actual = await Receive(consumer, expected.Count);
        actual.Select(message => Encoding.UTF8.GetString(message.Data.ToArray())).ShouldBe(expected);
        actual.ShouldAllBe(message => message.EncryptionContext != null && !message.EncryptionContext.IsEncrypted);
        actual.Select(message => message.EncryptionContext!.Keys.Keys.Single()).Distinct().ShouldBe([RsaKeyName, RsaPkcs8KeyName], ignoreOrder: true);
        (await TransactionFixture.ReceiveOrNull(consumerWithoutKeys, TimeSpan.FromSeconds(3), _cts.Token)).ShouldBeNull();
    }

    // Upstream: SimpleProducerConsumerTest#testECDSAEncryption
    [Fact]
    public async Task Receive_GivenEcdsaEncryptedLz4Messages_ShouldDecrypt()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, CreateSubscriptionName(), CreatePrivateKeyReader("ClientEcdsa"));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingPerf(topic, 10, 100, EcdsaKeyName, "public-key.client-ecdsa.pem", "-z LZ4");
        var actual = await ReceiveAll(consumer);

        //Assert
        actual.Count.ShouldBeGreaterThanOrEqualTo(10);
        actual.ShouldAllBe(message => message.Data.Length == 100);
        actual.ShouldAllBe(message => message.EncryptionContext != null && !message.EncryptionContext.IsEncrypted && message.EncryptionContext.CompressionType == DotPulsar.CompressionType.Lz4);
        actual.ShouldAllBe(message => message.EncryptionContext!.Keys.ContainsKey(EcdsaKeyName));
    }

    // Upstream: SimpleProducerConsumerTest#testCryptoWithChunking
    [Fact]
    public async Task Receive_GivenEcdsaEncryptedChunkedMessage_ShouldDecryptEveryChunkAndAssembleThem()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        const int messageSize = 12_000_000;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, CreateSubscriptionName(), CreatePrivateKeyReader("ClientEcdsa"));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingPerf(topic, 1, messageSize, EcdsaKeyName, "public-key.client-ecdsa.pem", "-ch", rate: 1);
        var message = await Receive(consumer);

        //Assert
        message.Data.Length.ShouldBe(messageSize);
        message.MessageId.GetChunkMessageIds()!.Length.ShouldBeGreaterThanOrEqualTo(3);
        message.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeFalse();
        message.EncryptionContext.Keys.Count.ShouldBe(1);
    }

    // The chunks cannot be assembled as long as they are encrypted, so the chunks of a message that cannot be decrypted are not delivered, whatever the action is
    [Fact]
    public async Task Receive_GivenEncryptedChunkedMessageAndConsumeWithoutCryptoKeyReader_ShouldNeitherDeliverNorAcknowledgeTheChunks()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, subscription, consumeAction: ConsumerCryptoFailureAction.Consume);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingPerf(topic, 1, 12_000_000, RsaKeyName, "public-key.client-rsa.pem", "-ch", rate: 1);
        var actual = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);

        //Assert
        actual.ShouldBeNull();
        (await GetBacklog(topic, subscription)).ShouldBeGreaterThanOrEqualTo(3);
    }

    // Upstream: SimpleProducerConsumerTest#testDefaultCryptoKeyReader
    [Fact]
    public async Task Receive_GivenDefaultCryptoKeyReaderWithFileAndDataUris_ShouldDecrypt()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var privateKey = CryptoTestKeys.Private("ClientRsa");
        var directory = Directory.CreateTempSubdirectory("dotpulsar-keys-");

        try
        {
            var file = new Uri(Path.Combine(directory.FullName, "private-key.client-rsa.pem")).AbsoluteUri;
            await File.WriteAllBytesAsync(Path.Combine(directory.FullName, "private-key.client-rsa.pem"), privateKey, _cts.Token);
            var dataUri = CreateDataUri(privateKey);
            var byName = (string uri) => new Dictionary<string, string> { [RsaKeyName] = uri };
            ICryptoKeyReader[] readers =
            [
                new DefaultCryptoKeyReader(null, dataUri),
                new DefaultCryptoKeyReader(null, file),
                new DefaultCryptoKeyReader(null, null, null, byName(dataUri)),
                new DefaultCryptoKeyReader(null, null, null, byName(file))
            ];

            await using var client = CreateClient();
            var consumers = readers.Select(reader => CreateConsumer(client, topic, CreateSubscriptionName(), reader)).ToArray();

            try
            {
                await CopyPublicKeys();
                var messages = Enumerable.Range(0, 10).Select(i => $"my-message-{i}").ToArray();

                //Act
                await ProduceUsingClient(topic, RsaKeyName, "public-key.client-rsa.pem", messages);

                //Assert
                foreach (var consumer in consumers)
                {
                    var actual = await Receive(consumer, messages.Length);
                    actual.Select(message => Encoding.UTF8.GetString(message.Data.ToArray())).ShouldBe(messages);
                }
            }
            finally
            {
                foreach (var consumer in consumers)
                {
                    await consumer.DisposeAsync();
                }
            }
        }
        finally
        {
            directory.Delete(true);
        }
    }

    // Upstream: SimpleProducerConsumerTest#testEncryptionFailure (the consumer side: a consumer that has no key, or a key that is not the one of the producer)
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Receive_GivenMessageThatCannotBeDecryptedAndFail_ShouldNeitherDeliverNorAcknowledgeIt(bool hasWrongKey)
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, subscription, hasWrongKey ? CreatePrivateKeyReader("ClientRsaPkcs8") : null);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingClient(topic, RsaKeyName, "public-key.client-rsa.pem", "message-0");
        var actual = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token);

        //Assert
        actual.ShouldBeNull();
        (await GetBacklog(topic, subscription)).ShouldBe(1);
    }

    // Upstream: SimpleProducerConsumerTest#testEncryptionFailure (consumer with CONSUME)
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Receive_GivenMessageThatCannotBeDecryptedAndConsume_ShouldDeliverTheEncryptedPayload(bool hasWrongKey)
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, CreateSubscriptionName(), hasWrongKey ? CreatePrivateKeyReader("ClientRsaPkcs8") : null, ConsumerCryptoFailureAction.Consume);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingClient(topic, RsaKeyName, "public-key.client-rsa.pem", "message-0");
        var actual = await Receive(consumer);

        //Assert
        actual.Data.Length.ShouldBe("message-0".Length + 16);
        actual.Data.ToArray().ShouldNotBe(Encoding.UTF8.GetBytes("message-0"));
        var context = actual.EncryptionContext.ShouldNotBeNull();
        context.IsEncrypted.ShouldBeTrue();
        context.Keys.Keys.ShouldBe([RsaKeyName]);
        context.Param.Length.ShouldBe(12);
    }

    // Upstream: SimpleProducerConsumerTest#testEncryptionFailure (consumer with DISCARD)
    [Fact]
    public async Task Receive_GivenMessageThatCannotBeDecryptedAndDiscard_ShouldAcknowledgeItWithoutDeliveringIt()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, subscription, consumeAction: ConsumerCryptoFailureAction.Discard);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingClient(topic, RsaKeyName, "public-key.client-rsa.pem", "message-0");
        var actual = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token);

        //Assert
        actual.ShouldBeNull();
        await WaitUntil(async () => await GetBacklog(topic, subscription) == 0);
    }

    // The message is redelivered when the ack timeout expires, so it is delivered when the key can be read
    [Fact]
    public async Task Receive_GivenMessageThatCannotBeDecryptedAndFailAndAckTimeout_ShouldRedeliverItAfterTheAckTimeout()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var cryptoKeyReader = new KeyReaderThatFindsTheKeyAfterNCalls(1, CryptoTestKeys.Private("ClientRsa"));
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, CreateSubscriptionName(), cryptoKeyReader, configure: builder => builder.SubscriptionType(SubscriptionType.Shared).AckTimeout(TimeSpan.FromSeconds(2)));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingClient(topic, RsaKeyName, "public-key.client-rsa.pem", "message-0");
        var actual = await Receive(consumer);

        //Assert
        Encoding.UTF8.GetString(actual.Data.ToArray()).ShouldBe("message-0");
        actual.RedeliveryCount.ShouldBeGreaterThan(0U);
        cryptoKeyReader.Calls.ShouldBeGreaterThanOrEqualTo(2);
    }

    // Upstream: SimpleProducerConsumerTest#testEncryptionConsumerWithoutCryptoReader
    [Fact]
    public async Task Receive_GivenConsumeActionWithoutCryptoKeyReader_ShouldAllowManualDecryption()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(DotPulsar.Schema.ByteArray)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(CreateSubscriptionName())
            .TopicsPattern(new Regex(topic))
            .CryptoFailureAction(ConsumerCryptoFailureAction.Consume)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();
        var expected = Enumerable.Range(0, 10).Select(i => $"my-message-{i}").ToArray();

        //Act
        await ProduceUsingClient(topic, RsaKeyName, "public-key.client-rsa.pem", expected);
        var actual = await Receive(consumer, expected.Length);

        //Assert
        var keyReader = new InMemoryCryptoKeyReader().WithPrivateKey(RsaKeyName, CryptoTestKeys.Private("ClientRsa"));
        using var messageCrypto = new MessageCrypto(false);
        var decrypted = actual.Select(message => Decrypt(messageCrypto, keyReader, message)).ToArray();
        decrypted.ShouldBe(expected);
    }

    // Upstream: SimpleProducerConsumerTest#testE2EEncryptionWithCompression (a consumer that cannot decrypt gets the compressed payload and the information to decompress it)
    [Fact]
    public async Task Receive_GivenMismatchedKeyAndConsumeAction_ShouldDeliverEncryptedCompressedPayload()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, CreateSubscriptionName(), CreatePrivateKeyReader("ClientRsa"), ConsumerCryptoFailureAction.Consume);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingPerf(topic, 10, 100, EcdsaKeyName, "public-key.client-ecdsa.pem", "-z LZ4");
        var actual = await ReceiveAll(consumer);

        //Assert
        actual.Count.ShouldBeGreaterThanOrEqualTo(10);
        actual.ShouldAllBe(message => message.EncryptionContext != null && message.EncryptionContext.IsEncrypted);
        actual.ShouldAllBe(message => message.EncryptionContext!.CompressionType == DotPulsar.CompressionType.Lz4 && message.EncryptionContext.UncompressedMessageSize == 100);
        actual.ShouldAllBe(message => message.Data.Length != 100);
    }

    // The messages of a batch are decrypted together, and they share the encryption context
    [Fact]
    public async Task Receive_GivenEncryptedBatch_ShouldUnpackItAfterDecryptingIt()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, CreateSubscriptionName(), CreatePrivateKeyReader("ClientRsa"));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingPerf(topic, 5, 8, RsaKeyName, "public-key.client-rsa.pem", "-z LZ4 -bm 5 -b 600000 -o 5", batching: true);
        var actual = await Receive(consumer, 5);

        //Assert
        actual.Select(message => message.MessageId.BatchIndex).ShouldBe([0, 1, 2, 3, 4]);
        actual.ShouldAllBe(message => message.Data.Length == 8);
        var context = actual[0].EncryptionContext.ShouldNotBeNull();
        context.IsEncrypted.ShouldBeFalse();
        context.BatchSize.ShouldBe(5);
        actual.ShouldAllBe(message => ReferenceEquals(message.EncryptionContext, context));
    }

    // Upstream: SimpleProducerConsumerTest#testEncryptionFailureWithBatchPublish
    // The broker deducts a permit for every message of a batch, so the consumer must count every message of the batches that it discards, or the broker stops sending
    [Fact]
    public async Task Receive_GivenEncryptedBatchesAndDiscard_ShouldDiscardEveryBatchWithoutRunningOutOfPermits()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, subscription, consumeAction: ConsumerCryptoFailureAction.Discard, configure: builder => builder.MessagePrefetchCount(100));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();
        using var receiveCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var receiving = consumer.Receive(receiveCts.Token).AsTask();

        //Act
        await ProduceUsingPerf(topic, 2000, 100, RsaKeyName, "public-key.client-rsa.pem", "-bm 5 -b 600000 -o 100", batching: true, rate: 2000);

        //Assert
        await WaitUntil(async () => await GetBacklog(topic, subscription) == 0);
        receiving.IsCompleted.ShouldBeFalse();
        receiveCts.Cancel();
        (await Record.ExceptionAsync(() => receiving)).ShouldBeAssignableTo<OperationCanceledException>();
    }

    [Fact]
    public async Task Receive_GivenMessageThatIsNotEncryptedAndCryptoKeyReader_ShouldDeliverItWithoutEncryptionContext()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, CreateSubscriptionName(), CreatePrivateKeyReader("ClientRsa"));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(DotPulsar.Schema.ByteArray).StateChangedHandler(_testOutputHelper.Log).Topic(topic).Create();

        //Act
        await producer.Send("not encrypted"u8.ToArray(), _cts.Token);
        var actual = await Receive(consumer);

        //Assert
        Encoding.UTF8.GetString(actual.Data.ToArray()).ShouldBe("not encrypted");
        actual.EncryptionContext.ShouldBeNull();
    }

    [Fact]
    public async Task Receive_GivenReaderWithCryptoKeyReader_ShouldDecryptTheMessages()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await CopyPublicKeys();
        var expected = Enumerable.Range(0, 10).Select(i => $"my-message-{i}").ToArray();
        await ProduceUsingClient(topic, RsaKeyName, "public-key.client-rsa.pem", expected);
        await using var reader = client.NewReader(DotPulsar.Schema.ByteArray)
            .StartMessageId(MessageId.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .DefaultCryptoKeyReader(CreateDataUri(CryptoTestKeys.Private("ClientRsa")))
            .Create();

        //Act
        var actual = await Receive(reader, expected.Length);

        //Assert
        actual.Select(message => Encoding.UTF8.GetString(message.Data.ToArray())).ShouldBe(expected);
        actual.ShouldAllBe(message => message.EncryptionContext != null && !message.EncryptionContext.IsEncrypted);
    }

    [Fact]
    public async Task Receive_GivenReaderWithConsumeActionAndNoCryptoKeyReader_ShouldDeliverTheEncryptedPayload()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await CopyPublicKeys();
        await ProduceUsingClient(topic, RsaKeyName, "public-key.client-rsa.pem", "message-0");
        await using var reader = client.NewReader(DotPulsar.Schema.ByteArray)
            .StartMessageId(MessageId.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .CryptoFailureAction(ConsumerCryptoFailureAction.Consume)
            .Create();

        //Act
        var actual = await Receive(reader);

        //Assert
        actual.Data.Length.ShouldBe("message-0".Length + 16);
        actual.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeTrue();
    }

    [Theory]
    [InlineData(ConsumerCryptoFailureAction.Fail)]
    [InlineData(ConsumerCryptoFailureAction.Discard)]
    public async Task Receive_GivenReaderThatCannotDecryptTheMessageAndNoDeliveryAction_ShouldSkipItAndDeliverTheNextMessage(ConsumerCryptoFailureAction cryptoFailureAction)
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await CopyPublicKeys();
        await ProduceUsingClient(topic, RsaKeyName, "public-key.client-rsa.pem", "message-0");
        await using var producer = client.NewProducer(DotPulsar.Schema.ByteArray).StateChangedHandler(_testOutputHelper.Log).Topic(topic).Create();
        await producer.Send("not encrypted"u8.ToArray(), _cts.Token);
        await using var reader = client.NewReader(DotPulsar.Schema.ByteArray)
            .StartMessageId(MessageId.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .CryptoFailureAction(cryptoFailureAction)
            .Create();

        //Act
        var actual = await Receive(reader);

        //Assert
        Encoding.UTF8.GetString(actual.Data.ToArray()).ShouldBe("not encrypted");
    }

    private static void SkipWhenExplicitCurvesAreNotSupported()
        => Assert.SkipWhen(OperatingSystem.IsMacOS(), "The platform does not support importing keys with explicit curve parameters");

    private static string CreateSubscriptionName() => $"subscription-{Guid.NewGuid():N}";

    private static string CreateDataUri(byte[] key)
        => "data:application/x-pem-file;base64," + Convert.ToBase64String(key);

    private static ICryptoKeyReader CreatePrivateKeyReader(string keySet)
        => new DefaultCryptoKeyReader(null, CreateDataUri(CryptoTestKeys.Private(keySet)));

    // Decrypts the payload with the keys, the IV and the key reader like a consumer does, using only what the encryption context has
    private static string Decrypt(MessageCrypto messageCrypto, ICryptoKeyReader keyReader, IMessage<byte[]> message)
    {
        var context = message.EncryptionContext!;
        var metadata = new MessageMetadata { EncryptionParam = Google.Protobuf.ByteString.CopyFrom(context.Param) };

        foreach (var key in context.Keys)
        {
            metadata.EncryptionKeys.Add(new EncryptionKeys { Key = key.Key, Value = Google.Protobuf.ByteString.CopyFrom(key.Value.KeyValue) });
        }

        messageCrypto.TryDecrypt(metadata, message.Data, keyReader, out var decrypted).ShouldBeTrue();
        return Encoding.UTF8.GetString(decrypted.ToArray());
    }

    private async Task CopyPublicKeys()
    {
        await _fixture.CopyToPulsar(CryptoTestKeys.Public("ClientRsa"), "pulsar/keys/public-key.client-rsa.pem", _cts.Token);
        await _fixture.CopyToPulsar(CryptoTestKeys.Public("ClientRsaPkcs8"), "pulsar/keys/public-key.client-rsa-pkcs8.pem", _cts.Token);
        await _fixture.CopyToPulsar(CryptoTestKeys.Public("ClientEcdsa"), "pulsar/keys/public-key.client-ecdsa.pem", _cts.Token);
    }

    // Every message is sent in an entry of its own
    private async Task ProduceUsingClient(string topic, string keyName, string keyFile, params string[] messages)
    {
        var command = $"bin/pulsar-client produce {topic} -db -ekn {keyName} -ekv file:///pulsar/keys/{keyFile} -m '{string.Join(',', messages)}'";
        var result = await _fixture.ExecInPulsar(command, _cts.Token);

        if (result.ExitCode != 0)
            throw new Exception($"Could not produce the encrypted messages: {result.Stderr}");
    }

    // The tool counts the messages when they are acknowledged, so it can send a few more than it was asked to
    private async Task ProduceUsingPerf(string topic, int numberOfMessages, int messageSize, string keyName, string keyFile, string options, bool batching = false, int rate = 10)
    {
        var command = $"bin/pulsar-perf produce {topic} -m {numberOfMessages} -s {messageSize} -r {rate} {(batching ? string.Empty : "-db")} -k {keyName} -v /pulsar/keys/{keyFile} {options}";
        var result = await _fixture.ExecInPulsar(command, _cts.Token);

        if (result.ExitCode != 0)
            throw new Exception($"Could not produce the encrypted messages: {result.Stderr}");
    }

    private IConsumer<byte[]> CreateConsumer(
        IPulsarClient client,
        string topic,
        string subscription,
        ICryptoKeyReader? cryptoKeyReader = null,
        ConsumerCryptoFailureAction consumeAction = ConsumerCryptoFailureAction.Fail,
        Action<IConsumerBuilder<byte[]>>? configure = null)
    {
        var builder = client.NewConsumer(DotPulsar.Schema.ByteArray)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscription)
            .Topic(topic)
            .CryptoFailureAction(consumeAction);

        if (cryptoKeyReader is not null)
            builder.CryptoKeyReader(cryptoKeyReader);

        configure?.Invoke(builder);
        return builder.Create();
    }

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();

    private async Task<IMessage<byte[]>> Receive(IReceive<IMessage<byte[]>> receiver)
    {
        var message = await TransactionFixture.ReceiveOrNull(receiver, TimeSpan.FromSeconds(60), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    private async Task<List<IMessage<byte[]>>> Receive(IReceive<IMessage<byte[]>> receiver, int numberOfMessages)
    {
        var messages = new List<IMessage<byte[]>>(numberOfMessages);

        while (messages.Count < numberOfMessages)
        {
            messages.Add(await Receive(receiver));
        }

        return messages;
    }

    // Until nothing has been received for 5 seconds
    private async Task<List<IMessage<byte[]>>> ReceiveAll(IConsumer<byte[]> consumer)
    {
        var messages = new List<IMessage<byte[]>> { await Receive(consumer) };

        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token) is { } message)
        {
            messages.Add(message);
        }

        return messages;
    }

    private async Task<long> GetBacklog(string topic, string subscription)
    {
        using var httpClient = _fixture.CreateAdminClient();
        using var response = await httpClient.GetAsync($"/admin/v2/persistent/{topic.Replace("persistent://", string.Empty)}/stats", _cts.Token);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(_cts.Token);
        using var json = await JsonDocument.ParseAsync(stream, cancellationToken: _cts.Token);
        return json.RootElement.GetProperty("subscriptions").GetProperty(subscription).GetProperty("msgBacklog").GetInt64();
    }

    private async Task WaitUntil(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);

        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The condition was not met within 60 seconds");

            await Task.Delay(TimeSpan.FromMilliseconds(200), _cts.Token);
        }
    }

    // Does not find the private key for the first calls, like a key store that is not available yet
    private sealed class KeyReaderThatFindsTheKeyAfterNCalls : ICryptoKeyReader
    {
        private readonly int _callsWithoutKey;
        private readonly byte[] _privateKey;
        private int _calls;

        public KeyReaderThatFindsTheKeyAfterNCalls(int callsWithoutKey, byte[] privateKey)
        {
            _callsWithoutKey = callsWithoutKey;
            _privateKey = privateKey;
        }

        public int Calls => Volatile.Read(ref _calls);

        public EncryptionKeyInfo? GetPublicKey(string keyName, IReadOnlyDictionary<string, string> metadata)
            => null;

        public EncryptionKeyInfo? GetPrivateKey(string keyName, IReadOnlyDictionary<string, string> metadata)
            => Interlocked.Increment(ref _calls) > _callsWithoutKey ? new EncryptionKeyInfo(_privateKey) : null;
    }
}
