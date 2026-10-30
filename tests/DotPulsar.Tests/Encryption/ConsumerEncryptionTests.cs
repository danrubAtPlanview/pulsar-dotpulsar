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

// The encryption tests of the Java client (SimpleProducerConsumerTest) for the producer and the consumer. The messages are encrypted by a DotPulsar producer and decrypted by a DotPulsar consumer or reader.
// EncryptionInteropTests has the tests where the Java client (the command line tools of the broker image) produces or consumes the messages.
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

        //Act
        foreach (var (keyName, keySet) in new[] { (RsaKeyName, "ClientRsa"), (RsaKeyName, "ClientRsa"), (RsaPkcs8KeyName, "ClientRsaPkcs8") })
        {
            var messages = Enumerable.Range(0, 10).Select(i => $"my-message-{i}").ToArray();
            expected.AddRange(messages);
            await using var producer = CreateProducer(client, topic, keyName, CreatePublicKeyReader(keySet));
            await Send(producer, messages);
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
        var expected = Enumerable.Range(0, 10).Select(_ => CreateRandomBytes(100)).ToArray();
        await using var producer = CreateProducer(client, topic, EcdsaKeyName, CreatePublicKeyReader("ClientEcdsa"), builder => builder.CompressionType(DotPulsar.CompressionType.Lz4));

        //Act
        foreach (var message in expected)
        {
            _ = await producer.Send(message, _cts.Token);
        }

        //Assert
        var actual = await Receive(consumer, expected.Length);
        actual.Select(message => Convert.ToHexString(message.Data.ToArray())).ShouldBe(expected.Select(Convert.ToHexString));
        actual.ShouldAllBe(message => message.EncryptionContext != null && !message.EncryptionContext.IsEncrypted && message.EncryptionContext.CompressionType == DotPulsar.CompressionType.Lz4);
        actual.ShouldAllBe(message => message.EncryptionContext!.Keys.ContainsKey(EcdsaKeyName));
    }

    // Upstream: SimpleProducerConsumerTest#testCryptoWithChunking
    // Upstream sets the maximum message size of the broker to 1000 bytes. The broker of the fixture has the default size, so the producer is given a maximum size of the chunks of 1000 bytes.
    [Fact]
    public async Task Receive_GivenEcdsaEncryptedChunkedMessage_ShouldDecryptEveryChunkAndAssembleThem()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, CreateSubscriptionName(), CreatePrivateKeyReader("ClientEcdsa"));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = CreateProducer(client, topic, EcdsaKeyName, CreatePublicKeyReader("ClientEcdsa"), builder => builder.EnableChunking(true).ChunkMaxMessageSize(1000));
        var data = CreateRandomBytes(5100);

        //Act
        var sentId = await producer.Send(data, _cts.Token);
        var message = await Receive(consumer);

        //Assert
        message.Data.ToArray().ShouldBe(data);
        message.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeFalse();
        message.EncryptionContext.Keys.Count.ShouldBe(1);
        sentId.GetChunkMessageIds()!.Length.ShouldBe(6);
        message.MessageId.GetChunkMessageIds()!.Length.ShouldBe(6);
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
        await using var producer = CreateProducer(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), builder => builder.EnableChunking(true).ChunkMaxMessageSize(1000));

        //Act
        _ = await producer.Send(CreateRandomBytes(5100), _cts.Token);
        var actual = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);

        //Assert
        actual.ShouldBeNull();
        (await GetBacklog(topic, subscription)).ShouldBe(6);
    }

    // Upstream: SimpleProducerConsumerTest#testDefaultCryptoKeyReader
    [Fact]
    public async Task Receive_GivenDefaultCryptoKeyReaderWithFileAndDataUris_ShouldDecrypt()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        const int numberOfMessages = 10;
        var topic = await _fixture.CreateTopic(_cts.Token);
        var directory = Directory.CreateTempSubdirectory("dotpulsar-keys-");

        try
        {
            string WriteFile(string name, byte[] key)
            {
                var path = Path.Combine(directory.FullName, name);
                File.WriteAllBytes(path, key);
                return new Uri(path).AbsoluteUri;
            }

            var ecdsaPublicFile = WriteFile("public-key.client-ecdsa.pem", CryptoTestKeys.Public("ClientEcdsa"));
            var ecdsaPrivateFile = WriteFile("private-key.client-ecdsa.pem", CryptoTestKeys.Private("ClientEcdsa"));
            var rsaPublicFile = WriteFile("public-key.client-rsa.pem", CryptoTestKeys.Public("ClientRsa"));
            var rsaPrivateFile = WriteFile("private-key.client-rsa.pem", CryptoTestKeys.Private("ClientRsa"));
            var ecdsaPublicData = CreateDataUri(CryptoTestKeys.Public("ClientEcdsa"));
            var ecdsaPrivateData = CreateDataUri(CryptoTestKeys.Private("ClientEcdsa"));
            var rsaPublicData = CreateDataUri(CryptoTestKeys.Public("ClientRsa"));
            var rsaPrivateData = CreateDataUri(CryptoTestKeys.Private("ClientRsa"));

            await using var client = CreateClient();
            await using var consumer1 = CreateConsumer(client, topic, "sub1", new DefaultCryptoKeyReader(null, ecdsaPrivateFile));
            await using var consumer2 = CreateConsumer(client, topic, "sub2", new DefaultCryptoKeyReader(null, ecdsaPrivateData));
            await using var consumer3 = CreateConsumer(client, topic, "sub3", new DefaultCryptoKeyReader(null, null, null, new Dictionary<string, string> { [EcdsaKeyName] = ecdsaPrivateFile, [RsaKeyName] = rsaPrivateFile }));
            await using var consumer4 = CreateConsumer(client, topic, "sub4", new DefaultCryptoKeyReader(null, null, null, new Dictionary<string, string> { [EcdsaKeyName] = ecdsaPrivateData, [RsaKeyName] = rsaPrivateData }));
            await Task.WhenAll(new[] { consumer1, consumer2, consumer3, consumer4 }.Select(consumer => consumer.StateChangedTo(ConsumerState.Active, _cts.Token).AsTask()));

            //Act
            var messages = Enumerable.Range(0, numberOfMessages * 4).Select(i => $"my-message-{i}").ToArray();
            await ProduceMessages(client, topic, EcdsaKeyName, new DefaultCryptoKeyReader(ecdsaPublicFile, null), messages[..numberOfMessages]);
            await ProduceMessages(client, topic, EcdsaKeyName, new DefaultCryptoKeyReader(ecdsaPublicData, null), messages[numberOfMessages..(numberOfMessages * 2)]);
            await ProduceMessages(client, topic, RsaKeyName, new DefaultCryptoKeyReader(rsaPublicFile, null), messages[(numberOfMessages * 2)..(numberOfMessages * 3)]);
            await ProduceMessages(client, topic, RsaKeyName, new DefaultCryptoKeyReader(rsaPublicData, null), messages[(numberOfMessages * 3)..]);

            //Assert
            foreach (var consumer in new[] { consumer1, consumer2 })
            {
                var actual = await Receive(consumer, numberOfMessages * 2);
                actual.Select(message => Encoding.UTF8.GetString(message.Data.ToArray())).ShouldBe(messages[..(numberOfMessages * 2)]);
                actual.ShouldAllBe(message => message.EncryptionContext != null && !message.EncryptionContext.IsEncrypted);
            }

            foreach (var consumer in new[] { consumer3, consumer4 })
            {
                var actual = await Receive(consumer, numberOfMessages * 4);
                actual.Select(message => Encoding.UTF8.GetString(message.Data.ToArray())).ShouldBe(messages);
                actual.ShouldAllBe(message => message.EncryptionContext != null && !message.EncryptionContext.IsEncrypted);
            }
        }
        finally
        {
            directory.Delete(true);
        }
    }

    // The data key is wrapped for every key name, so the consumer of every key can decrypt the message with its own private key
    [Fact]
    public async Task Receive_GivenMessagesEncryptedWithTwoKeys_ShouldDecryptWithEitherPrivateKey()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var rsaConsumer = CreateConsumer(client, topic, CreateSubscriptionName(), new DefaultCryptoKeyReader(null, null, null, new Dictionary<string, string> { [RsaKeyName] = CreateDataUri(CryptoTestKeys.Private("ClientRsa")) }));
        await using var pkcs8Consumer = CreateConsumer(client, topic, CreateSubscriptionName(), new DefaultCryptoKeyReader(null, null, null, new Dictionary<string, string> { [RsaPkcs8KeyName] = CreateDataUri(CryptoTestKeys.Private("ClientRsaPkcs8")) }));
        await rsaConsumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await pkcs8Consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var publicKeys = new DefaultCryptoKeyReader(null, null, new Dictionary<string, string>
        {
            [RsaKeyName] = CreateDataUri(CryptoTestKeys.Public("ClientRsa")),
            [RsaPkcs8KeyName] = CreateDataUri(CryptoTestKeys.Public("ClientRsaPkcs8"))
        });
        await using var producer = client.NewProducer(DotPulsar.Schema.ByteArray)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .EncryptionKey(RsaPkcs8KeyName)
            .EncryptionKey(RsaKeyName)
            .CryptoKeyReader(publicKeys)
            .Create();
        var messages = Enumerable.Range(0, 3).Select(i => $"my-message-{i}").ToArray();

        //Act
        await Send(producer, messages);

        //Assert
        foreach (var consumer in new[] { rsaConsumer, pkcs8Consumer })
        {
            var actual = await Receive(consumer, messages.Length);
            actual.Select(message => Encoding.UTF8.GetString(message.Data.ToArray())).ShouldBe(messages);
            actual.ShouldAllBe(message => message.EncryptionContext != null && !message.EncryptionContext.IsEncrypted);
            actual.ShouldAllBe(message => message.EncryptionContext!.Keys.Count == 2 && message.EncryptionContext.Keys.ContainsKey(RsaKeyName) && message.EncryptionContext.Keys.ContainsKey(RsaPkcs8KeyName));
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

        //Act
        await ProduceMessages(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), "message-0");
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

        //Act
        await ProduceMessages(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), "message-0");
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

        //Act
        await ProduceMessages(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), "message-0");
        var actual = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token);

        //Assert
        actual.ShouldBeNull();
        await WaitUntil(async () => await GetBacklog(topic, subscription) == 0);
    }

    // Upstream: SimpleProducerConsumerTest#testEncryptionFailure (all the steps in the order of the test: the producer, then a consumer without a key reader, with CONSUME, with a key reader, with DISCARD)
    [Fact]
    public async Task Receive_GivenEncryptionFailureActionsOneAfterTheOther_ShouldFailConsumeDecryptAndDiscard()
    {
        //Arrange
        const int totalMessages = 10;
        var topic = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        var consumer = CreateConsumer(client, topic, subscription);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        //Act & Assert
        // 1. A key that the key reader cannot read: the producer is not created
        var exception = Record.Exception(() => client.NewProducer(DotPulsar.Schema.ByteArray).Topic(topic).EncryptionKey("client-non-existant-rsa.pem").CryptoKeyReader(CreatePublicKeyReader("ClientRsa", RsaKeyName)).Create());
        exception.ShouldBeOfType<DotPulsar.Exceptions.CryptoException>();

        // 2. A producer with a key that the key reader can read
        await using (var producer = CreateProducer(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa", RsaKeyName)))
        {
            await Send(producer, Enumerable.Range(0, totalMessages).Select(i => $"my-message-{i}").ToArray());
        }

        // 3. A consumer without a key reader does not receive the messages
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token)).ShouldBeNull();

        // 4. A consumer that consumes the messages that it cannot decrypt receives the encrypted message
        await consumer.DisposeAsync();
        consumer = CreateConsumer(client, topic, subscription, consumeAction: ConsumerCryptoFailureAction.Consume);
        var encrypted = await Receive(consumer);
        Encoding.UTF8.GetString(encrypted.Data.ToArray()).ShouldNotBe("my-message-0");
        encrypted.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeTrue();
        await consumer.AcknowledgeCumulative(encrypted, _cts.Token);

        // 5. A consumer with a key reader decrypts the messages
        await consumer.DisposeAsync();
        consumer = CreateConsumer(client, topic, subscription, CreatePrivateKeyReader("ClientRsa", RsaKeyName));
        IMessage<byte[]>? last = null;

        for (var i = 1; i < totalMessages - 1; ++i)
        {
            last = await Receive(consumer);
            Encoding.UTF8.GetString(last.Data.ToArray()).ShouldBe($"my-message-{i}");
            last.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeFalse();
        }

        await consumer.AcknowledgeCumulative(last!, _cts.Token);

        // 6. A consumer that discards what it cannot decrypt does not receive the last message, and it is acknowledged
        await consumer.DisposeAsync();
        consumer = CreateConsumer(client, topic, subscription, consumeAction: ConsumerCryptoFailureAction.Discard);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token)).ShouldBeNull();
        await WaitUntil(async () => await GetBacklog(topic, subscription) == 0);
        await consumer.DisposeAsync();
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

        //Act
        await ProduceMessages(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), "message-0");
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
        var expected = Enumerable.Range(0, 10).Select(i => $"my-message-{i}").ToArray();

        //Act
        await ProduceMessages(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), expected);
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
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, CreateSubscriptionName(), CreatePrivateKeyReader("ClientRsaPkcs8"), ConsumerCryptoFailureAction.Consume);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = CreateProducer(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), builder => builder.CompressionType(DotPulsar.CompressionType.Lz4));
        var expected = Enumerable.Range(0, 10).Select(i => $"message-{i}").ToArray();

        //Act
        await Send(producer, expected);
        var actual = await Receive(consumer, expected.Length);

        //Assert
        actual.ShouldAllBe(message => message.EncryptionContext != null && message.EncryptionContext.IsEncrypted);
        actual.ShouldAllBe(message => message.EncryptionContext!.CompressionType == DotPulsar.CompressionType.Lz4);
        actual.Select(message => message.EncryptionContext!.UncompressedMessageSize).ShouldBe(expected.Select(message => (uint) message.Length));
        actual.ShouldAllBe(message => message.Data.Length > MessageCrypto.TagLength);
        actual.Zip(expected, (message, text) => message.Data.ToArray().SequenceEqual(Encoding.UTF8.GetBytes(text))).ShouldAllBe(isPlainText => !isPlainText);
    }

    // The messages of a batch are encrypted together, decrypted together, and they share the encryption context
    [Fact]
    public async Task Receive_GivenEncryptedBatch_ShouldUnpackItAfterDecryptingIt()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, CreateSubscriptionName(), CreatePrivateKeyReader("ClientRsa"));
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = CreateProducer(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), builder => builder.CompressionType(DotPulsar.CompressionType.Lz4).EnableBatching(true).BatchingMaxMessages(5).BatchingMaxPublishDelay(TimeSpan.FromHours(1)));
        var expected = Enumerable.Range(0, 5).Select(i => $"message-{i}").ToArray();

        //Act
        foreach (var message in expected)
        {
            await producer.SendChannel.Send(Encoding.UTF8.GetBytes(message), null, _cts.Token);
        }

        await producer.Flush(_cts.Token);
        var actual = await Receive(consumer, 5);

        //Assert
        actual.Select(message => message.MessageId.BatchIndex).ShouldBe([0, 1, 2, 3, 4]);
        actual.Select(message => Encoding.UTF8.GetString(message.Data.ToArray())).ShouldBe(expected);
        var context = actual[0].EncryptionContext.ShouldNotBeNull();
        context.IsEncrypted.ShouldBeFalse();
        context.BatchSize.ShouldBe(5);
        context.CompressionType.ShouldBe(DotPulsar.CompressionType.Lz4);
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
        await using var producer = CreateProducer(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), builder => builder.EnableBatching(true).BatchingMaxMessages(5).BatchingMaxPublishDelay(TimeSpan.FromHours(1)));
        using var receiveCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var receiving = consumer.Receive(receiveCts.Token).AsTask();
        var payload = CreateRandomBytes(100);

        //Act
        for (var i = 0; i < 2000; ++i)
        {
            await producer.SendChannel.Send(payload, null, _cts.Token);
        }

        await producer.Flush(_cts.Token);

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
        var expected = Enumerable.Range(0, 10).Select(i => $"my-message-{i}").ToArray();
        await ProduceMessages(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), expected);
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
        await ProduceMessages(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), "message-0");
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
        await ProduceMessages(client, topic, RsaKeyName, CreatePublicKeyReader("ClientRsa"), "message-0");
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

    // The reader returns the key of the key set for every key name, or only for the key name if there is one
    private static ICryptoKeyReader CreatePrivateKeyReader(string keySet, string? keyName = null)
        => keyName is null
            ? new DefaultCryptoKeyReader(null, CreateDataUri(CryptoTestKeys.Private(keySet)))
            : new DefaultCryptoKeyReader(null, null, null, new Dictionary<string, string> { [keyName] = CreateDataUri(CryptoTestKeys.Private(keySet)) });

    private static ICryptoKeyReader CreatePublicKeyReader(string keySet, string? keyName = null)
        => keyName is null
            ? new DefaultCryptoKeyReader(CreateDataUri(CryptoTestKeys.Public(keySet)), null)
            : new DefaultCryptoKeyReader(null, null, new Dictionary<string, string> { [keyName] = CreateDataUri(CryptoTestKeys.Public(keySet)) });

    private static byte[] CreateRandomBytes(int length)
    {
        var bytes = new byte[length];
        Random.Shared.NextBytes(bytes);
        return bytes;
    }

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

    private IProducer<byte[]> CreateProducer(IPulsarClient client, string topic, string keyName, ICryptoKeyReader cryptoKeyReader, Action<IProducerBuilder<byte[]>>? configure = null)
    {
        var builder = client.NewProducer(DotPulsar.Schema.ByteArray)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .EncryptionKey(keyName)
            .CryptoKeyReader(cryptoKeyReader);

        configure?.Invoke(builder);
        return builder.Create();
    }

    // Every message is sent after the one before it has been acknowledged
    private async Task Send(IProducer<byte[]> producer, params string[] messages)
    {
        foreach (var message in messages)
        {
            _ = await producer.Send(Encoding.UTF8.GetBytes(message), _cts.Token);
        }
    }

    private async Task ProduceMessages(IPulsarClient client, string topic, string keyName, ICryptoKeyReader cryptoKeyReader, params string[] messages)
    {
        await using var producer = CreateProducer(client, topic, keyName, cryptoKeyReader);
        await Send(producer, messages);
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
