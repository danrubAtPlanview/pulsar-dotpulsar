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
using DotPulsar.Tests.Internal;
using DotPulsar.Tests.Internal.Crypto;
using System.Buffers;
using System.Text;
using System.Text.Json;

// These tests use the 'pulsar-client' and 'pulsar-perf' command line tools (the Java client) of the apachepulsar/pulsar image together with a DotPulsar producer or consumer.
// A DotPulsar producer is consumed by the Java client, which proves that the messages that DotPulsar encrypts, compresses, batches and chunks are the ones that the Java client reads, and a DotPulsar consumer
// reads what the Java client produces. The byte-exact compatibility of the cryptography is also covered without a broker by MessageCryptoJavaInteropTests.
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class EncryptionInteropTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public EncryptionInteropTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(4));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    [Fact]
    public async Task Receive_GivenJavaProducedRsaEncryptedMessage_ShouldDecrypt()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, "ClientRsa");
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        //Act
        await ProduceUsingJavaClient(topic, "client-rsa.pem", "ClientRsa", "java-rsa-0");
        var actual = await Receive(consumer);

        //Assert
        Encoding.UTF8.GetString(actual.Data.ToArray()).ShouldBe("java-rsa-0");
        actual.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeFalse();
        actual.EncryptionContext.Keys.Keys.ShouldBe(["client-rsa.pem"]);
    }

    [Fact]
    public async Task Receive_GivenJavaProducedEcdsaEncryptedMessage_ShouldDecrypt()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, "ClientEcdsa");
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        //Act
        await ProduceUsingJavaClient(topic, "client-ecdsa.pem", "ClientEcdsa", "java-ecdsa-0");
        var actual = await Receive(consumer);

        //Assert
        Encoding.UTF8.GetString(actual.Data.ToArray()).ShouldBe("java-ecdsa-0");
        actual.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeFalse();
        actual.EncryptionContext.Keys.Keys.ShouldBe(["client-ecdsa.pem"]);
    }

    // Additional coverage: the messages of SimpleProducerConsumerTest#testECDSAEncryption are produced by the Java client, the test with a DotPulsar producer is ConsumerEncryptionTests
    [Fact]
    public async Task Receive_GivenJavaProducedEcdsaEncryptedLz4Messages_ShouldDecrypt()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, "ClientEcdsa");
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingPerf(topic, 10, 100, "client-ecdsa.pem", "public-key.client-ecdsa.pem", "-z LZ4");
        var actual = await ReceiveAll(consumer);

        //Assert
        actual.Count.ShouldBeGreaterThanOrEqualTo(10);
        actual.ShouldAllBe(message => message.Data.Length == 100);
        actual.ShouldAllBe(message => message.EncryptionContext != null && !message.EncryptionContext.IsEncrypted && message.EncryptionContext.CompressionType == DotPulsar.CompressionType.Lz4);
        actual.ShouldAllBe(message => message.EncryptionContext!.Keys.ContainsKey("client-ecdsa.pem"));
    }

    // Additional coverage: the message of SimpleProducerConsumerTest#testCryptoWithChunking is produced by the Java client, which splits it in chunks that are encrypted one by one, the test with a DotPulsar producer is ConsumerEncryptionTests
    [Fact]
    public async Task Receive_GivenJavaProducedEcdsaEncryptedChunkedMessage_ShouldDecryptEveryChunkAndAssembleThem()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        const int messageSize = 12_000_000;
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, "ClientEcdsa");
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingPerf(topic, 1, messageSize, "client-ecdsa.pem", "public-key.client-ecdsa.pem", "-ch", rate: 1);
        var message = await Receive(consumer);

        //Assert
        message.Data.Length.ShouldBe(messageSize);
        message.MessageId.GetChunkMessageIds()!.Length.ShouldBeGreaterThanOrEqualTo(3);
        message.EncryptionContext.ShouldNotBeNull().IsEncrypted.ShouldBeFalse();
        message.EncryptionContext.Keys.Count.ShouldBe(1);
    }

    // The messages of a batch that the Java client has encrypted as a whole
    [Fact]
    public async Task Receive_GivenJavaProducedEncryptedBatch_ShouldUnpackItAfterDecryptingIt()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic, "ClientRsa");
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await CopyPublicKeys();

        //Act
        await ProduceUsingPerf(topic, 5, 8, "client-rsa.pem", "public-key.client-rsa.pem", "-z LZ4 -bm 5 -b 600000 -o 5", batching: true);
        var actual = new List<IMessage<string>>();

        while (actual.Count < 5)
        {
            actual.Add(await Receive(consumer));
        }

        //Assert
        actual.Select(message => message.MessageId.BatchIndex).ShouldBe([0, 1, 2, 3, 4]);
        actual.ShouldAllBe(message => message.Data.Length == 8);
        var context = actual[0].EncryptionContext.ShouldNotBeNull();
        context.IsEncrypted.ShouldBeFalse();
        context.BatchSize.ShouldBe(5);
        actual.ShouldAllBe(message => ReferenceEquals(message.EncryptionContext, context));
    }

    // The key name of the Java client is a name, and the public key of it is the one of the file
    [Fact]
    public async Task Consume_GivenDotPulsarProducedRsaEncryptedMessage_ShouldBeDecryptedByJavaCli()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await CopyPrivateKeyToPulsar("ClientRsa", "client-rsa");
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topic, "client-rsa.pem", "ClientRsa");

        //Act
        _ = await producer.Send("dotpulsar-rsa-0", _cts.Token);
        var output = await ConsumeUsingJavaClient(topic, "interop", "-ekv file:///pulsar/keys/private-key.client-rsa.pem");

        //Assert
        output.ShouldContain("content:dotpulsar-rsa-0");
        await AssertStoredEncrypted(client, topic, "client-rsa.pem", "ClientRsa", 1);
    }

    // Proves the byte format of the wrapped data key (ECIES) against the BouncyCastle of the Java client
    [Fact]
    public async Task Consume_GivenDotPulsarProducedEcdsaEncryptedMessage_ShouldBeDecryptedByJavaCli()
    {
        //Arrange
        SkipWhenExplicitCurvesAreNotSupported();
        var topic = await _fixture.CreateTopic(_cts.Token);
        await CopyPrivateKeyToPulsar("ClientEcdsa", "client-ecdsa");
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topic, "client-ecdsa.pem", "ClientEcdsa");

        //Act
        _ = await producer.Send("dotpulsar-ecdsa-0", _cts.Token);
        var output = await ConsumeUsingJavaClient(topic, "interop", "-ekv file:///pulsar/keys/private-key.client-ecdsa.pem");

        //Assert
        output.ShouldContain("content:dotpulsar-ecdsa-0");
        await AssertStoredEncrypted(client, topic, "client-ecdsa.pem", "ClientEcdsa", 1);
    }

    // The payload is compressed before it is encrypted, so the Java client decrypts it before it decompresses it
    [Fact]
    public async Task Consume_GivenDotPulsarProducedEncryptedLz4Message_ShouldBeDecryptedByJavaCli()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await CopyPrivateKeyToPulsar("ClientRsa", "client-rsa");
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topic, "client-rsa.pem", "ClientRsa", builder => builder.CompressionType(DotPulsar.CompressionType.Lz4));
        var message = "dotpulsar-lz4-0 " + new string('a', 500);

        //Act
        _ = await producer.Send(message, _cts.Token);
        var output = await ConsumeUsingJavaClient(topic, "interop", "-ekv file:///pulsar/keys/private-key.client-rsa.pem");

        //Assert
        output.ShouldContain($"content:{message}");
        await AssertStoredEncrypted(client, topic, "client-rsa.pem", "ClientRsa", 1, DotPulsar.CompressionType.Lz4);
    }

    // A batch is encrypted as a whole, so the Java client decrypts it before it parses the batch
    [Fact]
    public async Task Consume_GivenDotPulsarProducedEncryptedBatch_ShouldBeDecryptedByJavaCli()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await CopyPrivateKeyToPulsar("ClientRsa", "client-rsa");
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topic, "client-rsa.pem", "ClientRsa", builder => builder.EnableBatching(true).BatchingMaxMessages(3).BatchingMaxPublishDelay(TimeSpan.FromHours(1)));

        //Act
        foreach (var message in new[] { "dotpulsar-batch-0", "dotpulsar-batch-1", "dotpulsar-batch-2" })
        {
            await producer.SendChannel.Send(message, null, _cts.Token);
        }

        await producer.Flush(_cts.Token);
        var output = await ConsumeUsingJavaClient(topic, "interop", "-n 3 -ekv file:///pulsar/keys/private-key.client-rsa.pem");

        //Assert
        output.ShouldContain("content:dotpulsar-batch-0");
        output.ShouldContain("content:dotpulsar-batch-1");
        output.ShouldContain("content:dotpulsar-batch-2");
        await AssertStoredEncrypted(client, topic, "client-rsa.pem", "ClientRsa", 3);
    }

    // Every chunk is encrypted on its own, and the Java client decrypts the chunks before it assembles them
    [Fact]
    public async Task Consume_GivenDotPulsarProducedEncryptedChunkedMessage_ShouldBeAssembledAndDecryptedByJavaCli()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await CopyPrivateKeyToPulsar("ClientRsa", "client-rsa");
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topic, "client-rsa.pem", "ClientRsa", builder => builder.EnableChunking(true).ChunkMaxMessageSize(1000));
        var message = "dotpulsar-chunked-" + new string('b', 4000);

        //Act
        var messageId = await producer.Send(message, _cts.Token);
        var output = await ConsumeUsingJavaClient(topic, "interop", "-ekv file:///pulsar/keys/private-key.client-rsa.pem");

        //Assert
        messageId.GetChunkMessageIds()!.Length.ShouldBeGreaterThanOrEqualTo(5);
        output.ShouldContain($"content:{message}");
        await AssertStoredEncrypted(client, topic, "client-rsa.pem", "ClientRsa", 1);
    }

    // The Java client does not read the message without a key, discards it and acknowledges it
    [Fact]
    public async Task Consume_GivenDotPulsarProducedEncryptedMessageWithoutKey_ShouldNotBeDelivered()
    {
        //Arrange
        const string subscription = "interop";
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topic, "client-rsa.pem", "ClientRsa");
        _ = await producer.Send("dotpulsar-rsa-0", _cts.Token);

        //Act
        var output = await ConsumeUsingJavaClient(topic, subscription, "-ca DISCARD", timeoutInSeconds: 10, expectMessage: false);

        //Assert
        output.ShouldNotContain("content:");
        await WaitUntil(async () => await GetBacklog(topic, subscription) == 0);
    }

    // The tool produces a message that is encrypted with the public key, and the consumer has the private key
    private async Task ProduceUsingJavaClient(string topic, string keyName, string keySet, string message)
    {
        var keyFile = $"/pulsar/keys/{keyName}.pub";
        await _fixture.CopyToPulsar(CryptoTestKeys.Public(keySet), keyFile.TrimStart('/'), _cts.Token);
        var result = await _fixture.ExecInPulsar($"bin/pulsar-client produce {topic} -m '{message}' -n 1 -ekn {keyName} -ekv file://{keyFile}", _cts.Token);

        if (result.ExitCode != 0)
            throw new Exception($"Could not produce the encrypted message: {result.Stderr}");
    }

    // The tool counts the messages when they are acknowledged, so it can send a few more than it was asked to
    private async Task ProduceUsingPerf(string topic, int numberOfMessages, int messageSize, string keyName, string keyFile, string options, bool batching = false, int rate = 10)
    {
        var command = $"bin/pulsar-perf produce {topic} -m {numberOfMessages} -s {messageSize} -r {rate} {(batching ? string.Empty : "-db")} -k {keyName} -v /pulsar/keys/{keyFile} {options}";
        var result = await _fixture.ExecInPulsar(command, _cts.Token);

        if (result.ExitCode != 0)
            throw new Exception($"Could not produce the encrypted messages: {result.Stderr}");
    }

    // The tool prints the content of the messages that it receives. It ends when it has received the messages, or when the time is up, which is the expected end if it receives nothing.
    private async Task<string> ConsumeUsingJavaClient(string topic, string subscription, string options, int timeoutInSeconds = 60, bool expectMessage = true)
    {
        var numberOfMessages = options.Contains("-n ", StringComparison.Ordinal) ? string.Empty : "-n 1";
        var result = await _fixture.ExecInPulsar($"timeout {timeoutInSeconds} bin/pulsar-client consume {topic} -s {subscription} -p Earliest {numberOfMessages} {options}", _cts.Token);
        _testOutputHelper.WriteLine(result.Stdout);

        if (expectMessage && result.ExitCode != 0)
            throw new Exception($"The Java client could not consume the encrypted message: {result.Stderr}{result.Stdout}");

        return result.Stdout;
    }

    // The Java client also reads a message that was not encrypted, so this proves that the producer encrypted it: only a message with encryption keys has an encryption context
    private async Task AssertStoredEncrypted(IPulsarClient client, string topic, string keyName, string privateKeySet, int numberOfMessages, DotPulsar.CompressionType? compressionType = null)
    {
        await using var reader = client.NewReader(Schema.String)
            .StartMessageId(MessageId.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .DefaultCryptoKeyReader(CreateDataUri(CryptoTestKeys.Private(privateKeySet)))
            .Create();

        for (var i = 0; i < numberOfMessages; ++i)
        {
            var message = await TransactionFixture.ReceiveOrNull(reader, TimeSpan.FromSeconds(60), _cts.Token);
            message.ShouldNotBeNull();
            var context = message.EncryptionContext.ShouldNotBeNull("the message is not encrypted");
            context.IsEncrypted.ShouldBeFalse();
            context.Keys.Keys.ShouldBe([keyName]);
            context.Param.Length.ShouldBe(12);

            if (compressionType is not null)
                context.CompressionType.ShouldBe(compressionType.Value);
        }
    }

    private async Task CopyPrivateKeyToPulsar(string keySet, string name)
        => await _fixture.CopyToPulsar(CryptoTestKeys.Private(keySet), $"pulsar/keys/private-key.{name}.pem", _cts.Token);

    private async Task CopyPublicKeys()
    {
        await _fixture.CopyToPulsar(CryptoTestKeys.Public("ClientRsa"), "pulsar/keys/public-key.client-rsa.pem", _cts.Token);
        await _fixture.CopyToPulsar(CryptoTestKeys.Public("ClientEcdsa"), "pulsar/keys/public-key.client-ecdsa.pem", _cts.Token);
    }

    private static void SkipWhenExplicitCurvesAreNotSupported()
        => Assert.SkipWhen(OperatingSystem.IsMacOS(), "The platform does not support importing keys with explicit curve parameters");

    private static string CreateDataUri(byte[] key)
        => "data:application/x-pem-file;base64," + Convert.ToBase64String(key);

    private IProducer<string> CreateProducer(IPulsarClient client, string topic, string keyName, string publicKeySet, Action<IProducerBuilder<string>>? configure = null)
    {
        var builder = client.NewProducer(Schema.String)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .EncryptionKey(keyName)
            .DefaultCryptoKeyReader(CreateDataUri(CryptoTestKeys.Public(publicKeySet)));

        configure?.Invoke(builder);
        return builder.Create();
    }

    private IConsumer<string> CreateConsumer(IPulsarClient client, string topic, string privateKeySet)
        => client.NewConsumer(Schema.String)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .Topic(topic)
            .DefaultCryptoKeyReader(CreateDataUri(CryptoTestKeys.Private(privateKeySet)))
            .Create();

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();

    private async Task<IMessage<string>> Receive(IConsumer<string> consumer)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(60), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    // Until nothing has been received for 5 seconds
    private async Task<List<IMessage<string>>> ReceiveAll(IConsumer<string> consumer)
    {
        var messages = new List<IMessage<string>> { await Receive(consumer) };

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
}
