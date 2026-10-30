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

namespace DotPulsar.Tests.Internal;

using DotPulsar.Abstractions;
using DotPulsar.Internal;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using ApiMetadata = DotPulsar.Internal.PulsarApi.MessageMetadata;
using ByteString = Google.Protobuf.ByteString;
using SentMetadata = DotPulsar.MessageMetadata;

[Trait("Category", "Unit")]
public sealed class DeadLetterHandlerTests
{
    private const string Topic = "persistent://public/default/my-topic";
    private const string Subscription = "my-subscription";
    private const string ConsumerName = "my-consumer";

    private static readonly PropertyInfo _apiMetadataProperty = typeof(SentMetadata).GetProperty("Metadata", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("MessageMetadata.Metadata was not found");

    private readonly SchemaInfo _schemaInfo;
    private readonly IProducer<ReadOnlySequence<byte>> _producer;
    private readonly Func<ProducerOptions<ReadOnlySequence<byte>>, string?, IProducer<ReadOnlySequence<byte>>> _producerFactory;
    private readonly ConcurrentQueue<SentMetadata> _sentMetadata;
    private readonly ConcurrentQueue<string> _sentPayloads;
    private readonly ConcurrentQueue<MessageId> _acknowledged;
    private readonly Func<MessageId, CancellationToken, ValueTask> _acknowledge;

    public DeadLetterHandlerTests()
    {
        _schemaInfo = new SchemaInfo("dead-letter-schema", Array.Empty<byte>(), SchemaType.String, ImmutableDictionary<string, string>.Empty);
        _producer = CreateProducer();
        _producerFactory = Substitute.For<Func<ProducerOptions<ReadOnlySequence<byte>>, string?, IProducer<ReadOnlySequence<byte>>>>();
        _producerFactory(Arg.Any<ProducerOptions<ReadOnlySequence<byte>>>(), Arg.Any<string?>()).Returns(_producer);
        _sentMetadata = new ConcurrentQueue<SentMetadata>();
        _sentPayloads = new ConcurrentQueue<string>();
        _acknowledged = new ConcurrentQueue<MessageId>();
        _acknowledge = (messageId, _) =>
        {
            _acknowledged.Enqueue(messageId);
            return default;
        };
        RecordSends(_producer);
    }

    // Additional coverage
    [Theory]
    [InlineData("persistent://public/default/t", "sub", "persistent://public/default/t-sub-DLQ")]
    [InlineData("persistent://public/default/t-partition-0", "sub", "persistent://public/default/t-partition-0-sub-DLQ")]
    [InlineData("my-topic", "my-subscription", "my-topic-my-subscription-DLQ")]
    public void Constructor_GivenNoDeadLetterTopic_ShouldUseTopicSubscriptionDlqSuffix(string topic, string subscriptionName, string expected)
    {
        //Arrange & Act
        var uut = new DeadLetterHandler<ReadOnlySequence<byte>>(new DeadLetterPolicy(3), topic, subscriptionName, ConsumerName, _schemaInfo, _producerFactory);

        //Assert
        uut.DeadLetterTopic.ShouldBe(expected);
    }

    // Additional coverage
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_GivenBlankDeadLetterTopic_ShouldUseTopicSubscriptionDlqSuffix(string deadLetterTopic)
    {
        //Arrange
        var policy = new DeadLetterPolicy(3) { DeadLetterTopic = deadLetterTopic };

        //Act
        var uut = CreateHandler(policy);

        //Assert
        uut.DeadLetterTopic.ShouldBe($"{Topic}-{Subscription}-DLQ");
    }

    // Additional coverage
    [Fact]
    public void Constructor_GivenCustomDeadLetterTopic_ShouldUseIt()
    {
        //Arrange
        var policy = new DeadLetterPolicy(3) { DeadLetterTopic = "persistent://public/default/my-custom-dlq" };

        //Act
        var uut = CreateHandler(policy);

        //Assert
        uut.DeadLetterTopic.ShouldBe("persistent://public/default/my-custom-dlq");
    }

    // Additional coverage
    [Fact]
    public void Constructor_GivenPolicy_ShouldExposeMaxRedeliverCountAndInitialSubscriptionName()
    {
        //Arrange
        var policy = new DeadLetterPolicy(7) { InitialSubscriptionName = "dlq-subscription" };

        //Act
        var uut = CreateHandler(policy);

        //Assert
        uut.MaxRedeliverCount.ShouldBe(7u);
        uut.InitialSubscriptionName.ShouldBe("dlq-subscription");
    }

    // Additional coverage
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void Constructor_GivenNoInitialSubscriptionName_ShouldExposeNull(string? initialSubscriptionName)
    {
        //Arrange
        var policy = new DeadLetterPolicy(3) { InitialSubscriptionName = initialSubscriptionName };

        //Act
        var uut = CreateHandler(policy);

        //Assert
        uut.InitialSubscriptionName.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void Constructor_GivenHandler_ShouldNotCreateProducer()
    {
        //Arrange & Act
        _ = CreateHandler();

        //Assert
        _producerFactory.DidNotReceiveWithAnyArgs()(default!, default);
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterWithoutConsumerReceiveImmediately
    [Fact]
    public async Task TryDeadLetter_GivenUnregisteredEntry_ShouldReturnFalseAndNotCreateProducer()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        var actual = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeFalse();
        _producerFactory.DidNotReceiveWithAnyArgs()(default!, default);
        _acknowledged.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenRegisteredEntry_ShouldSendAllMessagesThenAcknowledgeEntryAndRemove()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0"), CreateMessage(1, "payload-1"), CreateMessage(2, "payload-2")]);

        //Act
        var actual = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeTrue();
        _sentPayloads.ShouldBe(["payload-0", "payload-1", "payload-2"]);
        _acknowledged.ShouldBe([new MessageId(1, 2, 0, -1, Topic)]);
        uut.Count.ShouldBe(0);
        uut.Contains(1, 2).ShouldBeFalse();
    }

    // Upstream: DeadLetterTopicTest#testPossibleToDeadLetterEntryIsRemovedWhenRedeliveringABatchMessageId
    [Fact]
    public async Task TryDeadLetter_GivenBatchIndexMessageId_ShouldResolveEntryAndRemoveIt()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0"), CreateMessage(1, "payload-1")]);

        //Act
        var actual = await uut.TryDeadLetter(new MessageId(1, 2, 0, 1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeTrue();
        _sentPayloads.ShouldBe(["payload-0", "payload-1"]);
        _acknowledged.Single().BatchIndex.ShouldBe(-1);
        uut.Contains(1, 2).ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenNonBatchedMessage_ShouldSendItAndAcknowledgeItsId()
    {
        //Arrange
        var uut = CreateHandler();
        var messageId = new MessageId(1, 2, 3, -1, Topic);
        uut.Register(1, 2, CreateMessage(messageId, "payload"));

        //Act
        var actual = await uut.TryDeadLetter(messageId, _acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeTrue();
        _sentPayloads.ShouldBe(["payload"]);
        _acknowledged.ShouldBe([messageId]);
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenSendFails_ShouldReturnFalseAndKeepEntry()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        _producer.Send(Arg.Any<SentMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>())
            .Returns(_ => ValueTask.FromException<MessageId>(new InvalidOperationException("Send failed")));

        //Act
        var actual = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeFalse();
        uut.Contains(1, 2).ShouldBeTrue();
        _acknowledged.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenSendThrowsSynchronously_ShouldReturnFalseAndKeepEntry()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        _producer.Send(Arg.Any<SentMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>())
            .Returns<ValueTask<MessageId>>(_ => throw new InvalidOperationException("Send failed"));

        //Act
        var actual = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeFalse();
        uut.Contains(1, 2).ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenSecondMessageOfBatchFails_ShouldReturnFalseKeepEntryAndNotAcknowledge()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0"), CreateMessage(1, "payload-1"), CreateMessage(2, "payload-2")]);
        var sends = 0;
        _producer.Send(Arg.Any<SentMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>())
            .Returns(_ => ++sends == 2
                ? ValueTask.FromException<MessageId>(new InvalidOperationException("Send failed"))
                : new ValueTask<MessageId>(new MessageId(9, 9, 0, -1)));

        //Act
        var actual = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeFalse();
        sends.ShouldBe(2);
        uut.Contains(1, 2).ShouldBeTrue();
        _acknowledged.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenAcknowledgeFails_ShouldReturnFalseAndEntryRemoved()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        var acknowledge = Substitute.For<Func<MessageId, CancellationToken, ValueTask>>();
        acknowledge(Arg.Any<MessageId>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException(new InvalidOperationException("Acknowledge failed")));

        //Act
        var actual = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeFalse();
        _sentPayloads.ShouldBe(["payload-0"]);
        uut.Contains(1, 2).ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenFaultedProducer_ShouldDisposeAndRecreateProducerOnNextCall()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        var secondProducer = CreateProducer();
        RecordSends(secondProducer);
        _producerFactory(Arg.Any<ProducerOptions<ReadOnlySequence<byte>>>(), Arg.Any<string?>()).Returns(_producer, secondProducer);
        _producer.Send(Arg.Any<SentMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>())
            .Returns(_ => ValueTask.FromException<MessageId>(new InvalidOperationException("Producer faulted")));
        _producer.State.IsFinalState().Returns(true);

        //Act
        var first = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);
        var second = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        first.ShouldBeFalse();
        second.ShouldBeTrue();
        await _producer.Received(1).DisposeAsync();
        _producerFactory.ReceivedWithAnyArgs(2)(default!, default);
        await secondProducer.DidNotReceive().DisposeAsync();
        _sentPayloads.ShouldBe(["payload-0"]);
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenProducerFaultedBetweenCalls_ShouldDisposeAndRecreateProducer()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        uut.Register(1, 3, [CreateMessage(0, "payload-1")]);
        var secondProducer = CreateProducer();
        RecordSends(secondProducer);
        _producerFactory(Arg.Any<ProducerOptions<ReadOnlySequence<byte>>>(), Arg.Any<string?>()).Returns(_producer, secondProducer);
        _ = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);
        _producer.State.IsFinalState().Returns(true);

        //Act
        var actual = await uut.TryDeadLetter(new MessageId(1, 3, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeTrue();
        await _producer.Received(1).DisposeAsync();
        _producerFactory.ReceivedWithAnyArgs(2)(default!, default);
        _sentPayloads.ShouldBe(["payload-0", "payload-1"]);
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenSendFailsButProducerIsUsable_ShouldKeepTheProducer()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        _producer.Send(Arg.Any<SentMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>())
            .Returns(
                _ => ValueTask.FromException<MessageId>(new InvalidOperationException("Send failed")),
                _ => new ValueTask<MessageId>(new MessageId(9, 9, 0, -1)));

        //Act
        var first = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);
        var second = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        first.ShouldBeFalse();
        second.ShouldBeTrue();
        _producerFactory.ReceivedWithAnyArgs(1)(default!, default);
        await _producer.DidNotReceive().DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenProducerFactoryThrows_ShouldReturnFalseKeepEntryAndRetryOnNextCall()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        _producerFactory(Arg.Any<ProducerOptions<ReadOnlySequence<byte>>>(), Arg.Any<string?>())
            .Returns(_ => throw new InvalidOperationException("Creation failed"), _ => _producer);

        //Act
        var first = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);
        var second = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        first.ShouldBeFalse();
        second.ShouldBeTrue();
        _producerFactory.ReceivedWithAnyArgs(2)(default!, default);
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenCalledTwice_ShouldCreateOneProducer()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        uut.Register(1, 3, [CreateMessage(0, "payload-1")]);

        //Act
        _ = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);
        _ = await uut.TryDeadLetter(new MessageId(1, 3, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        _producerFactory.ReceivedWithAnyArgs(1)(default!, default);
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenCalledConcurrently_ShouldCreateOneProducer()
    {
        //Arrange
        const int entries = 32;
        var uut = CreateHandler();
        for (ulong entryId = 0; entryId < entries; ++entryId)
            uut.Register(1, entryId, [CreateMessage(0, $"payload-{entryId}")]);
        _producerFactory(Arg.Any<ProducerOptions<ReadOnlySequence<byte>>>(), Arg.Any<string?>())
            .Returns(_ =>
            {
                Thread.Sleep(20);
                return _producer;
            });
        using var start = new ManualResetEventSlim();

        //Act
        var tasks = Enumerable.Range(0, entries).Select(entryId => Run(async () =>
        {
            start.Wait(Current.CancellationToken);
            return await uut.TryDeadLetter(new MessageId(1, (ulong) entryId, 0, -1, Topic), _acknowledge, Current.CancellationToken);
        })).ToArray();
        start.Set();
        var results = await Task.WhenAll(tasks);

        //Assert
        results.ShouldAllBe(result => result);
        _producerFactory.ReceivedWithAnyArgs(1)(default!, default);
        uut.Count.ShouldBe(0);
        _acknowledged.Count.ShouldBe(entries);
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenFirstUse_ShouldCreateProducerWithDeadLetterTopicSchemaAndName()
    {
        //Arrange
        var policy = new DeadLetterPolicy(3) { DeadLetterTopic = "persistent://public/default/my-dlq", InitialSubscriptionName = "dlq-subscription" };
        var uut = CreateHandler(policy);
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        ProducerOptions<ReadOnlySequence<byte>>? options = null;
        string? initialSubscriptionName = null;
        _producerFactory(Arg.Any<ProducerOptions<ReadOnlySequence<byte>>>(), Arg.Any<string?>())
            .Returns(call =>
            {
                options = call.Arg<ProducerOptions<ReadOnlySequence<byte>>>();
                initialSubscriptionName = call.Arg<string?>();
                return _producer;
            });

        //Act
        _ = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        options.ShouldNotBeNull();
        options.Topic.ShouldBe("persistent://public/default/my-dlq");
        options.Schema.SchemaInfo.ShouldBeSameAs(_schemaInfo);
        options.MaxPendingMessages.ShouldBe(500u);
        options.CompressionType.ShouldBe(DotPulsar.CompressionType.None);
        initialSubscriptionName.ShouldBe("dlq-subscription");
    }

    // Upstream: ConsumerImpl#initDeadLetterProducerIfNeeded
    [Fact]
    public async Task TryDeadLetter_GivenFirstUse_ShouldCreateProducerThatChunksAndDoesNotBatch()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        ProducerOptions<ReadOnlySequence<byte>>? options = null;
        _producerFactory(Arg.Any<ProducerOptions<ReadOnlySequence<byte>>>(), Arg.Any<string?>())
            .Returns(call =>
            {
                options = call.Arg<ProducerOptions<ReadOnlySequence<byte>>>();
                return _producer;
            });

        //Act
        _ = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        options.ShouldNotBeNull();
        options.EnableChunking.ShouldBeTrue();
        options.EnableBatching.ShouldBeFalse("chunking and batching can not be combined");
        options.ChunkMaxMessageSize.ShouldBe(0);
    }

    // Upstream: ConsumerImpl#initDeadLetterProducerIfNeeded (the producer has no encryption keys, so the message is published to the dead letter topic as plain text, as in the Java client)
    [Fact]
    public async Task TryDeadLetter_GivenFirstUse_ShouldCreateProducerThatDoesNotEncrypt()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        ProducerOptions<ReadOnlySequence<byte>>? options = null;
        _producerFactory(Arg.Any<ProducerOptions<ReadOnlySequence<byte>>>(), Arg.Any<string?>())
            .Returns(call =>
            {
                options = call.Arg<ProducerOptions<ReadOnlySequence<byte>>>();
                return _producer;
            });

        //Act
        _ = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        options.ShouldNotBeNull();
        options.EncryptionKeys.ShouldBeEmpty();
        options.CryptoKeyReader.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenNoInitialSubscriptionName_ShouldCreateProducerWithoutIt()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);

        //Act
        _ = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        _producerFactory.Received(1)(Arg.Any<ProducerOptions<ReadOnlySequence<byte>>>(), Arg.Is<string?>(initialSubscriptionName => initialSubscriptionName == null));
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicWithProducerName
    [Fact]
    public async Task TryDeadLetter_GivenConsumerName_ShouldCreateProducerWithJavaCompatibleName()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        ProducerOptions<ReadOnlySequence<byte>>? options = null;
        _producerFactory(Arg.Any<ProducerOptions<ReadOnlySequence<byte>>>(), Arg.Any<string?>())
            .Returns(call =>
            {
                options = call.Arg<ProducerOptions<ReadOnlySequence<byte>>>();
                return _producer;
            });

        //Act
        _ = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        options.ShouldNotBeNull();
        options.ProducerName.ShouldNotBeNull();
        Regex.IsMatch(options.ProducerName, $"^{Regex.Escape(Topic)}-{Subscription}-{ConsumerName}-[a-zA-Z0-9]{{5}}-DLQ$").ShouldBeTrue(options.ProducerName);
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenMessageOfAnotherType_ShouldSendTheRawPayload()
    {
        //Arrange
        var uut = new DeadLetterHandler<string>(new DeadLetterPolicy(3), Topic, Subscription, ConsumerName, _schemaInfo, _producerFactory);
        var message = new MessageFactory<string>(DotPulsar.Schema.String).Create(
            new MessageId(1, 2, 0, -1, Topic),
            4,
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("string-payload")),
            CreateApiMetadata());
        uut.Register(1, 2, message);

        //Act
        var actual = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeTrue();
        _sentPayloads.ShouldBe(["string-payload"]);
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenSendNeverCompletes_ShouldReturnFalseAfterTheSendTimeoutAndKeepEntry()
    {
        //Arrange
        var uut = CreateHandler(sendTimeout: TimeSpan.FromMilliseconds(50));
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        _producer.Send(Arg.Any<SentMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>())
            .Returns(call => WaitUntilCancelled(call.Arg<CancellationToken>()));

        //Act
        var actual = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeFalse();
        uut.Contains(1, 2).ShouldBeTrue();
        _acknowledged.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenCancelledToken_ShouldThrowAndKeepEntry()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, cts.Token));

        //Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
        uut.Contains(1, 2).ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenTokenCancelledWhileSending_ShouldThrowAndKeepEntry()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Current.CancellationToken);
        var sending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _producer.Send(Arg.Any<SentMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                sending.TrySetResult();
                return WaitUntilCancelled(call.Arg<CancellationToken>());
            });
        var task = uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, cts.Token).AsTask();
        await sending.Task.WaitAsync(TimeSpan.FromSeconds(30), Current.CancellationToken);

        //Act
        cts.Cancel();
        var exception = await Record.ExceptionAsync(() => task);

        //Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
        uut.Contains(1, 2).ShouldBeTrue();
        _acknowledged.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenDisposedHandler_ShouldReturnFalseAndNotCreateProducer()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        await uut.DisposeAsync();

        //Act
        var actual = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        actual.ShouldBeFalse();
        _producerFactory.DidNotReceiveWithAnyArgs()(default!, default);
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenRegisteredAgainWhileFirstCandidateIsKept_ShouldSendTheLatestMessages()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "old-0"), CreateMessage(1, "old-1"), CreateMessage(2, "old-2")]);
        uut.Register(1, 2, [CreateMessage(1, "new-1")]);

        //Act
        _ = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        _sentPayloads.ShouldBe(["new-1"]);
    }

    // Additional coverage
    [Fact]
    public async Task DisposeAsync_GivenCreatedProducer_ShouldDisposeItOnce()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        _ = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Act
        await uut.DisposeAsync();
        await uut.DisposeAsync();

        //Assert
        await _producer.Received(1).DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public async Task DisposeAsync_GivenNoProducer_ShouldNotCreateOneAndClearCandidates()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);

        //Act
        await uut.DisposeAsync();

        //Assert
        _producerFactory.DidNotReceiveWithAnyArgs()(default!, default);
        uut.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public async Task DisposeAsync_GivenProducerDisposeThrows_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        _ = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);
        _producer.DisposeAsync().Returns(_ => ValueTask.FromException(new InvalidOperationException("Dispose failed")));

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.DisposeAsync());

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public async Task DisposeAsync_GivenProducerBeingCreated_ShouldDisposeTheCreatedProducer()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0")]);
        var creating = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        _producerFactory(Arg.Any<ProducerOptions<ReadOnlySequence<byte>>>(), Arg.Any<string?>())
            .Returns(_ =>
            {
                creating.TrySetResult();
                release.Wait(Current.CancellationToken);
                return _producer;
            });
        var deadLetterTask = Run(async () => await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken));
        await creating.Task.WaitAsync(TimeSpan.FromSeconds(30), Current.CancellationToken);

        //Act
        var disposeTask = uut.DisposeAsync().AsTask();
        release.Set();
        await Task.WhenAll(deadLetterTask, disposeTask);

        //Assert
        await _producer.Received(1).DisposeAsync();
    }

    // Additional coverage
    [Fact]
    public void Register_GivenNonBatchedMessage_ShouldAddCandidate()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        uut.Register(1, 2, CreateMessage(new MessageId(1, 2, 0, -1, Topic), "payload"));

        //Assert
        uut.Contains(1, 2).ShouldBeTrue();
        uut.Count.ShouldBe(1);
    }

    // Additional coverage
    [Fact]
    public void Register_GivenBatchedMessages_ShouldAddOneCandidatePerEntry()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        uut.Register(1, 2, [CreateMessage(0, "a"), CreateMessage(1, "b")]);
        uut.Register(1, 3, [CreateMessage(0, "c")]);

        //Assert
        uut.Count.ShouldBe(2);
        uut.Contains(1, 2).ShouldBeTrue();
        uut.Contains(1, 3).ShouldBeTrue();
        uut.Contains(2, 2).ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Register_GivenNoBatchedMessages_ShouldNotAddCandidate()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        uut.Register(1, 2, Array.Empty<IMessage<ReadOnlySequence<byte>>>());

        //Assert
        uut.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public void Register_GivenNoBatchedMessagesForRegisteredEntry_ShouldRemoveCandidate()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "a")]);

        //Act
        uut.Register(1, 2, Array.Empty<IMessage<ReadOnlySequence<byte>>>());

        //Assert
        uut.Contains(1, 2).ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Remove_GivenRegisteredEntry_ShouldRemove()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "a")]);
        uut.Register(1, 3, [CreateMessage(0, "b")]);

        //Act
        uut.Remove(1, 2);

        //Assert
        uut.Contains(1, 2).ShouldBeFalse();
        uut.Contains(1, 3).ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void Remove_GivenUnregisteredEntry_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        var exception = Record.Exception(() => uut.Remove(1, 2));

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void Clear_ShouldRemoveAll()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "a")]);
        uut.Register(1, 3, CreateMessage(new MessageId(1, 3, 0, -1, Topic), "b"));

        //Act
        uut.Clear();

        //Assert
        uut.Count.ShouldBe(0);
    }

    // Additional coverage
    [Fact]
    public async Task RegisterRemoveAndTryDeadLetter_GivenConcurrentUse_ShouldSendEveryEntryAndEndEmpty()
    {
        //Arrange
        const int entries = 200;
        var uut = CreateHandler();
        using var stop = new CancellationTokenSource();

        //Act
        var registerTasks = Enumerable.Range(0, 4).Select(thread => Run(() =>
        {
            for (var entryId = thread; entryId < entries; entryId += 4)
                uut.Register(1, (ulong) entryId, [CreateMessage(0, $"payload-{entryId}")]);
        })).ToArray();
        await Task.WhenAll(registerTasks);

        var observeTask = Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                for (ulong entryId = 0; entryId < entries; ++entryId)
                    _ = uut.Contains(1, entryId);

                _ = uut.Count;
            }
        });

        var deadLetterTasks = Enumerable.Range(0, 4).Select(thread => Run(async () =>
        {
            for (var entryId = 0; entryId < entries; ++entryId)
                _ = await uut.TryDeadLetter(new MessageId(1, (ulong) entryId, 0, -1, Topic), _acknowledge, Current.CancellationToken);
        })).ToArray();
        await Task.WhenAll(deadLetterTasks);
        stop.Cancel();
        await observeTask;

        //Assert
        uut.Count.ShouldBe(0);
        _producerFactory.ReceivedWithAnyArgs(1)(default!, default);
        _sentPayloads.Distinct().Count().ShouldBe(entries);
    }

    // Upstream: ConsumerImpl#getPropertiesMap, ConsumerImpl#copyMessageKeysIfNeeded, ConsumerImpl#copyMessageEventTime
    [Fact]
    public void CreateMetadata_GivenMessage_ShouldCopyPropertiesKeyOrderingKeyEventTimeAndAddOriginInfo()
    {
        //Arrange
        var uut = CreateHandler();
        var apiMetadata = CreateApiMetadata();
        apiMetadata.PartitionKey = "my-key";
        apiMetadata.OrderingKey = ByteString.CopyFromUtf8("my-ordering-key");
        apiMetadata.EventTime = 2000;
        apiMetadata.Properties.Add(new KeyValue { Key = "zeta", Value = "z" });
        apiMetadata.Properties.Add(new KeyValue { Key = "alpha", Value = "a" });
        var message = CreateMessage(new MessageId(5, 6, 0, 2, Topic), "payload", apiMetadata);

        //Act
        var actual = uut.CreateMetadata(message);

        //Assert
        var properties = GetApiMetadata(actual).Properties;
        properties.Select(property => property.Key).ShouldBe(["ORIGIN_MESSAGE_ID", "ORIGIN_MESSAGE_IDY_TIME", "REAL_SUBSCRIPTION", "REAL_TOPIC", "alpha", "zeta"]);
        properties.ToDictionary(property => property.Key, property => property.Value).ShouldBe(new Dictionary<string, string>
        {
            ["ORIGIN_MESSAGE_ID"] = "5:6:0:2",
            ["ORIGIN_MESSAGE_IDY_TIME"] = "5:6:0:2",
            ["REAL_SUBSCRIPTION"] = Subscription,
            ["REAL_TOPIC"] = Topic,
            ["alpha"] = "a",
            ["zeta"] = "z"
        });
        actual.Key.ShouldBe("my-key");
        actual.OrderingKey.ShouldBe("my-ordering-key"u8.ToArray());
        actual.EventTime.ShouldBe(2000ul);
    }

    // Upstream: DeadLetterTopicTest#testDeadLetterTopicHasOriginalInfo
    [Fact]
    public void CreateMetadata_GivenUserPropertiesWithOriginInfoNames_ShouldNotOverwriteThem()
    {
        //Arrange
        var uut = CreateHandler();
        var apiMetadata = CreateApiMetadata();
        apiMetadata.Properties.Add(new KeyValue { Key = "REAL_TOPIC", Value = "user-topic" });
        apiMetadata.Properties.Add(new KeyValue { Key = "ORIGIN_MESSAGE_ID", Value = "user-id" });
        apiMetadata.Properties.Add(new KeyValue { Key = "ORIGIN_MESSAGE_IDY_TIME", Value = "user-id-old" });
        apiMetadata.Properties.Add(new KeyValue { Key = "REAL_SUBSCRIPTION", Value = "user-subscription" });
        var message = CreateMessage(new MessageId(5, 6, 0, -1, Topic), "payload", apiMetadata);

        //Act
        var actual = uut.CreateMetadata(message);

        //Assert
        GetApiMetadata(actual).Properties.ToDictionary(property => property.Key, property => property.Value).ShouldBe(new Dictionary<string, string>
        {
            ["ORIGIN_MESSAGE_ID"] = "user-id",
            ["ORIGIN_MESSAGE_IDY_TIME"] = "user-id-old",
            ["REAL_SUBSCRIPTION"] = "user-subscription",
            ["REAL_TOPIC"] = "user-topic"
        });
    }

    // Additional coverage
    [Fact]
    public void CreateMetadata_GivenNonBatchedMessage_ShouldUseTheJavaFormatForTheOriginMessageId()
    {
        //Arrange
        var uut = CreateHandler();
        var message = CreateMessage(new MessageId(5, 6, 3, -1, Topic), "payload");

        //Act
        var actual = uut.CreateMetadata(message);

        //Assert
        actual["ORIGIN_MESSAGE_ID"].ShouldBe("5:6:3");
        actual["ORIGIN_MESSAGE_IDY_TIME"].ShouldBe("5:6:3");
    }

    // Additional coverage
    [Fact]
    public void CreateMetadata_GivenBase64Key_ShouldCopyKeyBytes()
    {
        //Arrange
        var uut = CreateHandler();
        var key = new byte[] { 0, 1, 2, 250, 251, 252 };
        var apiMetadata = CreateApiMetadata();
        apiMetadata.PartitionKey = Convert.ToBase64String(key);
        apiMetadata.PartitionKeyB64Encoded = true;
        var message = CreateMessage(new MessageId(5, 6, 0, -1, Topic), "payload", apiMetadata);

        //Act
        var actual = uut.CreateMetadata(message);

        //Assert
        actual.KeyBytes.ShouldBe(key);
        GetApiMetadata(actual).PartitionKeyB64Encoded.ShouldBeTrue();
        GetApiMetadata(actual).PartitionKey.ShouldBe(Convert.ToBase64String(key));
    }

    // Additional coverage
    [Fact]
    public void CreateMetadata_GivenNoKeyAndNoOrderingKey_ShouldNotSetThem()
    {
        //Arrange
        var uut = CreateHandler();
        var message = CreateMessage(new MessageId(5, 6, 0, -1, Topic), "payload");

        //Act
        var actual = uut.CreateMetadata(message);

        //Assert
        actual.OrderingKey.ShouldBeNull();
        GetApiMetadata(actual).HasPartitionKey.ShouldBeFalse();
        GetApiMetadata(actual).PartitionKeyB64Encoded.ShouldBeFalse();
        GetApiMetadata(actual).OrderingKey.IsEmpty.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public void CreateMetadata_GivenMessage_ShouldNotCopySequenceIdProducerNamePublishTimeOrSchemaVersion()
    {
        //Arrange
        var uut = CreateHandler();
        var apiMetadata = CreateApiMetadata();
        apiMetadata.SchemaVersion = ByteString.CopyFromUtf8("version");
        var message = CreateMessage(new MessageId(5, 6, 0, -1, Topic), "payload", apiMetadata);

        //Act
        var actual = uut.CreateMetadata(message);

        //Assert
        var api = GetApiMetadata(actual);
        api.HasSequenceId.ShouldBeFalse();
        api.HasProducerName.ShouldBeFalse();
        api.HasPublishTime.ShouldBeFalse();
        api.SchemaVersion.IsEmpty.ShouldBeTrue();
    }

    // Additional coverage
    [Fact]
    public async Task TryDeadLetter_GivenRegisteredMessages_ShouldSendMetadataOfEachMessage()
    {
        //Arrange
        var uut = CreateHandler();
        uut.Register(1, 2, [CreateMessage(0, "payload-0"), CreateMessage(1, "payload-1")]);

        //Act
        _ = await uut.TryDeadLetter(new MessageId(1, 2, 0, -1, Topic), _acknowledge, Current.CancellationToken);

        //Assert
        _sentMetadata.Select(metadata => metadata["ORIGIN_MESSAGE_ID"]).ShouldBe(["1:2:0:0", "1:2:0:1"]);
        _sentMetadata.ShouldAllBe(metadata => metadata["REAL_TOPIC"] == Topic && metadata["REAL_SUBSCRIPTION"] == Subscription);
    }

    // Additional coverage
    [Fact]
    public async Task OnRedeliveryCountExceeded_GivenCallback_ShouldInvokeItWithTheMessageId()
    {
        //Arrange
        var uut = CreateHandler();
        var invoked = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.RedeliveryCountExceeded = messageId =>
        {
            invoked.TrySetResult(messageId);
            return default;
        };
        var entryMessageId = new MessageId(1, 2, 0, -1, Topic);

        //Act
        uut.OnRedeliveryCountExceeded(entryMessageId);

        //Assert
        var actual = await invoked.Task.WaitAsync(TimeSpan.FromSeconds(30), Current.CancellationToken);
        actual.ShouldBeSameAs(entryMessageId);
    }

    // Additional coverage
    [Fact]
    public void OnRedeliveryCountExceeded_GivenNoCallback_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateHandler();

        //Act
        var exception = Record.Exception(() => uut.OnRedeliveryCountExceeded(new MessageId(1, 2, 0, -1, Topic)));

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void OnRedeliveryCountExceeded_GivenCallbackThatThrows_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateHandler();
        uut.RedeliveryCountExceeded = _ => throw new InvalidOperationException("Redelivery failed");

        //Act
        var exception = Record.Exception(() => uut.OnRedeliveryCountExceeded(new MessageId(1, 2, 0, -1, Topic)));

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void OnRedeliveryCountExceeded_GivenCallbackThatFaults_ShouldNotThrow()
    {
        //Arrange
        var uut = CreateHandler();
        uut.RedeliveryCountExceeded = _ => new ValueTask(Task.FromException(new InvalidOperationException("Redelivery failed")));

        //Act
        var exception = Record.Exception(() => uut.OnRedeliveryCountExceeded(new MessageId(1, 2, 0, -1, Topic)));

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void OnRedeliveryCountExceeded_GivenCallbackThatNeverCompletes_ShouldReturnWithoutWaiting()
    {
        //Arrange
        var uut = CreateHandler();
        var neverCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.RedeliveryCountExceeded = _ => new ValueTask(neverCompleted.Task);

        //Act
        var exception = Record.Exception(() => uut.OnRedeliveryCountExceeded(new MessageId(1, 2, 0, -1, Topic)));

        //Assert
        exception.ShouldBeNull();
    }

    private static Task Run(Action action)
        => Task.Run(action, Current.CancellationToken);

    private static Task<T> Run<T>(Func<Task<T>> function)
        => Task.Run(function, Current.CancellationToken);

    private static Task Run(Func<Task> function)
        => Task.Run(function, Current.CancellationToken);

    private DeadLetterHandler<ReadOnlySequence<byte>> CreateHandler(DeadLetterPolicy? policy = null, TimeSpan? sendTimeout = null)
        => new(policy ?? new DeadLetterPolicy(3), Topic, Subscription, ConsumerName, _schemaInfo, _producerFactory, sendTimeout);

    private static IProducer<ReadOnlySequence<byte>> CreateProducer()
    {
        var producer = Substitute.For<IProducer<ReadOnlySequence<byte>>>();
        producer.State.IsFinalState().Returns(false);
        producer.DisposeAsync().Returns(default(ValueTask));
        return producer;
    }

    private void RecordSends(IProducer<ReadOnlySequence<byte>> producer)
        => producer.Send(Arg.Any<SentMetadata>(), Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                _sentMetadata.Enqueue(call.Arg<SentMetadata>());
                _sentPayloads.Enqueue(Encoding.UTF8.GetString(call.Arg<ReadOnlySequence<byte>>().ToArray()));
                return new ValueTask<MessageId>(new MessageId(9, 9, 0, -1));
            });

    private static async ValueTask<MessageId> WaitUntilCancelled(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken);
        return new MessageId(9, 9, 0, -1);
    }

    private static ApiMetadata CreateApiMetadata()
        => new() { ProducerName = "original-producer", SequenceId = 42, PublishTime = 1000 };

    private static ApiMetadata GetApiMetadata(SentMetadata metadata)
        => (ApiMetadata) _apiMetadataProperty.GetValue(metadata)!;

    private static IMessage<ReadOnlySequence<byte>> CreateMessage(int batchIndex, string payload)
        => CreateMessage(new MessageId(1, 2, 0, batchIndex, Topic), payload);

    private static IMessage<ReadOnlySequence<byte>> CreateMessage(MessageId messageId, string payload, ApiMetadata? metadata = null)
        => new MessageFactory<ReadOnlySequence<byte>>(DotPulsar.Schema.ByteSequence).Create(
            messageId,
            4,
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(payload)),
            metadata ?? CreateApiMetadata());
}
