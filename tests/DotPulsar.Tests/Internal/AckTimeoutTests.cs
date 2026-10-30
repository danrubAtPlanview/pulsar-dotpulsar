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
using DotPulsar.Extensions;
using DotPulsar.Internal;

// The ack timeout tests of Java UnAcknowledgedMessagesTimeoutTest, PerMessageUnAcknowledgedRedeliveryTest and ConsumerRedeliveryTest that do not need the redelivery backoff (not supported).
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class AckTimeoutTests : IDisposable
{
    private static readonly TimeSpan _ackTimeout = TimeSpan.FromSeconds(2);
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public AckTimeoutTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Upstream: UnAcknowledgedMessagesTimeoutTest#testExclusiveSingleAckedNormalTopic (ackTimeoutRedeliveryBackoff = false)
    [Fact]
    public async Task Receive_GivenExclusiveSubscriptionAndUnacknowledgedMessages_ShouldRedeliverAfterAckTimeout()
    {
        //Arrange
        const int totalMessages = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName, SubscriptionType.Exclusive, 7);
        await using var producer = CreateProducer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await Send(producer, totalMessages);

        var received = new List<IMessage<string>>();
        for (var i = 0; i < totalMessages / 2; i++)
        {
            received.Add(await Receive(consumer, TimeSpan.FromMilliseconds(500)));
        }

        var trackedAfterReceive = UnAckedMessageCount(consumer);

        //Act
        await Task.Delay(_ackTimeout + TimeSpan.FromSeconds(2), _cts.Token);

        //Assert
        trackedAfterReceive.ShouldBe(5);
        UnAckedMessageCount(consumer).ShouldBe(0);
        var redelivered = new HashSet<string>();
        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token) is { } message)
        {
            redelivered.Add(message.Value());
        }

        redelivered.Count.ShouldBe(totalMessages);
    }

    // Upstream: UnAcknowledgedMessagesTimeoutTest#testExclusiveCumulativeAckedNormalTopic (ackTimeoutRedeliveryBackoff = false)
    [Fact]
    public async Task AcknowledgeCumulative_GivenAckTimeout_ShouldUntrackAllAndNotRedeliver()
    {
        //Arrange
        const int totalMessages = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName, SubscriptionType.Exclusive, 7);
        await using var producer = CreateProducer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await Send(producer, totalMessages);

        IMessage<string>? last = null;
        for (var i = 0; i < totalMessages; i++)
        {
            last = await Receive(consumer, TimeSpan.FromMilliseconds(500));
        }

        var trackedAfterReceive = UnAckedMessageCount(consumer);

        //Act
        await consumer.AcknowledgeCumulative(last!, _cts.Token);

        //Assert
        trackedAfterReceive.ShouldBe(totalMessages);
        UnAckedMessageCount(consumer).ShouldBe(0);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(4), _cts.Token)).ShouldBeNull();
    }

    // Upstream: UnAcknowledgedMessagesTimeoutTest#testCheckUnAcknowledgedMessageTimer
    [Fact]
    public async Task Acknowledge_GivenAllMessagesAcknowledged_ShouldLeaveTrackerEmptyAfterTimeout()
    {
        //Arrange
        const int totalMessages = 3;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName, SubscriptionType.Shared, 7);
        await using var producer = CreateProducer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await Send(producer, totalMessages);

        var received = new List<IMessage<string>>();
        for (var i = 0; i < totalMessages; i++)
        {
            received.Add(await Receive(consumer, TimeSpan.FromMilliseconds(500)));
        }

        var trackedAfterReceive = UnAckedMessageCount(consumer);

        //Act
        foreach (var message in received)
        {
            await consumer.Acknowledge(message, _cts.Token);
        }

        var trackedAfterAcknowledge = UnAckedMessageCount(consumer);
        await Task.Delay(TimeSpan.FromMilliseconds(2200), _cts.Token);

        //Assert
        trackedAfterReceive.ShouldBe(totalMessages);
        trackedAfterAcknowledge.ShouldBe(0);
        UnAckedMessageCount(consumer).ShouldBe(0);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token)).ShouldBeNull();
    }

    // Upstream: PerMessageUnAcknowledgedRedeliveryTest#testSharedAckedNormalTopic
    [Fact]
    public async Task Receive_GivenSharedSubscriptionAndPartiallyAcknowledgedMessages_ShouldRedeliverOnlyExpired()
    {
        //Arrange
        const int totalMessages = 15;
        const int numberOfMessagesInPhase = totalMessages / 3;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName, SubscriptionType.Shared, 50);
        await using var producer = CreateProducer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await Send(producer, totalMessages);

        var firstPhase = new List<IMessage<string>>();
        for (var i = 0; i < numberOfMessagesInPhase; i++)
        {
            firstPhase.Add(await Receive(consumer, TimeSpan.FromMilliseconds(100)));
        }

        await Task.Delay(_ackTimeout, _cts.Token);

        var secondPhase = new List<IMessage<string>>();
        for (var i = 0; i < numberOfMessagesInPhase; i++)
        {
            secondPhase.Add(await Receive(consumer, TimeSpan.FromMilliseconds(100)));
        }

        //Act
        // The messages of the second phase are acknowledged, so only the messages of the first phase and the third phase are left
        foreach (var message in secondPhase)
        {
            await consumer.Acknowledge(message, _cts.Token);
        }

        var thirdPhase = new List<IMessage<string>>();
        for (var i = 0; i < numberOfMessagesInPhase; i++)
        {
            thirdPhase.Add(await Receive(consumer, TimeSpan.FromMilliseconds(100)));
        }

        await Task.Delay(_ackTimeout + TimeSpan.FromSeconds(1), _cts.Token);

        //Assert
        // The first phase expired and is redelivered, together with the third phase that expired too
        var redelivered = new List<IMessage<string>>();
        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token) is { } message)
        {
            redelivered.Add(message);
        }

        var expectedValues = firstPhase.Concat(thirdPhase).Select(message => message.Value()).ToHashSet();
        redelivered.Select(message => message.Value()).ToHashSet().ShouldBe(expectedValues, true);
        redelivered.ShouldAllBe(message => message.RedeliveryCount >= 1);
    }

    // Upstream: PerMessageUnAcknowledgedRedeliveryTest#testUnAckedMessageTrackerSize
    [Fact]
    public async Task Receive_GivenAckTimeoutDisabled_ShouldNotTrackMessages()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var producer = CreateProducer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await Send(producer, 5);

        //Act
        for (var i = 0; i < 5; i++)
        {
            _ = await Receive(consumer, TimeSpan.FromSeconds(5));
        }

        //Assert
        UnAckedMessageCount(consumer).ShouldBe(0);
    }

    // Upstream: PerMessageUnAcknowledgedRedeliveryTest#testExclusiveAckedNormalTopic
    // Upstream: PerMessageUnAcknowledgedRedeliveryTest#testFailoverAckedNormalTopic
    [Theory]
    [InlineData(SubscriptionType.Exclusive)]
    [InlineData(SubscriptionType.Failover)]
    public async Task Receive_GivenSubscriptionWithSingleActiveConsumerAndPartiallyAcknowledgedMessages_ShouldRedeliverAllUnacknowledged(SubscriptionType subscriptionType)
    {
        //Arrange
        const int totalMessages = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName, subscriptionType, 50);
        await using var producer = CreateProducer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await Send(producer, totalMessages);

        var received = new List<IMessage<string>>();
        for (var i = 0; i < totalMessages; i++)
        {
            received.Add(await Receive(consumer, TimeSpan.FromMilliseconds(500)));
        }

        for (var i = 0; i < totalMessages; i += 2)
        {
            await consumer.Acknowledge(received[i], _cts.Token);
        }

        //Act
        await Task.Delay(_ackTimeout + TimeSpan.FromSeconds(2), _cts.Token);
        var trackedAfterExpiry = UnAckedMessageCount(consumer);

        //Assert
        // The expiry of the unacknowledged messages redelivers all the messages that are not acknowledged
        var redelivered = new List<string>();
        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token) is { } message)
        {
            redelivered.Add(message.Value());
        }

        trackedAfterExpiry.ShouldBe(0);
        redelivered.OrderBy(value => value).ShouldBe(received.Where((_, index) => index % 2 == 1).Select(message => message.Value()).OrderBy(value => value));
    }

    // Upstream: ConsumerRedeliveryTest#testMessageRedeliveryWhenTimeoutInListener
    // The message listener is a receive loop that is slower than the ack timeout for the first message.
    [Fact]
    public async Task Receive_GivenSlowProcessingBeyondAckTimeout_ShouldRedeliverOnlyThatMessage()
    {
        //Arrange
        const int totalMessages = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .AckTimeout(TimeSpan.FromSeconds(1))
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var producer = CreateProducer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < totalMessages; i++)
        {
            await producer.Send($"Hello-{i}", _cts.Token);
        }

        //Act
        var received = new List<IMessage<string>>();
        var slowMessageHandled = false;
        while (received.Count < totalMessages + 1)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
            message.ShouldNotBeNull();
            received.Add(message);

            if (message.Value() == "Hello-0" && !slowMessageHandled)
            {
                slowMessageHandled = true;
                await Task.Delay(TimeSpan.FromSeconds(3), _cts.Token);
            }

            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        var hello0 = received.Where(message => message.Value() == "Hello-0").ToList();
        hello0.Count.ShouldBe(2);
        hello0.Select(message => message.RedeliveryCount).Order().ShouldBe([0U, 1U]);
        received.Count(message => message.Value() != "Hello-0").ShouldBe(totalMessages - 1);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token)).ShouldBeNull();
    }

    // Upstream: PerMessageUnAcknowledgedRedeliveryTest#testSharedAckedPartitionedTopic
    [Fact]
    public async Task Receive_GivenSharedPartitionedTopicAndPartiallyAcknowledgedMessages_ShouldRedeliverOnlyExpired()
    {
        //Arrange
        const int totalMessages = 15;
        var topicName = await _fixture.CreatePartitionedTopic(3, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName, SubscriptionType.Shared, 50);
        await using var producer = CreateProducer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await Send(producer, totalMessages);

        var received = new List<IMessage<string>>();
        for (var i = 0; i < totalMessages; i++)
        {
            received.Add(await Receive(consumer, TimeSpan.FromMilliseconds(500)));
        }

        var trackedAfterReceive = UnAckedMessageCount(consumer);

        //Act
        var notAcknowledged = new List<string>();
        for (var i = 0; i < totalMessages; i++)
        {
            if (i % 3 == 0)
                notAcknowledged.Add(received[i].Value());
            else
                await consumer.Acknowledge(received[i], _cts.Token);
        }

        var trackedAfterAcknowledge = UnAckedMessageCount(consumer);
        await Task.Delay(_ackTimeout + TimeSpan.FromSeconds(1), _cts.Token);

        //Assert
        var redelivered = new List<string>();
        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token) is { } message)
        {
            redelivered.Add(message.Value());
        }

        trackedAfterReceive.ShouldBe(totalMessages);
        trackedAfterAcknowledge.ShouldBe(notAcknowledged.Count);
        redelivered.ShouldBe(notAcknowledged, true);
        UnAckedMessageCount(consumer).ShouldBe(notAcknowledged.Count);
    }

    // Upstream: UnAcknowledgedMessagesTimeoutTest#testSingleMessageBatch (the first half, the second half uses the redelivery backoff that is not supported)
    // The producer batches the message with a publish delay of 10 seconds, so the message is only sent because of the flush. The entry of the single message batch is tracked after the message
    // is received, and it is not tracked after the message is acknowledged.
    [Fact]
    public async Task Acknowledge_GivenSingleMessageBatch_ShouldUntrackEntry()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.String)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(10))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await using var consumer = client.NewConsumer(Schema.String)
            .AckTimeout(TimeSpan.FromHours(1))
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("subscription")
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        // Force the creation of a batch with a single message
        var receipt = await SendAsync(producer, "hello");
        await producer.Flush(_cts.Token);
        await receipt.WaitAsync(TimeSpan.FromSeconds(5), _cts.Token);

        //Act
        var message = await Receive(consumer, TimeSpan.FromSeconds(5));
        var trackedAfterReceive = UnAckedMessageCount(consumer);
        await consumer.Acknowledge(message, _cts.Token);

        //Assert
        trackedAfterReceive.ShouldBe(1);
        UnAckedMessageCount(consumer).ShouldBe(0);
    }

    // Upstream: ConsumerRedeliveryTest#testUnAckMessageRedeliveryWithReceiveAsync
    // Java receiveAsync is ten Receive calls that are waiting when the messages are sent. The producer batches the 10 messages in 2 batches of 5 messages.
    // The first 10 messages are not acknowledged, so the ack timeout of 3 seconds redelivers them, and the 10 messages that are received next are acknowledged.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Receive_GivenUnacknowledgedBatchedMessages_ShouldRedeliverAfterAckTimeout(bool batchIndexAckEnabled)
    {
        //Arrange
        const int numberOfMessages = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .AckReceiptEnabled(batchIndexAckEnabled)
            .AckTimeout(TimeSpan.FromSeconds(3))
            .BatchIndexAckEnabled(batchIndexAckEnabled)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("s1")
            .Topic(topicName)
            .Create();
        await using var producer = client.NewProducer(Schema.String)
            .BatchingMaxMessages(5)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(1))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        var pendingReceives = Enumerable.Range(0, numberOfMessages).Select(_ => consumer.Receive(_cts.Token).AsTask()).ToList();
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            receipts.Add(await SendAsync(producer, $"my-message-{i}"));
        }

        //Act
        // The messages are not acknowledged, the ack timeout redelivers them
        var firstDelivery = await Task.WhenAll(pendingReceives).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        var redelivered = new List<IMessage<string>>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await Receive(consumer, TimeSpan.FromSeconds(10));
            redelivered.Add(message);
            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        firstDelivery.Length.ShouldBe(numberOfMessages);
        firstDelivery.Select(message => message.Value()).ToHashSet().ShouldBe(Enumerable.Range(0, numberOfMessages).Select(i => $"my-message-{i}").ToHashSet(), true);
        redelivered.Count.ShouldBe(numberOfMessages);
        redelivered.Select(message => message.Value()).ToHashSet().ShouldBe(firstDelivery.Select(message => message.Value()).ToHashSet(), true);
    }

    private static int UnAckedMessageCount(IConsumer<string> consumer)
        => ((Consumer<string>) consumer).UnAckedMessageCount;

    private async Task Send(IProducer<string> producer, int count)
    {
        for (var i = 0; i < count; i++)
        {
            _ = await producer.Send($"my-message-{i}", _cts.Token);
        }
    }

    // The Java producer.sendAsync: the message is queued in the order of the calls, and the task completes when the broker has stored the message
    private async ValueTask<Task<MessageId>> SendAsync(IProducer<string> producer, string value)
    {
        var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);

        await producer.SendChannel.Send(new MessageMetadata(), value, messageId =>
        {
            _ = receipt.TrySetResult(messageId);
            return ValueTask.CompletedTask;
        }, _cts.Token);

        return receipt.Task;
    }

    private async Task<IMessage<string>> Receive(IConsumer<string> consumer, TimeSpan timeout)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, timeout + TimeSpan.FromSeconds(4), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    private IConsumer<string> CreateConsumer(IPulsarClient client, string topicName, SubscriptionType subscriptionType, uint prefetchCount)
        => client.NewConsumer(Schema.String)
            .AckTimeout(_ackTimeout)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(prefetchCount)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .SubscriptionType(subscriptionType)
            .Topic(topicName)
            .Create();

    private IProducer<string> CreateProducer(IPulsarClient client, string topicName)
        => client.NewProducer(Schema.String)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();
}
