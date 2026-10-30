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
    // The last message is not acknowledged, so it is the only one that is tracked. The blocking receive waits for its redelivery, and acknowledging it empties the tracker.
    [Fact]
    public async Task Receive_GivenLastMessageNotAcknowledged_ShouldTrackOnlyItAndLeaveTrackerEmptyAfterItIsRedeliveredAndAcknowledged()
    {
        //Arrange
        const int totalMessages = 3;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName, SubscriptionType.Shared, 7);
        await using var producer = CreateProducer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await Send(producer, totalMessages);

        //Act
        for (var i = 0; i < totalMessages; i++)
        {
            var message = await Receive(consumer, TimeSpan.FromSeconds(5));

            if (i != totalMessages - 1)
                await consumer.Acknowledge(message, _cts.Token);
        }

        var trackedAfterReceive = UnAckedMessageCount(consumer);

        // The ack timeout redelivers the message that was not acknowledged
        var redelivered = await consumer.Receive(_cts.Token);
        await consumer.Acknowledge(redelivered, _cts.Token);
        var trackedAfterAcknowledge = UnAckedMessageCount(consumer);
        await Task.Delay(TimeSpan.FromMilliseconds(2200), _cts.Token);

        //Assert
        trackedAfterReceive.ShouldBe(1);
        redelivered.Value().ShouldBe($"my-message-{totalMessages - 1}");
        trackedAfterAcknowledge.ShouldBe(0);
        UnAckedMessageCount(consumer).ShouldBe(0);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token)).ShouldBeNull();
    }

    // Upstream: PerMessageUnAcknowledgedRedeliveryTest#testSharedAckedNormalTopic
    // The messages of the phases have the same values, like upstream. A Shared subscription redelivers only the messages that expired: the 5 of the first phase. The 5 of the third phase
    // are tracked again from when they were received, so they have not expired yet when the redelivered messages are received.
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

        //Act
        // The messages are received and not acknowledged
        await Send(producer, numberOfMessagesInPhase);
        await ReceiveAll(consumer, TimeSpan.FromMilliseconds(100), false, true);
        var trackedAfterFirstPhase = UnAckedMessageCount(consumer);

        // The messages are received and acknowledged
        await Send(producer, numberOfMessagesInPhase);
        var received = await ReceiveAll(consumer, TimeSpan.FromMilliseconds(100), true, true);
        var trackedAfterSecondPhase = UnAckedMessageCount(consumer);

        // Simulate the ack timeout
        await Task.Delay(_ackTimeout, _cts.Token);

        // The messages are received and not acknowledged
        await Send(producer, numberOfMessagesInPhase);
        await ReceiveAll(consumer, TimeSpan.FromMilliseconds(100), false, true);
        var trackedAfterThirdPhase = UnAckedMessageCount(consumer);

        await Task.Delay(_ackTimeout, _cts.Token);

        // The redelivered messages are received and acknowledged
        var redelivered = await ReceiveAll(consumer, TimeSpan.FromMilliseconds(100), true, true);
        var trackedAfterRedelivery = UnAckedMessageCount(consumer);

        //Assert
        trackedAfterFirstPhase.ShouldBe(5);
        trackedAfterSecondPhase.ShouldBe(5);
        received.Count.ShouldBe(5);
        trackedAfterThirdPhase.ShouldBe(10);
        redelivered.Count.ShouldBe(5);
        redelivered.ShouldAllBe(message => message.RedeliveryCount >= 1);
        trackedAfterRedelivery.ShouldBe(5);
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
    // The expiry of the 5 messages of the first phase redelivers all the messages that are not acknowledged, which includes the 5 messages of the third phase, so 10 are redelivered.
    // The messages of the phases have the same values, like upstream.
    [Theory]
    [InlineData(SubscriptionType.Exclusive)]
    [InlineData(SubscriptionType.Failover)]
    public async Task Receive_GivenSubscriptionWithSingleActiveConsumerAndPartiallyAcknowledgedMessages_ShouldRedeliverAllUnacknowledged(SubscriptionType subscriptionType)
    {
        //Arrange
        const int totalMessages = 15;
        const int numberOfMessagesInPhase = totalMessages / 3;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName, subscriptionType, 50);
        await using var producer = CreateProducer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        //Act
        // The messages are received and not acknowledged
        await Send(producer, numberOfMessagesInPhase);
        await ReceiveAll(consumer, TimeSpan.FromMilliseconds(100), false, true);
        var trackedAfterFirstPhase = UnAckedMessageCount(consumer);

        // The messages are received and acknowledged
        await Send(producer, numberOfMessagesInPhase);
        var received = await ReceiveAll(consumer, TimeSpan.FromMilliseconds(100), true, true);
        var trackedAfterSecondPhase = UnAckedMessageCount(consumer);

        // Simulate the ack timeout
        await Task.Delay(_ackTimeout, _cts.Token);

        // The messages are received and not acknowledged
        await Send(producer, numberOfMessagesInPhase);
        await ReceiveAll(consumer, TimeSpan.FromMilliseconds(100), false, true);
        var trackedAfterThirdPhase = UnAckedMessageCount(consumer);

        await Task.Delay(_ackTimeout, _cts.Token);

        // The redelivered messages are received and acknowledged
        var redelivered = await ReceiveAll(consumer, TimeSpan.FromMilliseconds(100), true, true);
        var trackedAfterRedelivery = UnAckedMessageCount(consumer);

        //Assert
        trackedAfterFirstPhase.ShouldBe(5);
        trackedAfterSecondPhase.ShouldBe(5);
        received.Count.ShouldBe(5);
        trackedAfterThirdPhase.ShouldBe(10);
        redelivered.Count.ShouldBe(10);
        trackedAfterRedelivery.ShouldBe(0);
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

    // Upstream: UnAcknowledgedMessagesTimeoutTest#testSharedSingleAckedPartitionedTopic (the row {false}; the row {true} uses ackTimeoutRedeliveryBackoff, which DotPulsar does not support)
    // Consumer-1 receives its messages and does not acknowledge them, so they are redelivered to one of the consumers after the ack timeout. Consumer-2 acknowledges everything it receives,
    // and what is redelivered to it is not acknowledged until the last round.
    [Fact]
    public async Task Receive_GivenSharedPartitionedTopicAndOneConsumerNotAcknowledging_ShouldRedeliverAfterAckTimeout()
    {
        //Arrange
        const int totalMessages = 20;
        const string subscriptionName = "my-shared-subscription";
        var topicName = await _fixture.CreatePartitionedTopic(3, _cts.Token);
        await using var client = CreateClient();
        await using var consumer1 = CreateConsumer(client, topicName, SubscriptionType.Shared, 100, subscriptionName, "Consumer-1");
        await using var consumer2 = CreateConsumer(client, topicName, SubscriptionType.Shared, 100, subscriptionName, "Consumer-2");
        await using var producer = CreateProducer(client, topicName);
        await consumer1.StateChangedTo(ConsumerState.Active, _cts.Token);
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);
        await Send(producer, totalMessages);

        //Act
        var messageCount1 = (await ReceiveAll(consumer1, TimeSpan.FromSeconds(1), false, false)).Count;
        var messageCount2 = (await ReceiveAll(consumer2, TimeSpan.FromSeconds(1), true, false)).Count;
        var ackCount2 = messageCount2;
        var totalAfterFirstRound = messageCount1 + messageCount2;

        // The messages of Consumer-1 are redelivered after the ack timeout
        await Task.Delay(TimeSpan.FromMilliseconds(_ackTimeout.TotalMilliseconds * 1.1), _cts.Token);
        messageCount1 = (await ReceiveAll(consumer1, TimeSpan.FromSeconds(1), true, false)).Count;
        messageCount2 += (await ReceiveAll(consumer2, TimeSpan.FromSeconds(1), false, false)).Count;
        var ackCount1 = messageCount1;
        var totalAfterSecondRound = messageCount1 + messageCount2;
        var acknowledgedAfterSecondRound = ackCount1 + messageCount2;

        // The messages that were redelivered to Consumer-2 and not acknowledged are redelivered again after the ack timeout
        await Task.Delay(_ackTimeout * 2, _cts.Token);
        ackCount1 += (await ReceiveAll(consumer1, TimeSpan.FromSeconds(1), true, false)).Count;
        ackCount2 += (await ReceiveAll(consumer2, TimeSpan.FromSeconds(1), true, false)).Count;

        //Assert
        totalAfterFirstRound.ShouldBe(totalMessages);
        totalAfterSecondRound.ShouldBe(totalMessages);
        acknowledgedAfterSecondRound.ShouldBe(totalMessages);
        (ackCount1 + ackCount2).ShouldBe(totalMessages);
    }

    // Upstream: UnAcknowledgedMessagesTimeoutTest#testFailoverSingleAckedPartitionedTopic (the row {false}; the row {true} uses ackTimeoutRedeliveryBackoff, which DotPulsar does not support)
    // Despite its name, the upstream test uses a Shared subscription, and so does this test.
    [Fact]
    public async Task Receive_GivenTwoSharedConsumersOnPartitionedTopicOneNotAcknowledging_ShouldRedeliverOnlyUnacknowledged()
    {
        //Arrange
        const int totalMessages = 10;
        const string subscriptionName = "my-failover-subscription";
        var topicName = await _fixture.CreatePartitionedTopic(3, _cts.Token);
        await using var client = CreateClient();
        await using var consumer1 = CreateConsumer(client, topicName, SubscriptionType.Shared, 7, subscriptionName, "Consumer-1");
        await using var consumer2 = CreateConsumer(client, topicName, SubscriptionType.Shared, 7, subscriptionName, "Consumer-2");
        await using var producer = CreateProducer(client, topicName);
        await consumer1.StateChangedTo(ConsumerState.Active, _cts.Token);
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);
        await Send(producer, totalMessages);

        //Act
        var messagesReceived = (await ReceiveAll(consumer1, TimeSpan.FromMilliseconds(500), false, false)).Count;
        var ackCount = (await ReceiveAll(consumer2, TimeSpan.FromMilliseconds(500), true, false)).Count;
        messagesReceived += ackCount;
        var totalAfterFirstRound = messagesReceived;

        // The messages of Consumer-1 are redelivered after the ack timeout
        await Task.Delay(_ackTimeout * 2, _cts.Token);
        messagesReceived = (await ReceiveAll(consumer1, TimeSpan.FromMilliseconds(500), false, false)).Count;
        messagesReceived += (await ReceiveAll(consumer2, TimeSpan.FromMilliseconds(500), false, false)).Count;

        //Assert
        totalAfterFirstRound.ShouldBe(totalMessages);
        (messagesReceived + ackCount).ShouldBe(totalMessages);
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

    // The receive loop of the Java tests: the messages are received until no message arrives within the timeout. The first receive blocks if 'waitForFirst' is true, like consumer.receive().
    private async Task<List<IMessage<string>>> ReceiveAll(IConsumer<string> consumer, TimeSpan timeout, bool acknowledge, bool waitForFirst)
    {
        var messages = new List<IMessage<string>>();
        var message = waitForFirst ? await consumer.Receive(_cts.Token) : await TransactionFixture.ReceiveOrNull(consumer, timeout, _cts.Token);

        while (message is not null)
        {
            messages.Add(message);

            if (acknowledge)
                await consumer.Acknowledge(message, _cts.Token);

            message = await TransactionFixture.ReceiveOrNull(consumer, timeout, _cts.Token);
        }

        return messages;
    }

    private async Task<IMessage<string>> Receive(IConsumer<string> consumer, TimeSpan timeout)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, timeout + TimeSpan.FromSeconds(4), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    private IConsumer<string> CreateConsumer(IPulsarClient client, string topicName, SubscriptionType subscriptionType, uint prefetchCount, string? subscriptionName = null, string? consumerName = null)
    {
        var builder = client.NewConsumer(Schema.String)
            .AckTimeout(_ackTimeout)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(prefetchCount)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscriptionName ?? $"subscription-{Guid.NewGuid():N}")
            .SubscriptionType(subscriptionType)
            .Topic(topicName);

        if (consumerName is not null)
            builder = builder.ConsumerName(consumerName);

        return builder.Create();
    }

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
