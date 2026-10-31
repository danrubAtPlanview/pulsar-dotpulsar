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

namespace DotPulsar.Tests.Prerequisites;

using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using System.Reflection;
using System.Text;

// Upstream: BatchMessageWithBatchIndexLevelTest
// The broker has acknowledgmentAtBatchIndexLevelEnabled=true (the Transaction fixture). The Java tests read the unacknowledged messages and the permits from the dispatcher of the broker, and here
// they are read from the topic stats of the admin API. BatchIndexAckEnabled(true) is the default of the Java consumer, and DotPulsar sends every acknowledgment at once, so the acknowledgment group time
// of the Java consumers does not exist. The Java consumer starts to ask the broker for messages when it subscribes, and the DotPulsar consumer when it receives the first message.
[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class BatchMessageWithBatchIndexLevelTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public BatchMessageWithBatchIndexLevelTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: BatchMessageWithBatchIndexLevelTest#testBatchMessageAck
    // consumer.pause() of the Java test stops the consumer from sending flow commands, so that the broker cannot send the negatively acknowledged batch again until consumer.resume(). The relay holds
    // the flow commands of the consumer, except the first one that makes the broker send the messages.
    [Fact]
    public async Task Acknowledge_GivenTwoBatchesAndNegativeAcknowledge_ShouldCountTheUnackedMessagesOfTheConsumerInTheBroker()
    {
        //Arrange
        const int numberOfMessages = 40;
        const string subscriptionName = "sub-batch-1";
        var topic = TransactionFixture.CreateTopicName("batch-message-ack");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var relay = await FlowHoldingRelay.Start(_fixture.ServiceUrl, _cts.Token);
        await using var client = CreateClient(relay.ServiceUrl);
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .BatchIndexAckEnabled(true)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(numberOfMessages)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromMilliseconds(100))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topic)
            .Create();
        await using var producer = CreateBatchingProducer(client, topic, true);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        relay.HoldFlowsAfter(1);
        var receipts = new List<Task<MessageId>>();

        for (var i = 0; i < numberOfMessages; i++)
        {
            receipts.Add(await SendAsync(producer, Encoding.UTF8.GetBytes($"batch-message-{i}")));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Act
        var receive1 = await Receive(consumer);
        var receive2 = await Receive(consumer);
        await consumer.Acknowledge(receive1, _cts.Token);
        await consumer.Acknowledge(receive2, _cts.Token);
        var unackedAfterTwoAcknowledgments = await WaitForUnackedMessages(topic, subscriptionName, 38);

        var receive3 = await Receive(consumer);
        var receive4 = await Receive(consumer);
        await consumer.Acknowledge(receive3, _cts.Token);
        await consumer.Acknowledge(receive4, _cts.Token);
        var unackedAfterFourAcknowledgments = await WaitForUnackedMessages(topic, subscriptionName, 36);

        // The flow commands are held back from here
        var receive5 = await Receive(consumer);
        await consumer.NegativeAcknowledge(receive5, _cts.Token);
        var unackedAfterNegativeAcknowledgment = await WaitForUnackedMessages(topic, subscriptionName, 20);

        // The flow commands are sent again
        await relay.ReleaseFlows(_cts.Token);
        _ = await Receive(consumer);
        var unackedAfterRedelivery = await WaitForUnackedMessages(topic, subscriptionName, 36);

        //Assert
        unackedAfterTwoAcknowledgments.ShouldBe(38);
        unackedAfterFourAcknowledgments.ShouldBe(36);
        unackedAfterNegativeAcknowledgment.ShouldBe(20);
        unackedAfterRedelivery.ShouldBe(36);
    }

    // Upstream: BatchMessageWithBatchIndexLevelTest#testBatchMessageNAck
    // The Java test reads the number of messages in the queue of the consumer, and the number of messages that can be received stands in for it. The messages are not acknowledged, so
    // the first message that is negatively acknowledged is the only message of its entry that the consumer has taken, and the rest of the entry is taken out of the queue.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NegativeAcknowledge_GivenFirstMessageOfTwoEntriesOfTwentyMessages_ShouldDropTheEntryFromTheQueueAndTheUnackedMessagesOfTheBroker(bool enabledBatchSend)
    {
        //Arrange
        const string subscriptionName = "s1";
        var topic = TransactionFixture.CreateTopicName("batch-message-nack");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var relay = await FlowHoldingRelay.Start(_fixture.ServiceUrl, _cts.Token);
        await using var client = CreateClient(relay.ServiceUrl);
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .BatchIndexAckEnabled(true)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(21)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromMilliseconds(100))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topic)
            .Create();
        await using var producer = CreateBatchingProducer(client, topic, enabledBatchSend);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        relay.HoldFlowsAfter(1);
        var receipts = new List<Task<MessageId>>();

        // Send messages: 20 * 2
        for (var i = 0; i < 40; i++)
        {
            var receipt = await SendAsync(producer, Encoding.UTF8.GetBytes($"batch-message-{i}"));

            if (i == 19 || i == 39)
                _ = await receipt.WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
            else
                receipts.Add(receipt);
        }

        //Act
        // The consumer asks for the messages when it receives the first one
        var receive1 = await Receive(consumer);
        var unackedBeforeNegativeAcknowledgment = await WaitForUnackedMessages(topic, subscriptionName, enabledBatchSend ? 40 : 21);

        // The flow commands are held back from here
        await consumer.NegativeAcknowledge(receive1, _cts.Token);
        var unackedAfterNegativeAcknowledgment = await WaitForUnackedMessages(topic, subscriptionName, 20);
        var inQueue = 0;

        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token) is not null)
        {
            inQueue++;
        }

        //Assert
        unackedBeforeNegativeAcknowledgment.ShouldBe(enabledBatchSend ? 40 : 21);
        unackedAfterNegativeAcknowledgment.ShouldBe(20);
        inQueue.ShouldBe(20);
    }

    // Upstream: BatchMessageWithBatchIndexLevelTest#testBatchMessageMultiNegtiveAck
    // The second part of the Java test receives the messages with the second consumer and acknowledges them with the first consumer, and then checks the first topic again. That is kept.
    [Fact]
    public async Task NegativeAcknowledge_GivenEveryOtherMessageOfBatchNegativelyAcknowledged_ShouldKeepTheNegativelyAcknowledgedMessagesUnacked()
    {
        //Arrange
        const int numberOfMessages = 20;
        const string subscriptionName = "sub-negtive-1";
        const string subscriptionName2 = "sub-negtive-2";
        var topic = TransactionFixture.CreateTopicName("batch-message-multi-negtive-ack");
        var topic2 = TransactionFixture.CreateTopicName("batch-message-multi-negtive-ack2");
        await _fixture.CreateTopic(topic, _cts.Token);
        await _fixture.CreateTopic(topic2, _cts.Token);
        await using var client = CreateClient(_fixture.ServiceUrl);
        await using var consumer = CreateStringConsumer(client, topic, subscriptionName, 10);
        await using var producer = CreateStringBatchingProducer(client, topic);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < numberOfMessages; i++)
        {
            _ = await SendAsync(producer, $"test-{i}");
        }

        await producer.Flush(_cts.Token);

        //Act
        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await Receive(consumer);

            if (i % 2 == 0)
                _ = consumer.Acknowledge(message, _cts.Token).AsTask();
            else
                await consumer.NegativeAcknowledge(message, _cts.Token);
        }

        var unacked = await WaitForUnackedMessages(topic, subscriptionName, 10);

        // Test negtive ack with sleep
        await using var consumer2 = CreateStringConsumer(client, topic2, subscriptionName2, 10);
        await using var producer2 = CreateStringBatchingProducer(client, topic2);
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < numberOfMessages; i++)
        {
            _ = await SendAsync(producer2, $"test-{i}");
        }

        await producer2.Flush(_cts.Token);

        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await Receive(consumer2);

            if (i % 2 == 0)
                _ = consumer.Acknowledge(message, _cts.Token).AsTask();
            else
            {
                await consumer.NegativeAcknowledge(message, _cts.Token);
                await Task.Delay(100, _cts.Token);
            }
        }

        var unacked2 = await WaitForUnackedMessages(topic, subscriptionName, 10);

        //Assert
        unacked.ShouldBe(10);
        unacked2.ShouldBe(10);
    }

    // Upstream: BatchMessageWithBatchIndexLevelTest#testAckMessageWithNotOwnerConsumerUnAckMessageCount
    // The Java consumers ask for messages when they subscribe, and the DotPulsar consumers when they receive the first message, so the first receive of each consumer is started before the messages are sent.
    [Fact]
    public async Task Acknowledge_GivenMessagesOfBatchThatAreAcknowledgedByTheConsumerThatDoesNotOwnThem_ShouldCountTheUnackedMessagesOfTheOwnerCorrectly()
    {
        //Arrange
        const string subscriptionName = "test";
        var topic = TransactionFixture.CreateTopicName("not-owner-consumer-unacked-message-count");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = CreateClient(_fixture.ServiceUrl);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(1))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .Create();
        await using var consumer1 = CreateNotOwnerConsumer(client, topic, subscriptionName, "consumer-1");
        await using var consumer2 = CreateNotOwnerConsumer(client, topic, subscriptionName, "consumer-2");
        await consumer1.StateChangedTo(ConsumerState.Active, _cts.Token);
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);
        var firstReceive1 = consumer1.Receive(_cts.Token).AsTask();
        var firstReceive2 = consumer2.Receive(_cts.Token).AsTask();
        await WaitForAvailablePermits(topic, subscriptionName, 2);

        for (var i = 0; i < 5; i++)
        {
            _ = await SendAsync(producer, Encoding.UTF8.GetBytes($"Hello Pulsar - {i}"));
        }

        //Act
        // The Java consumer-1 receives the 5 messages of the batch. The broker sends the batch to the consumer that is next in its rotation, and the DotPulsar consumers are not connected in a fixed order
        // like the Java consumers, so the consumer that receives the batch first plays consumer-1, and the other one plays consumer-2.
        var firstDelivery = await Task.WhenAny(firstReceive1, firstReceive2).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        var (consumerOne, consumerTwo, firstReceiveOfTwo) = firstDelivery == firstReceive1 ? (consumer1, consumer2, firstReceive2) : (consumer2, consumer1, firstReceive1);
        var ids = new List<MessageId> { (await firstDelivery).MessageId };
        for (var i = 1; i < 5; i++)
        {
            ids.Add((await Receive(consumerOne)).MessageId);
        }

        // consumer-1 redelivers the batch
        await consumerOne.NegativeAcknowledge(ids[0], _cts.Token);

        // consumer-2 receives the messages that consumer-1 redelivered
        _ = await firstReceiveOfTwo.WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        for (var i = 1; i < 5; i++)
        {
            _ = await Receive(consumerTwo);
        }

        // consumer-1 acknowledges two messages of the batch
        await consumerOne.Acknowledge(ids[1], _cts.Token);
        await consumerOne.Acknowledge(ids[2], _cts.Token);

        // consumer-2 redelivers the rest of the messages
        await consumerTwo.NegativeAcknowledge(ids[1], _cts.Token);

        // The close of consumer-1 redelivers its messages to consumer-2
        await consumerOne.DisposeAsync();

        // consumer-2 receives the 3 messages that are left
        var rest = new List<IMessage<byte[]>>();
        for (var i = 0; i < 3; i++)
        {
            var message = await Receive(consumerTwo);
            await consumerTwo.Acknowledge(message, _cts.Token);
            rest.Add(message);
        }

        // consumer-2 can not receive any message, because all the messages of the batch are acknowledged
        var more = await TransactionFixture.ReceiveOrNull(consumerTwo, TimeSpan.FromSeconds(1), _cts.Token);

        // The number of unacknowledged messages of consumer-2 is 0
        var unacked = await WaitForUnackedMessages(topic, subscriptionName, 0);

        //Assert
        rest.Count.ShouldBe(3);
        more.ShouldBeNull();
        unacked.ShouldBe(0);
    }

    // Upstream: BatchMessageWithBatchIndexLevelTest#testNegativeAckAndLongAckDelayWillNotLeadRepeatConsume
    // The Java consumer waits an hour before it sends the acknowledgments, which makes the broker redeliver the whole batch when a message is negatively acknowledged. DotPulsar sends the acknowledgments
    // at once, so only the message that was negatively acknowledged is redelivered, and the number of messages that are received is the same.
    [Fact]
    public async Task NegativeAcknowledge_GivenMessageOfBatchNegativelyAcknowledged_ShouldNotConsumeTheOtherMessagesAgain()
    {
        //Arrange
        const int redeliveryDelaySeconds = 2;
        const int numberOfMessages = 10;
        const string subscriptionName = "s1";
        var topic = TransactionFixture.CreateTopicName("batch-message-nack-long-ack-delay");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = CreateClient(_fixture.ServiceUrl);
        await using var producer = client.NewProducer(Schema.String)
            .BatchingMaxMessages(1000)
            .BatchingMaxPublishDelay(TimeSpan.FromHours(1))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .Create();
        await using var consumer = client.NewConsumer(Schema.String)
            .BatchIndexAckEnabled(true)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromSeconds(redeliveryDelaySeconds))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topic)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        // Send 10 messages in batch
        var messagesSent = new List<string>();
        var sendTasks = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            sendTasks.Add(await SendAsync(producer, i.ToString()));
            messagesSent.Add(i.ToString());
        }

        await producer.Flush(_cts.Token);
        await Task.WhenAll(sendTasks).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Act
        // Receive messages. NegativeAck the message with the batch index 1 once.
        var messagesReceived = new List<string>();
        var index1HasBeenNegativeAcked = false;

        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(2), _cts.Token) is { } message)
        {
            if (index1HasBeenNegativeAcked)
            {
                messagesReceived.Add(message.Value());
                await consumer.Acknowledge(message, _cts.Token);
                continue;
            }

            if (message.MessageId.BatchIndex == 1)
            {
                await consumer.NegativeAcknowledge(message, _cts.Token);
                index1HasBeenNegativeAcked = true;
                continue;
            }

            messagesReceived.Add(message.Value());
            await consumer.Acknowledge(message, _cts.Token);
        }

        // Receive the negatively acknowledged messages, which means to wait until the negative acknowledgment is done
        var tripleRedeliveryDelaySeconds = redeliveryDelaySeconds * 3;
        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(tripleRedeliveryDelaySeconds), _cts.Token) is { } message)
        {
            messagesReceived.Add(message.Value());
            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        messagesReceived.Count.ShouldBe(messagesSent.Count);
        messagesReceived.OrderBy(value => value).ShouldBe(messagesSent.OrderBy(value => value));
    }

    // Upstream: BatchMessageWithBatchIndexLevelTest#testMixIndexAndNonIndexUnAckMessageCount
    // The topic has two entries, the first with the message 1 and the second with the messages 2 and 3. The Java consumer sends the acknowledgment of the first message as an acknowledgment of the entry,
    // and those of the second batch as acknowledgments of a batch index, in one group. The acknowledgment of the first message is not awaited, like acknowledgeAsync.
    [Fact]
    public async Task Acknowledge_GivenMessagesOfOneEntryAndOfABatch_ShouldLeaveNoUnackedMessages()
    {
        //Arrange
        const string subscriptionName = "sub";
        var topic = TransactionFixture.CreateTopicName("mix-index-and-non-index-unacked-message-count");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = CreateClient(_fixture.ServiceUrl);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(100))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .Create();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .AckReceiptEnabled(true)
            .BatchIndexAckEnabled(true)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(subscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topic)
            .Create();

        // Send two batch messages: [(1), (2,3)]
        _ = await producer.Send("1"u8.ToArray(), _cts.Token);
        var second = await SendAsync(producer, "2"u8.ToArray());
        _ = await producer.Send("3"u8.ToArray(), _cts.Token);
        await second.WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        var message1 = await Receive(consumer);
        var message2 = await Receive(consumer);
        var message3 = await Receive(consumer);

        //Act
        var firstAcknowledgment = consumer.Acknowledge(message1, _cts.Token).AsTask();
        await consumer.Acknowledge(message2, _cts.Token); // The acknowledgment of the entry of the first message, and of the batch index of the second message
        await consumer.Acknowledge(message3, _cts.Token); // The acknowledgment of the batch index of the third message
        await firstAcknowledgment;
        var unacked = await _fixture.GetUnackedMessages(topic, subscriptionName, _cts.Token);

        //Assert
        message1.MessageId.BatchIndex.ShouldBe(-1);
        message2.MessageId.BatchIndex.ShouldBe(0);
        message3.MessageId.BatchIndex.ShouldBe(1);
        unacked.ShouldBe(0);
    }

    // Upstream: BatchMessageWithBatchIndexLevelTest#testPermitsIfHalfAckBatchMessage
    // The Java consumer counts the permits that it did not give the broker yet in getAvailablePermits(). The DotPulsar consumer gives them when half of the prefetch count has been received, and
    // the messages that it has received since then are the permits that it did not give yet.
    [Fact]
    public async Task Receive_GivenBatchThatIsHalfAcknowledgedAndRedelivered_ShouldNotLoseOrCreatePermits()
    {
        //Arrange
        const int numberOfMessages = 100;
        const int receiverQueueSize = 1000;
        const int ackedMessagesCountInTheFistStep = 2;
        const string subscriptionName = "s1";
        var topic = TransactionFixture.CreateTopicName("permits-if-half-ack-batch-message");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = CreateClient(_fixture.ServiceUrl);
        await using var producer = client.NewProducer(Schema.String)
            .BatchingMaxPublishDelay(TimeSpan.FromHours(1))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .Create();

        // Send 100 messages
        var sendTasks = new List<Task<MessageId>>();
        for (var i = 1; i <= numberOfMessages; i++)
        {
            sendTasks.Add(await SendAsync(producer, i.ToString()));
        }

        await producer.Flush(_cts.Token);
        await Task.WhenAll(sendTasks).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        // Acknowledge 2 messages, and trigger a redelivery
        var consumer1 = CreatePermitsConsumer(client, topic, subscriptionName, receiverQueueSize);
        for (var i = 0; i < ackedMessagesCountInTheFistStep; i++)
        {
            var message = await Receive(consumer1);
            await consumer1.Acknowledge(message, _cts.Token);
        }

        await consumer1.DisposeAsync();

        //Act
        // Receive the 98 messages that are left, and acknowledge them
        await using var consumer2 = CreatePermitsConsumer(client, topic, subscriptionName, receiverQueueSize);
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);
        var messages = new List<IMessage<string>>();
        var nextMessageValue = ackedMessagesCountInTheFistStep + 1;
        var values = new List<string>();

        while (await TransactionFixture.ReceiveOrNull(consumer2, TimeSpan.FromSeconds(2), _cts.Token) is { } message)
        {
            values.Add(message.Value());
            messages.Add(message);
            nextMessageValue++;
        }

        await consumer2.Acknowledge(messages, _cts.Token);

        // After the messages were taken out of the queue, the permits that are in the memory of the client plus the permits in the broker are the size of the queue
        var permitsInTotal = 0L;
        for (var attempt = 0; attempt < 20; attempt++)
        {
            var permitsInClientMemory = GetUnsentPermits(consumer2);
            var permitsInBroker = await GetAvailablePermits(topic, subscriptionName);
            permitsInTotal = permitsInClientMemory + permitsInBroker;

            if (permitsInTotal == receiverQueueSize)
                break;

            await Task.Delay(TimeSpan.FromMilliseconds(500), _cts.Token);
        }

        //Assert
        values.ShouldBe(Enumerable.Range(ackedMessagesCountInTheFistStep + 1, numberOfMessages - ackedMessagesCountInTheFistStep).Select(i => i.ToString()));
        messages.Count.ShouldBe(98);
        permitsInTotal.ShouldBe(receiverQueueSize);
    }

    private IPulsarClient CreateClient(Uri serviceUrl)
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(serviceUrl)
        .Build();

    private IProducer<byte[]> CreateBatchingProducer(IPulsarClient client, string topic, bool enableBatching)
        => client.NewProducer(Schema.ByteArray)
        .BatchingMaxMessages(20)
        .BatchingMaxPublishDelay(TimeSpan.FromHours(1))
        .EnableBatching(enableBatching)
        .StateChangedHandler(_testOutputHelper.Log)
        .Topic(topic)
        .Create();

    private IProducer<string> CreateStringBatchingProducer(IPulsarClient client, string topic)
        => client.NewProducer(Schema.String)
        .BatchingMaxMessages(20)
        .BatchingMaxPublishDelay(TimeSpan.FromHours(1))
        .EnableBatching(true)
        .StateChangedHandler(_testOutputHelper.Log)
        .Topic(topic)
        .Create();

    private IConsumer<string> CreateStringConsumer(IPulsarClient client, string topic, string subscriptionName, uint prefetchCount)
        => client.NewConsumer(Schema.String)
        .BatchIndexAckEnabled(true)
        .InitialPosition(SubscriptionInitialPosition.Earliest)
        .MessagePrefetchCount(prefetchCount)
        .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromMilliseconds(100))
        .StateChangedHandler(_testOutputHelper.Log)
        .SubscriptionName(subscriptionName)
        .SubscriptionType(SubscriptionType.Shared)
        .Topic(topic)
        .Create();

    private IConsumer<byte[]> CreateNotOwnerConsumer(IPulsarClient client, string topic, string subscriptionName, string consumerName)
        => client.NewConsumer(Schema.ByteArray)
        .AckReceiptEnabled(true)
        .BatchIndexAckEnabled(true)
        .ConsumerName(consumerName)
        .InitialPosition(SubscriptionInitialPosition.Earliest)
        .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromSeconds(1))
        .StateChangedHandler(_testOutputHelper.Log)
        .SubscriptionName(subscriptionName)
        .SubscriptionType(SubscriptionType.Shared)
        .Topic(topic)
        .Create();

    private IConsumer<string> CreatePermitsConsumer(IPulsarClient client, string topic, string subscriptionName, uint receiverQueueSize)
        => client.NewConsumer(Schema.String)
        .AckReceiptEnabled(true)
        .BatchIndexAckEnabled(true)
        .InitialPosition(SubscriptionInitialPosition.Earliest)
        .MessagePrefetchCount(receiverQueueSize)
        .StateChangedHandler(_testOutputHelper.Log)
        .SubscriptionName(subscriptionName)
        .SubscriptionType(SubscriptionType.Shared)
        .Topic(topic)
        .Create();

    // The Java producer.sendAsync: the message is queued in the order of the calls, and the task completes when the broker has stored the message
    private async ValueTask<Task<MessageId>> SendAsync<TMessage>(IProducer<TMessage> producer, TMessage value)
    {
        var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);

        await producer.SendChannel.Send(new MessageMetadata(), value, messageId =>
        {
            _ = receipt.TrySetResult(messageId);
            return ValueTask.CompletedTask;
        }, _cts.Token);

        return receipt.Task;
    }

    private async Task<IMessage<TMessage>> Receive<TMessage>(IConsumer<TMessage> consumer)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(15), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    // Awaitility.await().untilAsserted(): the last value is returned when the expected value did not show up within 10 seconds
    private async Task<long> WaitForUnackedMessages(string topic, string subscriptionName, long expected)
    {
        var unacked = long.MaxValue;

        for (var attempt = 0; attempt < 20 && unacked != expected; attempt++)
        {
            unacked = await _fixture.GetUnackedMessages(topic, subscriptionName, _cts.Token);

            if (unacked != expected)
                await Task.Delay(TimeSpan.FromMilliseconds(500), _cts.Token);
        }

        return unacked;
    }

    private async Task<long> GetAvailablePermits(string topic, string subscriptionName)
    {
        var stats = await _fixture.GetTopicStats(topic, _cts.Token);
        var consumers = stats.GetProperty("subscriptions").GetProperty(subscriptionName).GetProperty("consumers");
        return consumers.EnumerateArray().Sum(consumer => consumer.GetProperty("availablePermits").GetInt64());
    }

    private async Task WaitForAvailablePermits(string topic, string subscriptionName, int numberOfConsumersWithPermits)
    {
        for (var attempt = 0; attempt < 40; attempt++)
        {
            var stats = await _fixture.GetTopicStats(topic, _cts.Token);

            if (stats.TryGetProperty("subscriptions", out var subscriptions) &&
                subscriptions.TryGetProperty(subscriptionName, out var subscription) &&
                subscription.GetProperty("consumers").EnumerateArray().Count(consumer => consumer.GetProperty("availablePermits").GetInt64() > 0) == numberOfConsumersWithPermits)
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(250), _cts.Token);
        }

        throw new TimeoutException($"{numberOfConsumersWithPermits} consumers never got permits");
    }

    // The permits that the consumer has used up by receiving messages, and has not given the broker yet. The library has no InternalsVisibleTo, so the state of the channel of the consumer is read with reflection.
    private static long GetUnsentPermits<TMessage>(IConsumer<TMessage> consumer)
    {
        var subConsumer = GetField(consumer, "_singleSubConsumer");
        var channel = GetField(subConsumer, "_channel");
        var sendWhenZero = (uint) GetField(channel, "_sendWhenZero");
        var flow = (DotPulsar.Internal.PulsarApi.CommandFlow) GetField(channel, "_cachedCommandFlow");

        return (long) flow.MessagePermits - sendWhenZero;
    }

    private static object GetField(object instance, string name)
    {
        var field = instance.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic) ?? throw new InvalidOperationException($"The field {name} of {instance.GetType()} was not found");
        return field.GetValue(instance) ?? throw new InvalidOperationException($"The field {name} of {instance.GetType()} is null");
    }

    public void Dispose() => _cts.Dispose();
}
