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

// TODO(WP6): The rows of the tests that need the producer to batch messages are skipped.
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class NegativeAcknowledgeTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public NegativeAcknowledgeTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Upstream: NegativeAcksTest#testNegativeAcks
    // The columns are batching, partitions, subscription type, negative acknowledge redelivery delay in milliseconds and ack timeout in milliseconds.
    // The rows that batch the messages are skipped until the producer can batch them (TODO(WP6)).
    [Theory]
    [InlineData(false, false, SubscriptionType.Shared, 100, 0)]
    [InlineData(false, false, SubscriptionType.Shared, 100, 1000)]
    [InlineData(false, false, SubscriptionType.Shared, 0, 0)]
    [InlineData(false, false, SubscriptionType.Shared, 0, 1000)]
    [InlineData(false, false, SubscriptionType.Failover, 100, 0)]
    [InlineData(false, false, SubscriptionType.Failover, 100, 1000)]
    [InlineData(false, false, SubscriptionType.Failover, 0, 0)]
    [InlineData(false, false, SubscriptionType.Failover, 0, 1000)]
    [InlineData(false, true, SubscriptionType.Shared, 100, 0)]
    [InlineData(false, true, SubscriptionType.Shared, 100, 1000)]
    [InlineData(false, true, SubscriptionType.Shared, 0, 0)]
    [InlineData(false, true, SubscriptionType.Shared, 0, 1000)]
    [InlineData(false, true, SubscriptionType.Failover, 100, 0)]
    [InlineData(false, true, SubscriptionType.Failover, 100, 1000)]
    [InlineData(false, true, SubscriptionType.Failover, 0, 0)]
    [InlineData(false, true, SubscriptionType.Failover, 0, 1000)]
    [InlineData(true, false, SubscriptionType.Shared, 100, 0, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, false, SubscriptionType.Shared, 100, 1000, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, false, SubscriptionType.Shared, 0, 0, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, false, SubscriptionType.Shared, 0, 1000, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, false, SubscriptionType.Failover, 100, 0, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, false, SubscriptionType.Failover, 100, 1000, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, false, SubscriptionType.Failover, 0, 0, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, false, SubscriptionType.Failover, 0, 1000, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, true, SubscriptionType.Shared, 100, 0, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, true, SubscriptionType.Shared, 100, 1000, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, true, SubscriptionType.Shared, 0, 0, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, true, SubscriptionType.Shared, 0, 1000, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, true, SubscriptionType.Failover, 100, 0, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, true, SubscriptionType.Failover, 100, 1000, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, true, SubscriptionType.Failover, 0, 0, Skip = "Requires producer batching (WP6)")]
    [InlineData(true, true, SubscriptionType.Failover, 0, 1000, Skip = "Requires producer batching (WP6)")]
    public async Task NegativeAcknowledge_GivenVariations_ShouldRedeliverAllNegativelyAcknowledgedMessages(bool batching, bool usePartitions, SubscriptionType subscriptionType, int negativeAcknowledgeDelayMilliseconds, int ackTimeoutMilliseconds)
    {
        //Arrange
        _ = batching;
        const int numberOfMessages = 10;
        var topicName = usePartitions ? await _fixture.CreatePartitionedTopic(3, _cts.Token) : await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .AckTimeout(TimeSpan.FromMilliseconds(ackTimeoutMilliseconds))
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromMilliseconds(negativeAcknowledgeDelayMilliseconds))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .SubscriptionType(subscriptionType)
            .Topic(topicName)
            .Create();
        await using var producer = client.NewProducer(Schema.String)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        var sentMessages = new HashSet<string>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            var value = $"test-{i}";
            sentMessages.Add(value);
            _ = await producer.Send(value, _cts.Token);
        }

        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await ReceiveMessage(consumer);
            await consumer.NegativeAcknowledge(message, _cts.Token);
        }

        //Act
        var receivedMessages = new HashSet<string>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await ReceiveMessage(consumer);
            receivedMessages.Add(message.Value());
            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        receivedMessages.ShouldBe(sentMessages, true);
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromMilliseconds(500), _cts.Token)).ShouldBeNull();
    }

    // TODO(WP6): Remove the skip. The test needs the producer to batch messages (BatchingMaxPublishDelay of 1 hour and BatchingMaxMessages of 2): two messages are sent as one batch,
    // the first is negatively acknowledged, and the second is negatively acknowledged 2 seconds later. The first message must be redelivered within 2 seconds.
    // Upstream: NegativeAcksTest#testNegativeAcksWithBatch
    [Fact(Skip = "Requires producer batching (WP6)")]
    public Task NegativeAcknowledge_GivenTwoMessagesInSameBatchNackedTwoSecondsApart_ShouldRedeliverFirstWithinTwoSeconds()
        => Task.CompletedTask;

    // TODO(WP6): Remove the skip. The test needs the producer to batch messages and BatchIndexAckEnabled. Ten messages are sent, all are negatively acknowledged,
    // and all are received again and acknowledged, after which no more messages are received.
    // Upstream: NegativeAcksTest#testNegativeAcksWithBatchAckEnabled
    [Fact(Skip = "Requires producer batching (WP6)")]
    public Task NegativeAcknowledge_GivenTenBatchedMessages_ShouldRedeliverAllAndThenNoMore()
        => Task.CompletedTask;

    [Fact]
    public async Task NegativeAcknowledge_GivenMessageId_ShouldRedeliverAfterTheDelayAndNotBefore()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.String)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromSeconds(3))
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var producer = client.NewProducer(Schema.String)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        _ = await producer.Send("test", _cts.Token);
        var message = await ReceiveMessage(consumer);

        //Act
        await consumer.NegativeAcknowledge(message.MessageId, _cts.Token);
        var earlyRedelivery = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token);
        var redelivery = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);

        //Assert
        earlyRedelivery.ShouldBeNull();
        redelivery.ShouldNotBeNull();
        redelivery.Value().ShouldBe("test");
        redelivery.RedeliveryCount.ShouldBe(1U);
    }

    private async Task<IMessage<string>> ReceiveMessage(IConsumer<string> consumer)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();
}
