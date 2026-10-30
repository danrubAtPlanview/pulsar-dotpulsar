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

// The broker of the IntegrationFixture has acknowledgmentAtBatchIndexLevelEnabled=false (verified by the test below).
// The batches are produced by IntegrationFixture.ProduceBatchedMessages, and by a producer that batches in the test of the shared subscription.
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class BatchMessageIndexAckDisableTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public BatchMessageIndexAckDisableTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Additional coverage
    [Fact]
    public async Task Broker_ShouldNotHaveBatchIndexAcknowledgmentEnabled()
    {
        //Act
        using var httpClient = _fixture.CreateAdminClient();
        using var response = await httpClient.GetAsync("/admin/v2/brokers/configuration/runtime", _cts.Token);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(_cts.Token);
        using var json = await System.Text.Json.JsonDocument.ParseAsync(stream, cancellationToken: _cts.Token);

        //Assert
        json.RootElement.GetProperty("acknowledgmentAtBatchIndexLevelEnabled").GetString().ShouldBe("false");
    }

    // Upstream: BatchMessageIndexAckDisableTest#testBatchMessageIndexAckForSharedSubscription
    // The consumer acknowledges the first 50 messages of the batch of 100 messages. The broker does not acknowledge at the batch index level, so it still has the whole batch, and the ack timeout of
    // 1 second makes it redeliver the batch. The consumer receives the other 50 messages that were already sent to it, and then the messages of the redelivered batch.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Acknowledge_GivenBatchIndexAckDisabledOnBrokerAndSharedSubscription_ShouldReceiveAllMessagesAfterAckTimeout(bool ackReceiptEnabled)
    {
        //Arrange
        const int numberOfMessages = 100;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.Int32)
            .AckReceiptEnabled(ackReceiptEnabled)
            .AckTimeout(TimeSpan.FromSeconds(1))
            .BatchIndexAckEnabled(true) // The default of the Java consumer
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(numberOfMessages)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("sub")
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var producer = client.NewProducer(Schema.Int32)
            .BatchingMaxMessages(numberOfMessages)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(5))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            receipts.Add(await SendAsync(producer, i));
        }

        var sentIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        sentIds.Select(messageId => (messageId.LedgerId, messageId.EntryId)).Distinct().Count().ShouldBe(1);

        //Act
        for (var i = 0; i < numberOfMessages; i++)
        {
            if (i % 2 == 0)
                await consumer.Acknowledge(await Receive(consumer), _cts.Token);
        }

        var received = new List<IMessage<int>>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            received.Add(await Receive(consumer));
        }

        //Assert
        received.Count.ShouldBe(numberOfMessages);
        received.Count(message => message.RedeliveryCount >= 1).ShouldBeGreaterThanOrEqualTo(numberOfMessages / 2);
    }

    // Upstream: BatchMessageIndexAckDisableTest#testBatchMessageIndexAckForExclusiveSubscription
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AcknowledgeCumulative_GivenBatchIndexAckDisabledOnBrokerAndExclusiveSubscription_ShouldRedeliverAllMessages(bool ackReceiptEnabled)
    {
        //Arrange
        const int numberOfMessages = 100;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();

        // The Java consumer enables batch index acknowledgment by default
        var consumer = CreateConsumer(client, topicName, subscription, ackReceiptEnabled, true);
        await _fixture.ProduceBatchedMessages(topicName, 1, numberOfMessages, _cts.Token);

        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await consumer.Receive(_cts.Token);

            if (i == 49)
                await consumer.AcknowledgeCumulative(message, _cts.Token);
        }

        // Wait for the acknowledgment to be sent
        await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);
        await consumer.DisposeAsync();
        await using var resubscribedConsumer = CreateConsumer(client, topicName, subscription, false, false);

        //Act
        var received = new List<IMessage<byte[]>>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            received.Add(await resubscribedConsumer.Receive(_cts.Token));
        }

        //Assert
        received.Count.ShouldBe(numberOfMessages);
    }

    // Additional coverage
    [Fact]
    public async Task AcknowledgeCumulative_GivenDefaultOptionsAndBatchIndexAckDisabledOnBroker_ShouldRedeliverAllMessages()
    {
        //Arrange
        const int numberOfMessages = 100;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        var consumer = CreateConsumer(client, topicName, subscription, false, false);
        await _fixture.ProduceBatchedMessages(topicName, 1, numberOfMessages, _cts.Token);

        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await consumer.Receive(_cts.Token);

            if (i == 49)
                await consumer.AcknowledgeCumulative(message, _cts.Token);
        }

        await Task.Delay(TimeSpan.FromSeconds(1), _cts.Token);
        await consumer.DisposeAsync();
        await using var resubscribedConsumer = CreateConsumer(client, topicName, subscription, false, false);

        //Act
        var received = new List<IMessage<byte[]>>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            received.Add(await resubscribedConsumer.Receive(_cts.Token));
        }

        //Assert
        received.Count.ShouldBe(numberOfMessages);
    }

    private static string CreateSubscriptionName() => $"subscription-{Guid.NewGuid():N}";

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

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();

    private IConsumer<byte[]> CreateConsumer(IPulsarClient client, string topicName, string subscription, bool ackReceiptEnabled, bool batchIndexAckEnabled)
        => client.NewConsumer(Schema.ByteArray)
        .AckReceiptEnabled(ackReceiptEnabled)
        .BatchIndexAckEnabled(batchIndexAckEnabled)
        .InitialPosition(SubscriptionInitialPosition.Earliest)
        .MessagePrefetchCount(100)
        .StateChangedHandler(_testOutputHelper.Log)
        .SubscriptionName(subscription)
        .Topic(topicName)
        .Create();

    public void Dispose() => _cts.Dispose();
}
