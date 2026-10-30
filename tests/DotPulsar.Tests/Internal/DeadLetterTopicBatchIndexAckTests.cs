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
using System.Text;

// The dead letter test of Java DeadLetterTopicTest that acknowledges single messages of a batch. It needs a broker that has acknowledgmentAtBatchIndexLevelEnabled, so the redelivered batch does not
// contain the messages that were acknowledged. That is the broker of the TransactionFixture, and not the broker of the IntegrationFixture that the other dead letter tests use.
[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class DeadLetterTopicBatchIndexAckTests : IDisposable
{
    private const string SubscriptionName = "my-subscription";
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public DeadLetterTopicBatchIndexAckTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Upstream: DeadLetterTopicTest#testAckedBatchMessageNotSentToDeadLetterTopicOnFinalRedeliveryRound
    // The batch indexes 1 and 2 are left to time out for the first maxRedeliverCount rounds, and acknowledged in the final round (the redelivery count is the maximum), which is the last chance of the
    // application to prevent that they are sent to the dead letter topic. The other 3 messages of the batch are acknowledged in the first delivery.
    [Fact]
    public async Task Receive_GivenBatchIndexesAcknowledgedOnFinalRound_ShouldNotSendAnyMessageToDeadLetterTopic()
    {
        //Arrange
        const int maxRedeliverCount = 3;
        const int batchSize = 5;
        var topicName = TransactionFixture.CreateTopicName("testAckedBatchMessageNotSentToDeadLetterTopicOnFinalRedeliveryRound");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .AckTimeout(TimeSpan.FromSeconds(1))
            .BatchIndexAckEnabled(true) // Upstream: enableBatchIndexAcknowledgment(true)
            .DeadLetterPolicy(new DeadLetterPolicy(maxRedeliverCount))
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(100)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topicName)
            .Create();
        await using var deadLetterConsumer = client.NewConsumer(Schema.ByteArray)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(SubscriptionName)
            .Topic($"{topicName}-{SubscriptionName}-DLQ")
            .Create();
        await using var producer = client.NewProducer(Schema.ByteArray)
            .BatchingMaxMessages(batchSize)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(1))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await deadLetterConsumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < batchSize; i++)
        {
            receipts.Add(await SendAsync(producer, Encoding.UTF8.GetBytes($"message-{i}")));
        }

        var sentIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        sentIds.Select(messageId => messageId.BatchIndex).ShouldBe(Enumerable.Range(0, batchSize));

        // Expected deliveries: (batchSize - 2) once each, plus the indexes 1 and 2 on every round from 0 through maxRedeliverCount inclusive
        const int expectedDeliveries = (batchSize - 2) + (2 * (maxRedeliverCount + 1));

        //Act
        var received = 0;
        while (received < expectedDeliveries)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token);
            message.ShouldNotBeNull("The consumer should keep receiving messages until the batch settles");
            received++;
            var batchIndex = message.MessageId.BatchIndex;

            // Let it time out instead of acknowledging it
            if ((batchIndex == 1 || batchIndex == 2) && message.RedeliveryCount < maxRedeliverCount)
                continue;

            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        // No message should ever be sent to the dead letter topic, since every message was acknowledged at or before its final allowed redelivery round
        var deadLetter = await TransactionFixture.ReceiveOrNull(deadLetterConsumer, TimeSpan.FromSeconds(5), _cts.Token);
        deadLetter.ShouldBeNull();
    }

    // The Java producer.sendAsync: the message is queued in the order of the calls, and the task completes when the broker has stored the message
    private async ValueTask<Task<MessageId>> SendAsync(IProducer<byte[]> producer, byte[] value)
    {
        var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);

        await producer.SendChannel.Send(new MessageMetadata(), value, messageId =>
        {
            _ = receipt.TrySetResult(messageId);
            return ValueTask.CompletedTask;
        }, _cts.Token);

        return receipt.Task;
    }
}
