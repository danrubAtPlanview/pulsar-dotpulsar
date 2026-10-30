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

// The test of Java NegativeAcksTest that acknowledges a message of a batch cumulatively. It needs a broker that has acknowledgmentAtBatchIndexLevelEnabled (the default of the broker upstream), so the redelivered
// batch does not contain the messages that were acknowledged. That is the broker of the TransactionFixture, and not the broker of the IntegrationFixture that the other negative acknowledge tests use.
[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class NegativeAcknowledgeBatchIndexAckTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public NegativeAcknowledgeBatchIndexAckTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Upstream: NegativeAcksTest#testFailoverConsumerBatchCumulateAck
    // The upstream consumer acknowledges in groups of 100 milliseconds, and DotPulsar acknowledges immediately. The upstream producer routes the 10 messages to one partition, because the routing changes
    // after the publish delay, and DotPulsar routes them in turn to the 2 partitions, so the 10 messages are in two batches of 5 messages that are sent when the publish delay has passed.
    [Fact]
    public async Task AcknowledgeCumulative_GivenFailoverConsumerAndMessageOfBatch_ShouldRedeliverEveryOtherMessage()
    {
        //Arrange
        const int numberOfMessages = 10;
        var topicName = await _fixture.CreatePartitionedTopic(2, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = client.NewConsumer(Schema.Int32)
            .BatchIndexAckEnabled(true) // The default of the Java consumer
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .MessagePrefetchCount(10)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("sub")
            .SubscriptionType(SubscriptionType.Failover)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await using var producer = client.NewProducer(Schema.Int32)
            .BatchingMaxMessages(10)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(3))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();

        var datas = new HashSet<int>();
        var receipts = new List<Task<MessageId>>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            _ = datas.Add(i);
            receipts.Add(await SendAsync(producer, i));
        }

        var sentIds = await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        sentIds.ShouldAllBe(messageId => messageId.BatchIndex >= 0);

        //Act
        var first = await Receive(consumer, TimeSpan.FromSeconds(30));
        _ = datas.Remove(first.Value());
        await consumer.AcknowledgeCumulative(first, _cts.Token);
        await Task.Delay(TimeSpan.FromMilliseconds(500), _cts.Token);
        await consumer.RedeliverUnacknowledgedMessages(_cts.Token);
        await Task.Delay(TimeSpan.FromMilliseconds(500), _cts.Token);
        var count = 0;
        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token) is { } message)
        {
                await consumer.AcknowledgeCumulative(message, _cts.Token);
            await Task.Delay(TimeSpan.FromMilliseconds(200), _cts.Token);
            _ = datas.Remove(message.Value());
            count++;
        }

        //Assert
        count.ShouldBe(9);
        datas.Count.ShouldBe(0);
    }

    private async Task<IMessage<TMessage>> Receive<TMessage>(IConsumer<TMessage> consumer, TimeSpan timeout)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, timeout, _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }

    // The Java producer.sendAsync: the message is queued in the order of the calls, and the task completes when the broker has stored the message
    private async ValueTask<Task<MessageId>> SendAsync(IProducer<int> producer, int value)
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
