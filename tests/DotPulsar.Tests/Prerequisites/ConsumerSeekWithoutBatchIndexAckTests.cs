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

// The broker has acknowledgmentAtBatchIndexLevelEnabled=false, so it ignores the ack set of a seek
[Collection("TransactionWithoutBatchIndexAck"), Trait("Category", "Integration")]
public sealed class ConsumerSeekWithoutBatchIndexAckTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly TransactionWithoutBatchIndexAckFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public ConsumerSeekWithoutBatchIndexAckTests(TransactionWithoutBatchIndexAckFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    public void Dispose() => _cts.Dispose();

    // Additional coverage
    // The upstream broker setting that the ack set of a seek needs is acknowledgmentAtBatchIndexLevelEnabled. Without it the broker redelivers the whole batch, which is what DotPulsar did before
    // it sent the ack set, and the messages of the batch before the message that was seeked to are delivered again. The Java client also drops those messages itself, because it remembers the id that
    // was seeked to (ConsumerImpl#isPriorBatchIndex), and DotPulsar does not, so this is a difference with the Java client when the broker does not have the setting.
    [Fact]
    public async Task Seek_GivenMessageIdOfBatchMessageAndBrokerWithoutBatchIndexAck_ShouldReplayTheWholeBatch()
    {
        //Arrange
        const int batchSize = 5;
        var topicName = TransactionFixture.CreateTopicName("seek-batch-index-without");
        await _fixture.CreateTopic(topicName, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var consumer = client.NewConsumer(Schema.String)
            .BatchIndexAckEnabled(true)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName($"subscription-{Guid.NewGuid():N}")
            .Topic(topicName)
            .Create();
        await using var producer = client.NewProducer(Schema.String)
            .BatchingMaxMessages(batchSize)
            .BatchingMaxPublishDelay(TimeSpan.FromHours(1))
            .EnableBatching(true)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topicName)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var receipts = new List<Task<MessageId>>();

        for (var i = 0; i < batchSize; i++)
        {
            var receipt = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);

            await producer.SendChannel.Send(new MessageMetadata(), i.ToString(), messageId =>
            {
                _ = receipt.TrySetResult(messageId);
                return ValueTask.CompletedTask;
            }, _cts.Token);

            receipts.Add(receipt.Task);
        }

        await producer.Flush(_cts.Token);
        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        var received = new List<IMessage<string>>();

        for (var i = 0; i < batchSize; i++)
        {
            received.Add(await Receive(consumer));
        }

        //Act
        await consumer.Seek(received[2].MessageId, _cts.Token);
        var replayed = new List<IMessage<string>>();

        for (var i = 0; i < batchSize; i++)
        {
            replayed.Add(await Receive(consumer));
        }

        //Assert
        received[2].MessageId.BatchIndex.ShouldBe(2);
        replayed.Select(message => message.MessageId.BatchIndex).ShouldBe(Enumerable.Range(0, batchSize));
        (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(1), _cts.Token)).ShouldBeNull();
    }

    private async Task<IMessage<string>> Receive(IConsumer<string> consumer)
    {
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
        message.ShouldNotBeNull();
        return message;
    }
}
