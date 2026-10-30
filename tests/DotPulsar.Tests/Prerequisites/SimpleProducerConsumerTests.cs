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
using DotPulsar.Exceptions;
using DotPulsar.Extensions;

// Upstream: SimpleProducerConsumerTest
// The broker of the Java tests is stopped and started, and here the connection to the broker is cut and restored with Toxiproxy.
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class SimpleProducerConsumerTests : IDisposable
{
    private static readonly TimeSpan _receiveTimeout = TimeSpan.FromSeconds(5);
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public SimpleProducerConsumerTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: SimpleProducerConsumerTest#testSendTimeout
    [Theory]
    [InlineData(0)]
    [InlineData(1000)]
    public async Task Send_GivenBrokerUnreachable_ShouldFailWithOperationTimeoutException(int batchMessageDelayMs)
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var builder = client.NewProducer(Schema.String).Topic(topic).SendTimeout(TimeSpan.FromSeconds(1)).StateChangedHandler(_testOutputHelper.Log);

        if (batchMessageDelayMs != 0)
            builder = builder.EnableBatching(true).BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(batchMessageDelayMs)).BatchingMaxMessages(5);

        await using var producer = builder.Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);

        //Act
        Exception? exception;
        await using (await _fixture.DisableThePulsarConnection())
        {
            await producer.StateChangedTo(ProducerState.Disconnected, _cts.Token);
            exception = await Record.ExceptionAsync(async () => await producer.Send("my-message", _cts.Token));
        }

        //Assert
        exception.ShouldBeOfType<OperationTimeoutException>();
        producer.State.IsFinalState().ShouldBeFalse();
        (await TransactionFixture.ReceiveOrNull(consumer, _receiveTimeout, _cts.Token)).ShouldBeNull();
    }

    // Upstream: SimpleProducerConsumerTest#testSendCallBackReturnSequenceId
    // The exception of DotPulsar has no sequence id property, so the sequence id is in the message of the exception.
    [Fact]
    public async Task Send_GivenBrokerUnreachable_ShouldFailEachMessageWithItsSequenceId()
    {
        //Arrange
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = client.NewProducer(Schema.String).Topic(topic).InitialSequenceId(0).SendTimeout(TimeSpan.FromSeconds(1)).StateChangedHandler(_testOutputHelper.Log).Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        var sends = new List<Task<MessageId>>();

        //Act
        await using (await _fixture.DisableThePulsarConnection())
        {
            await producer.StateChangedTo(ProducerState.Disconnected, _cts.Token);

            for (var i = 0; i < 3; ++i)
            {
                sends.Add(producer.Send($"message-{i}", _cts.Token).AsTask());
            }

            foreach (var send in sends)
                _ = await Record.ExceptionAsync(() => send);
        }

        //Assert
        for (var i = 0; i < 3; ++i)
        {
            var exception = await Should.ThrowAsync<OperationTimeoutException>(sends[i]);
            exception.Message.ShouldContain($"sequence id {i} ");
        }
    }

    // Upstream: SimpleProducerConsumerTest#testSendTimeoutAndRecover
    // Java checks that the flush throws, but the messages that failed are not waiting any longer in DotPulsar, so there is nothing for the flush to wait for.
    [Theory]
    [InlineData(0)]
    [InlineData(1000)]
    public async Task Send_GivenSendTimeoutAndBrokerRecovers_ShouldSendAfterRecovery(int batchMessageDelayMs)
    {
        //Arrange
        var topic = await _fixture.CreatePartitionedTopic(6, _cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topic);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var builder = client.NewProducer(Schema.String).Topic(topic).SendTimeout(TimeSpan.FromSeconds(1)).StateChangedHandler(_testOutputHelper.Log);

        if (batchMessageDelayMs != 0)
            builder = builder.EnableBatching(true).BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(batchMessageDelayMs)).BatchingMaxMessages(5);

        await using var producer = builder.Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        Exception? exception;

        await using (await _fixture.DisableThePulsarConnection())
        {
            await producer.StateChangedTo(ProducerState.Disconnected, _cts.Token);
            exception = await Record.ExceptionAsync(async () => await producer.Send("before", _cts.Token));
        }

        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);

        //Act
        await producer.Send("after", _cts.Token);

        //Assert
        exception.ShouldBeOfType<OperationTimeoutException>();
        var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(30), _cts.Token);
        message.ShouldNotBeNull();
        message.Value().ShouldBe("after");
    }

    // Upstream: SimpleProducerConsumerTest#testSendMsgGreaterThanBatchingMaxBytes
    // A message that is larger than the maximum size of a batch is sent alone, and the send must complete before the send timeout.
    [Fact]
    public async Task Send_GivenMessageLargerThanBatchingMaxBytes_ShouldCompleteWithinSendTimeout()
    {
        //Arrange
        const int batchingMaxBytes = 1024;
        var sendTimeout = TimeSpan.FromSeconds(10);
        var message = new byte[batchingMaxBytes * 2];
        Random.Shared.NextBytes(message);
        var topic = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName("my-subscription")
            .Topic(topic)
            .Create();
        await using var producer = client.NewProducer(Schema.ByteArray)
            .BatchingMaxBytes(batchingMaxBytes)
            .BatchingMaxMessages(1000)
            .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(1))
            .EnableBatching(true)
            .SendTimeout(sendTimeout)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic)
            .Create();

        //Act
        // The send should complete in time
        var messageId = await producer.Send(message, _cts.Token).AsTask().WaitAsync(sendTimeout, _cts.Token);

        //Assert
        messageId.ShouldNotBeNull();
        messageId.BatchIndex.ShouldBe(-1);
        var received = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);
        received.ShouldNotBeNull();
        received.Value().ShouldBe(message);
    }

    public void Dispose()
        => _cts.Dispose();

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();

    private IConsumer<string> CreateConsumer(IPulsarClient client, string topic)
        => client.NewConsumer(Schema.String)
        .Topic(topic)
        .InitialPosition(SubscriptionInitialPosition.Earliest)
        .StateChangedHandler(_testOutputHelper.Log)
        .SubscriptionName("my-subscription")
        .Create();
}
