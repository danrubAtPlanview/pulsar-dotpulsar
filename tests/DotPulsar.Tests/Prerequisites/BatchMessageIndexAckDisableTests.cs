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
// The batches are produced by IntegrationFixture.ProduceBatchedMessages.
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

    // TODO(WP7): Remove the skip. The test needs ackTimeout(1 second), which redelivers the acknowledged half of the batch.
    // Upstream: BatchMessageIndexAckDisableTest#testBatchMessageIndexAckForSharedSubscription
    [Theory(Skip = "Requires ack timeout (WP7)")]
    [InlineData(true)]
    [InlineData(false)]
    public Task Acknowledge_GivenBatchIndexAckDisabledOnBrokerAndSharedSubscription_ShouldReceiveAllMessagesAfterAckTimeout(bool ackReceiptEnabled)
    {
        _ = ackReceiptEnabled;
        return Task.CompletedTask;
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
