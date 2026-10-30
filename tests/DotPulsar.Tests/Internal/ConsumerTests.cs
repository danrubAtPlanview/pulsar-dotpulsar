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

using Avro.Generic;
using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Tests.Schemas.TestSamples.AvroModels;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

[Collection("Integration"), Trait("Category", "Integration")]
public sealed class ConsumerTests : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public ConsumerTests(IntegrationFixture fixture, ITestOutputHelper testOutputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        _fixture = fixture;
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task GetLastMessageIds_GivenNonPartitionedTopic_ShouldGetMessageIdFromPartition()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        const int numberOfMessages = 6;

        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName);
        await using var producer = CreateProducer(client, topicName);

        var expected = new List<MessageId>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            var messageId = await producer.Send("test-message", _cts.Token);
            if (i >= 5)
            {
                expected.Add(messageId);
            }
        }

        //Act
        var actual = await consumer.GetLastMessageIds(_cts.Token);

        //Assert
        actual.ShouldBe(expected);
    }

    [Fact]
    public async Task GetLastMessageIds_GivenPartitionedTopic_ShouldGetMessageIdFromAllPartitions()
    {
        //Arrange
        const int numberOfMessages = 6;
        const int partitions = 3;
        var topicName = await _fixture.CreatePartitionedTopic(partitions, _cts.Token);

        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName);
        await using var producer = CreateProducer(client, topicName);

        var expected = new List<MessageId>();
        for (var i = 0; i < numberOfMessages; i++)
        {
            var messageId = await producer.Send("test-message", _cts.Token);
            if (i >= 3)
            {
                expected.Add(messageId);
            }
        }

        //Act
        var actual = await consumer.GetLastMessageIds(_cts.Token);

        //Assert
        actual.ShouldBeEquivalentTo(expected);
    }

    [Fact]
    public async Task GetLastMessageIds_GivenEmptyTopic_ShouldBeEqualToMessageIdEarliest()
    {
        //Arrange
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, await _fixture.CreateTopic(_cts.Token));
        var expected = new List<MessageId> { MessageId.Earliest };

        //Act
        var actual = await consumer.GetLastMessageIds(_cts.Token);

        //Assert
        actual.ShouldBe(expected);
    }

    [Fact]
    public async Task Receive_GivenNonPartitionedTopic_ShouldReceiveAll()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        const int numberOfMessages = 1000;

        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName);
        await using var producer = CreateProducer(client, topicName);

        //Act
        var produced = await ProduceMessages(producer, numberOfMessages, "test-message", _cts.Token);
        var consumed = await ConsumeMessages(consumer, numberOfMessages, _cts.Token);

        //Assert
        consumed.ShouldBe(produced);
    }

    [Fact]
    public async Task Receive_GivenPartitionedTopic_ShouldReceiveAll()
    {
        //Arrange
        const int numberOfMessages = 1000;
        const int partitions = 3;

        var topicName = await _fixture.CreatePartitionedTopic(partitions, _cts.Token);

        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName);
        await using var producer = CreateProducer(client, topicName);

        //Act
        var produced = await ProduceMessages(producer, numberOfMessages, "test-message", _cts.Token);
        var consumed = await ConsumeMessages(consumer, numberOfMessages, _cts.Token);

        //Assert
        consumed.ShouldBe(produced, true);
    }

    [Fact]
    public async Task Receive_GivenMultipleTopics_ShouldReceiveAll()
    {
        //Arrange
        const int numberOfMessages = 100;
        const int partitions = 3;

        var topic = await _fixture.CreateTopic(_cts.Token);
        var partitionedTopic = await _fixture.CreatePartitionedTopic(partitions, _cts.Token);

        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, [topic, partitionedTopic]);
        await using var producer = CreateProducer(client, topic);
        await using var partitionedProducer = CreateProducer(client, partitionedTopic);

        //Act
        var produced = new List<MessageId>();
        produced.AddRange(await ProduceMessages(producer, numberOfMessages, "test-message", _cts.Token));
        produced.AddRange(await ProduceMessages(partitionedProducer, numberOfMessages, "test-message", _cts.Token));
        var consumed = await ConsumeMessages(consumer, produced.Count, _cts.Token);

        //Assert
        consumed.ShouldBe(produced, true);
    }

    [Fact]
    public async Task Receive_GivenTopicsPattern_ShouldReceiveAll()
    {
        //Arrange
        var match1 = $"persistent://public/default/match-{Guid.NewGuid():N}";
        var match2 = $"persistent://public/default/match-{Guid.NewGuid():N}";
        var nomatch1 = $"non-persistent://public/default/match-{Guid.NewGuid():N}";
        const string nomatch2 = "persistent://public/default/nomatch";

        await _fixture.CreateTopics([match1, match2, nomatch1, nomatch2], _cts.Token);

        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, new Regex(@"persistent://public/default/match.*"));
        await using var producer1 = CreateProducer(client, match1);
        await using var producer2 = CreateProducer(client, match2);
        await using var producer3 = CreateProducer(client, nomatch1);
        await using var producer4 = CreateProducer(client, nomatch2);

        //Act
        var produced = new List<MessageId>();
        produced.AddRange(await ProduceMessages(producer1, 10, "test message", _cts.Token));
        produced.AddRange(await ProduceMessages(producer2, 10, "test message", _cts.Token));
        _ = await ProduceMessages(producer3, 10, "test message", _cts.Token);
        _ = await ProduceMessages(producer4, 10, "test message", _cts.Token);
        var consumed = await ConsumeMessages(consumer, produced.Count, _cts.Token);

        //Assert
        consumed.ShouldBe(produced, true);
    }

    [Fact]
    public async Task Receive_GivenTopicsPatternWithNoMatches_ShouldFaultConsumer()
    {
        //Arrange
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, new Regex(@"persistent://public/default/nosuchtopics.*"));

        //Act
        var exception = await Record.ExceptionAsync(consumer.Receive(_cts.Token).AsTask);

        //Assert
        exception.ShouldBeOfType<ConsumerFaultedException>();
    }

    [Fact]
    public async Task Receive_GivenInvalidTopicsPattern_ShouldFaultConsumer()
    {
        //Arrange
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, new Regex(@"invalid://public/default/match.*"));

        //Act
        var exception = await Record.ExceptionAsync(consumer.Receive(_cts.Token).AsTask);

        //Assert
        exception.ShouldBeOfType<ConsumerFaultedException>();
    }

    [Fact]
    public async Task Receive_WhenFaultedAfterInvokingReceive_ShouldThrowConsumerFaultedException()
    {
        //Arrange
        var semaphoreSlim = new SemaphoreSlim(1);
        await using var client = PulsarClient.Builder().ExceptionHandler(context =>
        {
            semaphoreSlim.WaitAsync();
            context.Result = FaultAction.Rethrow;
            context.ExceptionHandled = true;
        })
        .ServiceUrl(new Uri("pulsar://nosuchhost")).Build();

        await using var consumer = CreateConsumer(client, await _fixture.CreateTopic(_cts.Token));

        var receiveTask = consumer.Receive(_cts.Token);
        semaphoreSlim.Release();

        //Act
        var exception = await Record.ExceptionAsync(receiveTask.AsTask);

        //Assert
        exception.ShouldBeOfType<ConsumerFaultedException>();
    }

    [Fact]
    public async Task Receive_WhenFaultedBeforeInvokingReceive_ShouldThrowConsumerFaultedException()
    {
        //Arrange
        await using var client = PulsarClient.Builder().ExceptionHandler(context =>
        {
            context.Result = FaultAction.Rethrow;
            context.ExceptionHandled = true;
        })
        .ServiceUrl(new Uri("pulsar://nosuchhost")).Build();

        await using var consumer = CreateConsumer(client, await _fixture.CreateTopic(_cts.Token));

        await consumer.State.OnStateChangeTo(ConsumerState.Faulted, _cts.Token);

        //Act
        var exception = await Record.ExceptionAsync(consumer.Receive(_cts.Token).AsTask);

        //Assert
        exception.ShouldBeOfType<ConsumerFaultedException>();
    }

    [Fact]
    public async Task Receive_WhenReceivingFromTopicWithSchemaAndReceiverHasWrongAvroISpecificRecordSchema_ShouldThrowException()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var pulsarSchema = Schema.AvroISpecificRecord<ValidModel>();
        await _fixture.AddSchemaToExistingTopic(topicName, pulsarSchema.SchemaInfo, _cts.Token);
        var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName, Schema.String);
        await using var producer = CreateProducer(client, topicName, pulsarSchema);
        await producer.Send(new ValidModel(), _cts.Token);

        //Act
        var exception = await Record.ExceptionAsync(consumer.Receive(Current.CancellationToken).AsTask);

        //Assert
        exception.ShouldBeOfType<IncompatibleSchemaException>();
    }

    [Fact]
    public async Task Receive_WhenReceivingFromTopicWithSchemaAndReceiverHasRightAvroISpecificRecordSchema_ShouldBeAbleToRecieve()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var pulsarSchema = Schema.AvroISpecificRecord<ValidModel>();
        await _fixture.AddSchemaToExistingTopic(topicName, pulsarSchema.SchemaInfo, _cts.Token);
        var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName, pulsarSchema);
        await using var producer = CreateProducer(client, topicName, pulsarSchema);
        var expected = new ValidModel();
        await producer.Send(expected, _cts.Token);

        //Act
        var message = await consumer.Receive(_cts.Token);
        var actual = message.Value();

        //Assert
        actual.Name.ShouldBe(expected.Name);
        actual.Surname.ShouldBe(expected.Surname);
        actual.Age.ShouldBe(expected.Age);
    }

    [Fact]
    public async Task Receive_WhenReceivingFromTopicWithSchemaAndReceiverHasRightAvroGenericRecordSchema_ShouldBeAbleToRecieve()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var pulsarISpecificSchema = Schema.AvroISpecificRecord<ValidModel>();
        var pulsarGenericRecordSchema = Schema.AvroGenericRecord<GenericRecord>(ValidModel._SCHEMA.ToString());
        await _fixture.AddSchemaToExistingTopic(topicName, pulsarISpecificSchema.SchemaInfo, _cts.Token);
        var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName, pulsarGenericRecordSchema);
        await using var producer = CreateProducer(client, topicName, pulsarISpecificSchema);
        var expected = new ValidModel
        {
            Name = "Shukri",
            Surname = "Klinaku",
            Age = 57
        };
        await producer.Send(expected, _cts.Token);

        //Act
        var message = await consumer.Receive(_cts.Token);
        var actual = message.Value();

        //Assert
        actual["Name"].ShouldBe(expected.Name);
        actual["Surname"].ShouldBe(expected.Surname);
        actual["Age"].ShouldBe(expected.Age);
    }

    [Fact]
    public async Task Subscription_WhenSubscribingToAnExistingTopicWithNoSchema_ShouldNotSubscribeWithSchemaTypeNone()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName, Schema.ByteSequence);
        await producer.Send(Encoding.UTF8.GetBytes("Test"), _cts.Token);
        await using var consumer = CreateConsumer(client, topicName, Schema.ByteSequence);
        var receiveTask = consumer.Receive(_cts.Token);

        //Act
        var exception = await Record.ExceptionAsync(receiveTask.AsTask);

        //Assert
        exception.ShouldBeNull();
    }

    [Fact]
    public async Task Connectivity_WhenInitiallyConnectedWithNoMessagesThenGoesDown_ShouldBeAbleToReceiveWhenUpAgain()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, topicName);
        await using var producer = CreateProducer(client, topicName);
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var receiveTask = consumer.Receive(_cts.Token);
        await using (await _fixture.DisableThePulsarConnection())
        {
            await consumer.StateChangedTo(ConsumerState.Disconnected, _cts.Token);
        }
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await ProduceMessages(producer, 1, "test-message", _cts.Token);

        //Act
        var exception = await Record.ExceptionAsync(receiveTask.AsTask);

        //Assert
        exception.ShouldBeNull();
    }

    [Fact]
    public async Task Connectivity_WhenConnectionIsInitiallyDownAndComesUp_ShouldBeAbleToReceive()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await ProduceMessages(producer, 1, "test-message", _cts.Token);
        var connectionDown = await _fixture.DisableThePulsarConnection();
        await using var consumer = CreateConsumer(client, topicName);

        //Act
        await connectionDown.DisposeAsync();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var exception = await Record.ExceptionAsync(consumer.Receive(_cts.Token).AsTask);

        //Assert
        exception.ShouldBeNull();
    }

    [Fact]
    public async Task Connectivity_WhenConnectionIsInitiallyDown_ShouldBeAbleToDispose()
    {
        //Arrange
        await using var connectionDown = await _fixture.DisableThePulsarConnection();
        await using var client = CreateClient();
        await using var consumer = CreateConsumer(client, await _fixture.CreateTopic(_cts.Token));

        //Act
        var exception = await Record.ExceptionAsync(consumer.DisposeAsync().AsTask);

        //Assert
        exception.ShouldBeNull();
    }

    [Fact]
    public async Task Connectivity_WhenConnectionIsInitiallyUpAndGoesDown_ShouldBeAbleToDispose()
    {
        //Arrange
        await using var client = CreateClient();
        var consumer = CreateConsumer(client, await _fixture.CreateTopic(_cts.Token));
        await consumer.State.OnStateChangeTo(ConsumerState.Active, _cts.Token);

        //Act
        await using var connectionDown = await _fixture.DisableThePulsarConnection();
        await consumer.StateChangedTo(ConsumerState.Disconnected, _cts.Token);
        var exception = await Record.ExceptionAsync(consumer.DisposeAsync().AsTask);

        //Assert
        exception.ShouldBeNull();
    }

    [Fact]
    public async Task Connectivity_WhenConnectionIsInitiallyUpAndReconnects_ShouldBeAbleToReceive()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateConsumer(client, topicName);
        await ProduceMessages(producer, 1, "test-message", _cts.Token);
        await consumer.State.OnStateChangeTo(ConsumerState.Active, _cts.Token);

        //Act
        await using (await _fixture.DisableThePulsarConnection())
        {
            await consumer.StateChangedTo(ConsumerState.Disconnected, _cts.Token);
        }
        await consumer.State.OnStateChangeTo(ConsumerState.Active, _cts.Token);
        var exception = await Record.ExceptionAsync(consumer.Receive(_cts.Token).AsTask);

        //Assert
        exception.ShouldBeNull();
    }

    [Fact]
    public async Task TryReceive_WhenBufferIsEmpty_ShouldNotIncreasePermits()
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        var maxPrefetch = 2;

        using var httpClient = CreateAdminClient();
        await using var pulsarClient = CreateClient();
        await using var consumer = CreateConsumer(pulsarClient, topicName, subscription, Schema.ByteSequence, (uint) maxPrefetch);
        await using var producer = CreateProducer(pulsarClient, topicName, Schema.ByteSequence);

        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        // Wait until we get our first message
        await producer.Send([1], _cts.Token);
        var message = await consumer.Receive(_cts.Token);
        await consumer.Acknowledge(message, _cts.Token);

        //Act
        var maxPermits = 0L;
        for (int i = 0; i < maxPrefetch * 5; i++)
        {
            consumer.TryReceive(out _).ShouldBe(false);
            await Task.Delay(50, _cts.Token);
            var permits = await GetPermits(httpClient, topicName, subscription, _cts.Token);
            maxPermits = Math.Max(maxPermits, permits);
        }

        //Assert
        Assert.True(maxPermits <= maxPrefetch, $"availablePermits increased above the threshold of {maxPrefetch} to {maxPermits}");
    }

    private static async Task<IEnumerable<MessageId>> ProduceMessages(IProducer<string> producer, int numberOfMessages, string content, CancellationToken ct)
    {
        var messageIds = new MessageId[numberOfMessages];

        for (var i = 0; i < numberOfMessages; ++i)
        {
            messageIds[i] = await producer.Send(content, ct);
        }

        return messageIds;
    }

    private static async Task<IEnumerable<MessageId>> ConsumeMessages(IConsumer<string> consumer, int numberOfMessages, CancellationToken ct)
    {
        var messageIds = new List<MessageId>(numberOfMessages);

        await foreach (var message in consumer.Messages(ct))
        {
            messageIds.Add(message.MessageId);

            if (messageIds.Count != numberOfMessages)
                continue;

            await consumer.AcknowledgeCumulative(message, ct);

            break;
        }

        return messageIds;
    }

    [Fact]
    public async Task Acknowledge_GivenAckReceiptEnabled_ShouldCompleteAndAdvanceSubscription()
    {
        //Arrange
        const int numberOfMessages = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateAckReceiptConsumer(client, topicName, subscription);

        for (var i = 0; i < numberOfMessages; i++)
            _ = await producer.Send($"message-{i}", _cts.Token);

        //Act
        for (var i = 0; i < numberOfMessages; i++)
        {
            var message = await consumer.Receive(_cts.Token);
            await consumer.Acknowledge(message, _cts.Token);
        }

        //Assert
        using var adminClient = CreateAdminClient();
        var backlog = await GetBacklog(adminClient, topicName, subscription, _cts.Token);
        backlog.ShouldBe(0);
    }

    [Fact]
    public async Task AcknowledgeCumulative_GivenAckReceiptEnabled_ShouldCompleteAndAdvanceSubscription()
    {
        //Arrange
        const int numberOfMessages = 10;
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateAckReceiptConsumer(client, topicName, subscription);

        for (var i = 0; i < numberOfMessages; i++)
            _ = await producer.Send($"message-{i}", _cts.Token);

        IMessage<string>? last = null;
        for (var i = 0; i < numberOfMessages; i++)
            last = await consumer.Receive(_cts.Token);

        //Act
        await consumer.AcknowledgeCumulative(last!, _cts.Token);

        //Assert
        using var adminClient = CreateAdminClient();
        var backlog = await GetBacklog(adminClient, topicName, subscription, _cts.Token);
        backlog.ShouldBe(0);
    }

    // Upstream: AckReceiptOnTopicUnloadTest#testDelayedAckReceipt
    // The broker side of the upstream test (delaying the processing of the CommandAck on the broker) is not observable
    // from the client. Delaying the client to broker direction with Toxiproxy produces the same ordering instead.
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true, Skip = "Requires broker fix apache/pulsar#27128 (AckResponse for discarded acks), which is not in apachepulsar/pulsar:4.0.9")]
    [InlineData(true, true, Skip = "Requires broker fix apache/pulsar#27128 (AckResponse for discarded acks), which is not in apachepulsar/pulsar:4.0.9")]
    public async Task Acknowledge_GivenDelayedAckReceipt_ShouldCompleteOrFailWithConsumerNotFound(bool cumulative, bool unload)
    {
        //Arrange
        var topicName = await _fixture.CreateTopic(_cts.Token);
        var subscription = CreateSubscriptionName();
        await using var client = CreateClient();
        await using var producer = CreateProducer(client, topicName);
        await using var consumer = CreateAckReceiptConsumer(client, topicName, subscription);
        _ = await producer.Send("message", _cts.Token);
        var message = await consumer.Receive(_cts.Token);

        Task acknowledgeTask;
        await using (await _fixture.DelayUpstream(TimeSpan.FromSeconds(3)))
        {
            //Act
            acknowledgeTask = (cumulative ? consumer.AcknowledgeCumulative(message, _cts.Token) : consumer.Acknowledge(message, _cts.Token)).AsTask();
            await Task.Delay(TimeSpan.FromMilliseconds(500), _cts.Token);
            acknowledgeTask.IsCompleted.ShouldBeFalse();

            if (unload)
            {
                using var adminClient = CreateAdminClient();
                using var response = await adminClient.PutAsync($"/admin/v2/persistent/{topicName.Replace("persistent://", string.Empty)}/unload", null, _cts.Token);
                response.EnsureSuccessStatusCode();
            }

            //Assert
            if (unload)
                await Should.ThrowAsync<ConsumerNotFoundException>(acknowledgeTask);
            else
                await acknowledgeTask;
        }

        // The connection must still be usable after the failed or delayed acknowledgment
        await using var controlProducer = CreateProducer(client, topicName);
        var messageId = await controlProducer.Send("control", _cts.Token);
        messageId.ShouldNotBe(MessageId.Earliest);
    }

    private static string CreateSubscriptionName() => $"subscription-{Guid.NewGuid():N}";

    private IConsumer<string> CreateAckReceiptConsumer(IPulsarClient pulsarClient, string topicName, string subscription)
        => pulsarClient.NewConsumer(Schema.String)
        .InitialPosition(SubscriptionInitialPosition.Earliest)
        .SubscriptionName(subscription)
        .Topic(topicName)
        .AckReceiptEnabled(true)
        .StateChangedHandler(_testOutputHelper.Log)
        .Create();

    private static async ValueTask<long> GetBacklog(HttpClient httpClient, string topic, string subscription, CancellationToken cancellationToken)
    {
        topic = topic.Replace("persistent://", string.Empty);
        using var response = await httpClient.GetAsync($"/admin/v2/persistent/{topic}/stats", cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var json = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
        return json.RootElement.GetProperty("subscriptions").GetProperty(subscription).GetProperty("msgBacklog").GetInt64();
    }

    private IProducer<T> CreateProducer<T>(
        IPulsarClient pulsarClient,
        string topicName,
        ISchema<T> schema)
        => pulsarClient.NewProducer(schema)
        .Topic(topicName)
        .StateChangedHandler(_testOutputHelper.Log)
        .Create();

    private IProducer<string> CreateProducer(
        IPulsarClient pulsarClient,
        string topicName) => CreateProducer(pulsarClient, topicName, Schema.String);

    private IConsumer<string> CreateConsumer(IPulsarClient pulsarClient, string topicName)
        => CreateConsumer(pulsarClient, topicName, Schema.String);

    private IConsumer<T> CreateConsumer<T>(IPulsarClient pulsarClient, string topicName, ISchema<T> schema)
       => pulsarClient.NewConsumer(schema)
       .InitialPosition(SubscriptionInitialPosition.Earliest)
       .SubscriptionName(CreateSubscriptionName())
       .Topic(topicName)
       .StateChangedHandler(_testOutputHelper.Log)
       .Create();

    private IConsumer<string> CreateConsumer(IPulsarClient pulsarClient, IEnumerable<string> topics)
        => pulsarClient.NewConsumer(Schema.String)
        .InitialPosition(SubscriptionInitialPosition.Earliest)
        .SubscriptionName(CreateSubscriptionName())
        .Topics(topics)
        .StateChangedHandler(_testOutputHelper.Log)
        .Create();

    private IConsumer<string> CreateConsumer(IPulsarClient pulsarClient, Regex topicsPattern)
        => pulsarClient.NewConsumer(Schema.String)
        .InitialPosition(SubscriptionInitialPosition.Earliest)
        .SubscriptionName(CreateSubscriptionName())
        .TopicsPattern(topicsPattern)
        .StateChangedHandler(_testOutputHelper.Log)
        .Create();

    private IConsumer<T> CreateConsumer<T>(IPulsarClient pulsarClient, string topicName, string subscription, ISchema<T> schema, uint maxPrefetch)
        => pulsarClient.NewConsumer(schema)
       .InitialPosition(SubscriptionInitialPosition.Earliest)
       .SubscriptionName(subscription)
       .Topic(topicName)
       .StateChangedHandler(_testOutputHelper.Log)
       .MessagePrefetchCount(maxPrefetch)
       .Create();

    private IPulsarClient CreateClient()
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(_fixture.ServiceUrl)
        .Build();

    private HttpClient CreateAdminClient() => _fixture.CreateAdminClient();

    private static async ValueTask<long> GetPermits(HttpClient httpClient, string topic, string subscription, CancellationToken cancellationToken)
    {
        topic = topic.Replace("persistent://", string.Empty);
        using var response = await httpClient.GetAsync($"/admin/v2/persistent/{topic}/stats", cancellationToken).ConfigureAwait(false);
        if (response.IsSuccessStatusCode)
        {
            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            var json = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            if (!json.RootElement.TryGetProperty("subscriptions", out var subscriptionsProperty))
            {
                return 0;
            }

            if (!subscriptionsProperty.TryGetProperty(subscription, out var subscriptionProperty))
            {
                return 0;
            }

            if (subscriptionProperty.TryGetProperty("consumers", out var consumersProperty))
            {
                foreach (var consumer in consumersProperty.EnumerateArray())
                {
                    if (consumer.TryGetProperty("availablePermits", out var permitsProperty))
                    {
                        var permits = permitsProperty.GetInt64();
                        if (permits > 0)
                        {
                            return permits;
                        }
                    }
                }
            }
        }

        return 0;
    }

    public void Dispose() => _cts.Dispose();
}
