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
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;

// A producer that connects again must be the same producer for the broker, with or without a name that the user gave it: the broker deduplicates by the name of the producer, and it only replaces
// the producer that it has by the one that connects again if that is the same producer. Acceptance criterion 7 of 06-prerequisites/producer-batching.md.
[Collection("Integration"), Trait("Category", "Integration")]
public sealed class ProducerReconnectTests : IDisposable
{
    private const string _subscription = "my-subscription";

    // The broker closes a connection that it does not hear from after its keep alive of 30 seconds (it sends a ping, and closes when the next one is due and it has not heard from the client)
    private static readonly TimeSpan _keepAlive = TimeSpan.FromSeconds(30);

    private readonly CancellationTokenSource _cts;
    private readonly IntegrationFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public ProducerReconnectTests(IntegrationFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(4));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Additional coverage
    // The first 20 messages are acknowledged by the broker. Then the receipts are lost, so the producer sends the other messages, and the broker stores them, but the producer never gets the receipts.
    // When the connection is closed, the producer connects again and sends the messages that it has no receipt for again, with the same sequence ids. The broker deduplicates them if it takes the
    // producer for the one that it had, which it does if the name is the same, so the topic has every message once and in order. This is the same for a producer that has a name and for one that has not.
    [Theory]
    [InlineData(null)]
    [InlineData("deduplication-producer")]
    public async Task Send_GivenLostReceiptsAndTheConnectionIsClosed_ShouldResendWithoutDuplicates(string? producerName)
    {
        //Arrange
        const int numberOfMessages = 200;
        const int numberOfAcknowledgedMessages = 20;
        var topic = await CreateTopicWithDeduplication();
        await using var relay = await LossyRelay.Start(_fixture.ServiceUrl, _cts.Token);
        await using var client = CreateClient(relay.ServiceUrl);
        await using var producer = await CreateProducer(client, topic, producerName, numberOfMessages);
        var receipts = new List<Task<MessageId>>();

        for (var i = 0; i < numberOfAcknowledgedMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        relay.SwallowBrokerToClient();

        for (var i = numberOfAcknowledgedMessages; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        // The broker has stored all the messages, and the producer has not received a receipt
        await WaitUntilAsync(async () => await GetMessagesPublished(topic) >= numberOfMessages);
        receipts.Skip(numberOfAcknowledgedMessages).ShouldAllBe(receipt => !receipt.IsCompleted);

        //Act
        relay.DropConnections();
        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(60), _cts.Token);

        //Assert
        (await ReceiveAll(topic)).ShouldBe(Enumerable.Range(0, numberOfMessages).Select(i => $"msg-{i}"));
    }

    // Additional coverage
    // The connection is cut like a network is: the producer sees the connection close, and the broker does not, so the broker still has the producer of the old connection when the producer connects
    // again. It does not give the name to a second producer, and it asks the old connection whether it is alive: it sends a ping, and when it has no answer in 5 seconds (connectionLivenessCheckTimeoutMillis)
    // it closes the connection and takes the new producer. The producer is connected again about 5 seconds after the cut, and not after the keep alive of the broker (30 seconds, and another 30 before it closes).
    // The messages that the broker had stored are deduplicated, because the producer is the same producer for the broker.
    [Theory]
    [InlineData(null)]
    [InlineData("deduplication-producer")]
    public async Task Send_GivenLostReceiptsAndTheBrokerStillHasTheOldProducer_ShouldConnectAgainBeforeTheKeepAliveOfTheBrokerAndResendWithoutDuplicates(string? producerName)
    {
        //Arrange
        const int numberOfMessages = 200;
        const int numberOfAcknowledgedMessages = 20;
        var topic = await CreateTopicWithDeduplication();
        await using var relay = await LossyRelay.Start(_fixture.ServiceUrl, _cts.Token);
        await using var client = CreateClient(relay.ServiceUrl);
        await using var producer = await CreateProducer(client, topic, producerName, numberOfMessages);
        var receipts = new List<Task<MessageId>>();

        for (var i = 0; i < numberOfAcknowledgedMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);
        relay.SwallowBrokerToClient();

        for (var i = numberOfAcknowledgedMessages; i < numberOfMessages; ++i)
        {
            receipts.Add(await SendAsync(producer, $"msg-{i}"));
        }

        await WaitUntilAsync(async () => await GetMessagesPublished(topic) >= numberOfMessages);
        receipts.Skip(numberOfAcknowledgedMessages).ShouldAllBe(receipt => !receipt.IsCompleted);
        var publishersBefore = await GetPublishers(topic);

        //Act
        var stopwatch = Stopwatch.StartNew();
        relay.DropClientConnections();
        var publishersWhileTheBrokerStillHasTheOldProducer = await GetPublishers(topic);
        await Task.WhenAll(receipts).WaitAsync(TimeSpan.FromSeconds(60), _cts.Token);
        stopwatch.Stop();
        _testOutputHelper.Log($"The producer {(producerName is null ? "without" : "with")} a name was connected again and had every receipt {stopwatch.Elapsed.TotalSeconds:F1} seconds after the connection was cut");

        //Assert
        publishersBefore.Count.ShouldBe(1);
        publishersWhileTheBrokerStillHasTheOldProducer.ShouldBe(publishersBefore, "the broker has not seen the connection close, so it still has the old producer");
        stopwatch.Elapsed.ShouldBeLessThan(_keepAlive - TimeSpan.FromSeconds(10), "the producer is connected again after the liveness check of the broker, and not after its keep alive");
        (await GetPublishers(topic)).Select(publisher => publisher.Name).ShouldBe(publishersBefore.Select(publisher => publisher.Name), "the broker has the producer once, with the name that it generated the first time");
        (await ReceiveAll(topic)).ShouldBe(Enumerable.Range(0, numberOfMessages).Select(i => $"msg-{i}"));
    }

    // Additional coverage
    // The broker closes the producer when the topic is unloaded, and the producer connects again on the same connection with the same id and the name that it was given. A producer without a name
    // is the same producer for the broker after that, so it has the same name, and it sends again with the sequence ids that it had.
    [Fact]
    public async Task Connectivity_GivenTopicIsUnloaded_ShouldConnectAgainWithTheSameProducerNameAndProducerId()
    {
        //Arrange
        var topic = await CreateTopicWithDeduplication();
        await using var client = CreateClient(_fixture.ServiceUrl);
        var states = new ConcurrentQueue<ProducerState>();
        await using var producer = client.NewProducer(Schema.String)
            .StateChangedHandler(change =>
            {
                states.Enqueue(change.ProducerState);
                _testOutputHelper.Log(change);
            })
            .Topic(topic)
            .Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        _ = await producer.Send("msg-0", _cts.Token);
        var publishersBefore = await GetPublishers(topic);
        var numberOfStates = states.Count;

        //Act
        var unload = await _fixture.ExecInPulsar($"bin/pulsar-admin topics unload {topic}", _cts.Token);
        unload.ExitCode.ShouldBe(0, unload.Stderr);
        await WaitUntilAsync(() => Task.FromResult(states.Skip(numberOfStates).Any(state => state == ProducerState.Disconnected)));
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        _ = await producer.Send("msg-1", _cts.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        publishersBefore.Count.ShouldBe(1);
        (await GetPublishers(topic)).ShouldBe(publishersBefore);
        (await ReceiveAll(topic)).ShouldBe(["msg-0", "msg-1"]);
    }

    // Additional coverage
    // Every partition of a topic has a producer of its own for the broker, with its own id and its own generated name, and when the connection is cut they all connect again as the same producers
    [Fact]
    public async Task Connectivity_GivenPartitionedTopicAndTheBrokerStillHasTheOldProducers_ShouldKeepTheProducerNameAndProducerIdOfEveryPartition()
    {
        //Arrange
        const int numberOfPartitions = 3;
        var topic = await CreatePartitionedTopicWithDeduplication(numberOfPartitions);
        await using var relay = await LossyRelay.Start(_fixture.ServiceUrl, _cts.Token);
        await using var client = CreateClient(relay.ServiceUrl);
        var states = new ConcurrentQueue<ProducerState>();
        await using var producer = client.NewProducer(Schema.String)
            .StateChangedHandler(change =>
            {
                states.Enqueue(change.ProducerState);
                _testOutputHelper.Log(change);
            })
            .Topic(topic)
            .Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);

        for (var i = 0; i < numberOfPartitions * 2; ++i)
        {
            _ = await producer.Send($"msg-{i}", _cts.Token);
        }

        var publishersBefore = await GetPublishersOfPartitions(topic, numberOfPartitions);
        var numberOfStates = states.Count;

        //Act
        var stopwatch = Stopwatch.StartNew();
        relay.DropClientConnections();
        await WaitUntilAsync(() => Task.FromResult(states.Skip(numberOfStates).Any(state => state is ProducerState.Disconnected or ProducerState.PartiallyConnected)));
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        stopwatch.Stop();
        _testOutputHelper.Log($"Every partition was connected again {stopwatch.Elapsed.TotalSeconds:F1} seconds after the connection was cut");
        _ = await producer.Send("msg-after", _cts.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(30), _cts.Token);

        //Assert
        publishersBefore.Select(publisher => publisher.Partition).ShouldBe([0, 1, 2], "the broker has one producer on every partition");
        publishersBefore.Select(publisher => publisher.Id).Distinct().Count().ShouldBe(numberOfPartitions, "every partition has its own producer id");
        publishersBefore.Select(publisher => publisher.Name).Distinct().Count().ShouldBe(numberOfPartitions, "every partition has its own generated name");
        (await GetPublishersOfPartitions(topic, numberOfPartitions)).ShouldBe(publishersBefore);
        stopwatch.Elapsed.ShouldBeLessThan(_keepAlive - TimeSpan.FromSeconds(10));
    }

    public void Dispose()
        => _cts.Dispose();

    private IPulsarClient CreateClient(Uri serviceUrl)
        => PulsarClient
        .Builder()
        .Authentication(_fixture.Authentication)
        .ExceptionHandler(_testOutputHelper.Log)
        .ServiceUrl(serviceUrl)
        .Build();

    private async Task<IProducer<string>> CreateProducer(IPulsarClient client, string topic, string? producerName, int maxPendingMessages)
    {
        var builder = client.NewProducer(Schema.String)
            .MaxPendingMessages((uint) maxPendingMessages * 2)
            .StateChangedHandler(_testOutputHelper.Log)
            .Topic(topic);

        if (producerName is not null)
            builder = builder.ProducerName(producerName);

        var producer = builder.Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        return producer;
    }

    // The producer.sendAsync of Java: the message is queued in the order of the calls, and the task completes when the broker has stored the message
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

    // Every message of the topic, read by a consumer that starts at the first message, until it has waited 5 seconds for one
    private async Task<List<string>> ReceiveAll(string topic)
    {
        await using var client = CreateClient(_fixture.ServiceUrl);
        await using var consumer = client.NewConsumer(Schema.String)
            .Topic(topic)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .StateChangedHandler(_testOutputHelper.Log)
            .SubscriptionName(_subscription)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var received = new List<string>();

        while (await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token) is { } message)
        {
            received.Add(message.Value());
        }

        return received;
    }

    private async Task<string> CreateTopicWithDeduplication()
    {
        var ns = await CreateNamespaceWithDeduplication();
        var topic = $"persistent://{ns}/{Guid.NewGuid():N}";
        await _fixture.CreateTopic(topic, _cts.Token);
        return topic;
    }

    private async Task<string> CreatePartitionedTopicWithDeduplication(int numberOfPartitions)
    {
        var ns = await CreateNamespaceWithDeduplication();
        var topic = $"persistent://{ns}/{Guid.NewGuid():N}";
        await _fixture.CreatePartitionedTopic(topic, numberOfPartitions, _cts.Token);
        return topic;
    }

    private async Task<string> CreateNamespaceWithDeduplication()
    {
        var ns = $"public/deduplication-{Guid.NewGuid():N}";
        await ExecInPulsar($"bin/pulsar-admin namespaces create {ns}");
        await ExecInPulsar($"bin/pulsar-admin namespaces set-deduplication {ns} --enable");
        return ns;
    }

    private async Task ExecInPulsar(string command)
    {
        var result = await _fixture.ExecInPulsar(command, _cts.Token);

        if (result.ExitCode != 0)
            throw new Exception($"The command '{command}' failed: {result.Stderr}");
    }

    private async Task<JsonDocument> GetStats(string topic, string kind = "stats")
    {
        using var admin = _fixture.CreateAdminClient();
        using var response = await admin.GetAsync($"/admin/v2/{topic.Replace("://", "/")}/{kind}", _cts.Token);
        _ = response.EnsureSuccessStatusCode();
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync(_cts.Token));
    }

    // The number of messages that the producers have published to the topic
    private async Task<long> GetMessagesPublished(string topic)
    {
        using var stats = await GetStats(topic);
        return stats.RootElement.GetProperty("msgInCounter").GetInt64();
    }

    // The producers that the broker has on the topic: the id that the producer has on its connection and the name
    private async Task<List<Publisher>> GetPublishers(string topic)
    {
        using var stats = await GetStats(topic);
        return ReadPublishers(stats.RootElement);
    }

    // The producer of every partition, in the order of the partitions. A partition that has more than one producer is in the list as many times.
    private async Task<List<PartitionPublisher>> GetPublishersOfPartitions(string topic, int numberOfPartitions)
    {
        var publishers = new List<PartitionPublisher>();

        for (var i = 0; i < numberOfPartitions; ++i)
        {
            publishers.AddRange((await GetPublishers($"{topic}-partition-{i}")).Select(publisher => new PartitionPublisher(i, publisher.Id, publisher.Name)));
        }

        return publishers;
    }

    private static List<Publisher> ReadPublishers(JsonElement stats)
        => stats.GetProperty("publishers").EnumerateArray().Select(publisher => new Publisher(publisher.GetProperty("producerId").GetInt64(), publisher.GetProperty("producerName").GetString()!)).ToList();

    private async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);

        while (!await condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), _cts.Token);
        }

        (await condition()).ShouldBeTrue();
    }

    private sealed record Publisher(long Id, string Name);

    private sealed record PartitionPublisher(int Partition, long Id, string Name);
}
