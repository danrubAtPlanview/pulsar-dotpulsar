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

namespace DotPulsar.Tests.Transactions;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using System.Text;
using System.Text.RegularExpressions;

// The tests of the Java class PendingAckPersistentTest that run on the broker of the TransactionFixture. cumulativePendingAckReplayTest needs a broker with other settings,
// and it is in TransactionPendingAckReplayTests. testTransactionConflictExceptionWhenAckBatchMessage is in TransactionConsumerTests.
// The Java methods testDeleteSubThenDeletePendingAckManagedLedger and testDeleteTopicThenDeletePendingAckManagedLedger are private, and the tests are ported regardless.
[Collection("Transaction"), Trait("Category", "Integration")]
public sealed class PendingAckPersistentTests : IDisposable
{
    private const string PendingAckStoreSuffix = "__transaction_pending_ack";

    private readonly CancellationTokenSource _cts;
    private readonly TransactionFixture _fixture;
    private readonly ITestOutputHelper _testOutputHelper;

    public PendingAckPersistentTests(TransactionFixture fixture, ITestOutputHelper outputHelper)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        _fixture = fixture;
        _testOutputHelper = outputHelper;
    }

    // Upstream: PendingAckPersistentTest#individualPendingAckReplayTest
    [Fact]
    public async Task Acknowledge_GivenIndividualPendingAcksAndTopicUnload_ShouldReplayPendingAckState()
    {
        //Arrange
        const int messageCount = 1000;
        const string subscription = "individual-test";
        var topic = TransactionFixture.CreateTopicName("pending-ack-replay");
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .EnableBatching(true)
            .BatchingMaxMessages(200)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .SubscriptionName(subscription)
            .SubscriptionType(SubscriptionType.Shared)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var abortTransaction = await NewTransaction(client, TimeSpan.FromSeconds(30));

        var pendingAckMessageIds = new List<MessageId>();
        var normalAckMessageIds = new List<MessageId>();
        for (var i = 0; i < messageCount; i++)
        {
            _ = await producer.Send(Encoding.UTF8.GetBytes("Hello Pulsar!"), _cts.Token);
            var message = await consumer.Receive(_cts.Token);

            if (i % 2 == 0)
            {
                await consumer.Acknowledge(message.MessageId, abortTransaction, _cts.Token);
                pendingAckMessageIds.Add(message.MessageId);
            }
            else
            {
                normalAckMessageIds.Add(message.MessageId);
            }
        }

        //in order to test pending ack replay
        await _fixture.UnloadTopic(topic, consumer, _cts.Token);
        var commitTransaction = await NewTransaction(client, TimeSpan.FromSeconds(30));
        var transaction = await NewTransaction(client, TimeSpan.FromSeconds(30));

        // this messageIds are ack by transaction
        for (var i = 0; i < pendingAckMessageIds.Count; i++)
        {
            await Should.ThrowAsync<TransactionConflictException>(async () => await consumer.Acknowledge(pendingAckMessageIds[i], transaction, _cts.Token));
        }

        // this messageIds are not ack by transaction
        for (var i = 0; i < normalAckMessageIds.Count; i++)
        {
            await consumer.Acknowledge(normalAckMessageIds[i], commitTransaction, _cts.Token);
        }

        await transaction.Abort(_cts.Token);
        // commit this txn, normalAckMessageIds are in pending ack state
        await commitTransaction.Commit(_cts.Token);
        // abort this txn, pendingAckMessageIds are delete from pending ack state
        await abortTransaction.Abort(_cts.Token);

        // replay this pending ack
        await _fixture.UnloadTopic(topic, consumer, _cts.Token);
        abortTransaction = await NewTransaction(client, TimeSpan.FromSeconds(30));
        commitTransaction = await NewTransaction(client, TimeSpan.FromSeconds(30));

        // normalAckMessageIds are ack and then commit, so ack fail
        for (var i = 0; i < normalAckMessageIds.Count; i++)
        {
            await Should.ThrowAsync<TransactionConflictException>(async () => await consumer.Acknowledge(normalAckMessageIds[i], abortTransaction, _cts.Token));
        }

        // pendingAckMessageIds are all abort, so can ack again
        for (var i = 0; i < pendingAckMessageIds.Count; i++)
        {
            await consumer.Acknowledge(pendingAckMessageIds[i], commitTransaction, _cts.Token);
        }

        await abortTransaction.Abort(_cts.Token);
        await commitTransaction.Commit(_cts.Token);

        //Act
        // in order to check out the pending ack cursor is clear whether or not.
        await Eventually(async () =>
        {
            var (markDeletePosition, lastConfirmedEntry) = await _fixture.GetPendingAckLogPositions(topic, subscription, _cts.Token);

            //Assert
            markDeletePosition.CompareTo(lastConfirmedEntry).ShouldBeLessThan(0);
        }, TimeSpan.FromSeconds(10));
    }

    // Upstream: PendingAckPersistentTest#testPendingAckMetrics
    // The Java test also checks the OpenTelemetry counters of the broker. The standalone broker has no OpenTelemetry reader, so only the Prometheus metrics are checked.
    [Fact]
    public async Task AcknowledgeCumulative_GivenCommittedAndAbortedTransactions_ShouldReportPendingAckMetrics()
    {
        //Arrange
        const int messageCount = 100;
        var subscription = "testMetric" + Guid.NewGuid();
        var topic = TransactionFixture.CreateTopicName("pending-ack-replay");
        var topicWithoutDomain = topic.Substring("persistent://".Length);
        await _fixture.CreateTopic(topic, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.String)
            .Topic(topic)
            .EnableBatching(true) // Upstream relies on the default of the Java client, which is batching
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await using var consumer = client.NewConsumer(Schema.String)
            .Topic(topic)
            .SubscriptionName(subscription)
            .SubscriptionType(SubscriptionType.Exclusive)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var a = 0; a < messageCount; a++)
        {
            _ = await producer.Send(Guid.NewGuid().ToString(), _cts.Token);
        }

        //Act
        for (var a = 0; a < messageCount; a++)
        {
            var message = await TransactionFixture.ReceiveOrNull(consumer, TimeSpan.FromSeconds(10), _cts.Token);

            if (message is null)
                break;

            var transaction = await NewTransaction(client, TimeSpan.FromSeconds(10));
            await consumer.AcknowledgeCumulative(message.MessageId, transaction, _cts.Token);

            if (a % 2 == 0)
                await transaction.Abort(_cts.Token);
            else
                await transaction.Commit(_cts.Token);
        }

        var metrics = await _fixture.GetPrometheusMetrics(_cts.Token);

        //Assert
        var commitLatency = metrics.Where(metric => metric.Name == "pulsar_txn_tp_commit_latency").ToList();
        var abortedCount = metrics.Where(metric => metric.Name == "pulsar_txn_tp_aborted_count_total");
        var committedCount = metrics.Where(metric => metric.Name == "pulsar_txn_tp_committed_count_total");
        commitLatency.Count.ShouldBeGreaterThan(0);
        commitLatency.Count(metric => metric.Labels["topic"].EndsWith(topicWithoutDomain) && metric.Value > 0).ShouldBeGreaterThan(0);

        foreach (var counts in new[] { abortedCount, committedCount })
        {
            var succeeded = counts.Where(metric => metric.Labels["subscription"] == subscription && metric.Labels["status"] == "succeed").ToList();
            succeeded.ShouldNotBeEmpty();

            foreach (var metric in succeeded)
            {
                metric.Labels["topic"].ShouldEndWith(topicWithoutDomain);
                metric.Value.ShouldBeGreaterThan(0);
            }
        }
    }

    // Upstream: PendingAckPersistentTest#testDeleteSubThenDeletePendingAckManagedLedger
    // The Java test only subscribes, and the broker creates the pending ack topic of a subscription when the first transactional acknowledgment arrives, so there is no pending ack
    // topic that the deletion could leave behind, and the system topics are left out of the listing besides. Here a message is acknowledged in a transaction first, so the pending ack
    // topic exists, and the topics are also listed with the system topics.
    [Fact]
    public async Task DeleteSubscription_GivenSubscription_ShouldNotLeavePendingAckTopic()
    {
        //Arrange
        const string subscription = "test-delete";
        var topic = TransactionFixture.CreateTopicName("test-delete");
        var pendingAckTopic = $"{topic}-{subscription}{PendingAckStoreSuffix}";
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        var consumer = client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .SubscriptionName(subscription)
            .SubscriptionType(SubscriptionType.Failover)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await AcknowledgeInTransaction(client, topic, consumer);
        await consumer.DisposeAsync();
        (await _fixture.GetNamespaceTopics(TransactionFixture.Namespace, true, _cts.Token)).ShouldContain(pendingAckTopic);

        //Act
        await _fixture.DeleteSubscription(topic, subscription, _cts.Token);
        var topics = await _fixture.GetNamespaceTopics(TransactionFixture.Namespace, false, _cts.Token);
        var topicsWithSystemTopics = await _fixture.GetNamespaceTopics(TransactionFixture.Namespace, true, _cts.Token);
        _ = await _fixture.GetTopicStats(topic, _cts.Token);

        //Assert
        topics.ShouldNotContain(pendingAckTopic);
        topicsWithSystemTopics.ShouldNotContain(pendingAckTopic);
        topics.ShouldContain(topic);
    }

    // Upstream: PendingAckPersistentTest#testDeleteTopicThenDeletePendingAckManagedLedger
    // See DeleteSubscription_GivenSubscription_ShouldNotLeavePendingAckTopic for why a message is acknowledged in a transaction first.
    [Fact]
    public async Task DeleteTopic_GivenSubscriptions_ShouldNotLeavePendingAckTopics()
    {
        //Arrange
        const string subscription1 = "test-delete";
        const string subscription2 = "test-delete";
        var topic = TransactionFixture.CreateTopicName("test-delete");
        var pendingAckTopic1 = $"{topic}-{subscription1}{PendingAckStoreSuffix}";
        var pendingAckTopic2 = $"{topic}-{subscription2}{PendingAckStoreSuffix}";
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        var consumer1 = client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .SubscriptionName(subscription1)
            .SubscriptionType(SubscriptionType.Failover)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer1.StateChangedTo(ConsumerState.Active, _cts.Token);
        await AcknowledgeInTransaction(client, topic, consumer1);
        await consumer1.DisposeAsync();
        var consumer2 = client.NewConsumer(Schema.ByteArray)
            .Topic(topic)
            .SubscriptionName(subscription2)
            .SubscriptionType(SubscriptionType.Failover)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer2.StateChangedTo(ConsumerState.Active, _cts.Token);
        await consumer2.DisposeAsync();
        (await _fixture.GetNamespaceTopics(TransactionFixture.Namespace, true, _cts.Token)).ShouldContain(pendingAckTopic1);

        //Act
        await _fixture.DeleteTopic(topic, false, _cts.Token);
        var topics = await _fixture.GetNamespaceTopics(TransactionFixture.Namespace, false, _cts.Token);
        var topicsWithSystemTopics = await _fixture.GetNamespaceTopics(TransactionFixture.Namespace, true, _cts.Token);

        //Assert
        topics.ShouldNotContain(pendingAckTopic1);
        topics.ShouldNotContain(pendingAckTopic2);
        topics.ShouldNotContain(topic);
        topicsWithSystemTopics.ShouldNotContain(pendingAckTopic1);
        topicsWithSystemTopics.ShouldNotContain(pendingAckTopic2);
        topicsWithSystemTopics.ShouldNotContain(topic);
    }

    // Upstream: PendingAckPersistentTest#testGetSubPatternTopicFilterTxnInternalTopic
    [Fact]
    public async Task Receive_GivenTopicsPatternAndPendingAckTopics_ShouldReceiveFromUserTopicsOnly()
    {
        //Arrange
        const int partitions = 3;
        // The namespace of the Java test is new for every test method, and this one is shared, so the pattern only matches the topics of this test
        var ns = $"{TransactionFixture.Tenant}/testGetSubPatternTopicFilterTxnInternalTopic-{Guid.NewGuid():N}";
        await _fixture.CreateNamespace(ns, 1, _cts.Token);
        var topic = $"persistent://{ns}/testGetSubPatternTopicFilterTxnInternalTopic";
        await _fixture.CreatePartitionedTopic(topic, partitions, _cts.Token);
        await using var client = _fixture.CreateClient(_testOutputHelper.Log);
        await using var producer = client.NewProducer(Schema.String)
            .Topic(topic)
            .EnableBatching(false)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        var consumer = client.NewConsumer(Schema.String)
            .SubscriptionName("sub")
            .SubscriptionType(SubscriptionType.Shared)
            .Topic(topic)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < partitions; i++)
        {
            _ = await producer.Send("test", _cts.Token);
        }

        // creat pending ack managedLedger
        for (var i = 0; i < partitions; i++)
        {
            var transaction = await NewTransaction(client, TimeSpan.FromSeconds(5));
            var message = await consumer.Receive(_cts.Token);
            var ack = consumer.Acknowledge(message.MessageId, transaction, _cts.Token).AsTask();
            await transaction.Commit(_cts.Token);
            await ack;
        }

        await consumer.DisposeAsync();

        //Act
        var topics = await _fixture.GetNamespaceTopics(ns, false, _cts.Token);

        //Assert
        topics.ShouldNotBeEmpty();
        topics.ShouldAllBe(name => !IsTransactionInternalName(name));

        //Arrange
        await using var patternConsumer = client.NewConsumer(Schema.String)
            .SubscriptionName("patternSub")
            .SubscriptionType(SubscriptionType.Shared)
            .TopicsPattern(new Regex($"persistent://{ns}/.*"))
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        await patternConsumer.StateChangedTo(ConsumerState.Active, _cts.Token);

        for (var i = 0; i < partitions; i++)
        {
            _ = await producer.Send("test" + i, _cts.Token);
        }

        //Act
        // can use pattern sub consume
        for (var i = 0; i < partitions; i++)
        {
            var message = await TransactionFixture.ReceiveOrNull(patternConsumer, TimeSpan.FromSeconds(10), _cts.Token);

            //Assert
            message.ShouldNotBeNull();
            message.Value().ShouldStartWith("test");
            await patternConsumer.Acknowledge(message, _cts.Token);
        }
    }

    public void Dispose()
        => _cts.Dispose();

    // Makes the broker create the pending ack topic of the subscription of the consumer
    private async Task AcknowledgeInTransaction(IPulsarClient client, string topic, IConsumer<byte[]> consumer)
    {
        await using var producer = client.NewProducer(Schema.ByteArray)
            .Topic(topic)
            .EnableBatching(false)
            .StateChangedHandler(_testOutputHelper.Log)
            .Create();
        _ = await producer.Send(Encoding.UTF8.GetBytes("test"), _cts.Token);
        var message = await consumer.Receive(_cts.Token);
        var transaction = await NewTransaction(client, TimeSpan.FromSeconds(10));
        var ack = consumer.Acknowledge(message.MessageId, transaction, _cts.Token).AsTask();
        await transaction.Commit(_cts.Token);
        await ack;
    }

    private async Task<ITransaction> NewTransaction(IPulsarClient client, TimeSpan timeout)
        => await client.NewTransaction().Timeout(timeout).Create(_cts.Token);

    // Java SystemTopicNames.isTransactionInternalName
    private static bool IsTransactionInternalName(string topic)
        => topic.EndsWith(PendingAckStoreSuffix) || topic.Contains("__transaction_log_") || topic.Contains("__transaction_buffer_snapshot");

    // Java Awaitility.await().untilAsserted(...): at most 10 seconds by default, polling every 100 milliseconds
    private async Task Eventually(Func<Task> assertion, TimeSpan atMost)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (true)
        {
            try
            {
                await assertion();
                return;
            }
            catch (ShouldAssertException) when (stopwatch.Elapsed < atMost)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), _cts.Token);
            }
        }
    }
}
