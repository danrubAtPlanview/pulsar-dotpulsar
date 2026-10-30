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
using DotPulsar.Internal.Abstractions;
using System.Text.Json;

// The Java test creates a new broker for every test method and uses one topic. These tests share a broker, so every test creates its own partitioned topic (with one partition,
// like upstream) in the namespace 'tnx/ns1', because the subscriptions of one test would give messages to the next test. The topic name is the only difference.
[Collection("AuthenticatedTransaction"), Trait("Category", "Integration")]
public sealed class AuthenticatedTransactionProducerConsumerTests : IDisposable
{
    private const string Subscription = "sub";

    private readonly CancellationTokenSource _cts;
    private readonly AuthenticatedTransactionFixture _fixture;

    public AuthenticatedTransactionProducerConsumerTests(AuthenticatedTransactionFixture fixture)
    {
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        _fixture = fixture;
    }

    // Upstream: AuthenticatedTransactionProducerConsumerTest#testEndTxn
    [Theory]
    [InlineData("client", true)]
    [InlineData("client", false)]
    [InlineData("client2", true)]
    [InlineData("client2", false)]
    [InlineData("admin", true)]
    [InlineData("admin", false)]
    public async Task Commit_GivenTransactionOfAnotherRole_ShouldOnlySucceedForOwnerOrSuperUser(string actor, bool afterUnload)
    {
        //Arrange
        var (topic, _) = await CreateTopic(partitions: 1);
        await using var owner = await CreateClient(AuthenticatedTransactionFixture.ClientRole);
        await using var other = await CreateClient(actor);
        var otherCoordinatorClient = await StartTransactionCoordinatorClient(other);
        var transaction = await owner.NewTransaction().Timeout(TimeSpan.FromSeconds(60)).Create(_cts.Token);
        await using var consumer = owner.NewConsumer(Schema.String).SubscriptionName("test").Topic(topic).Create();
        await using var producer = owner.NewProducer(Schema.String).SendTimeout(TimeSpan.FromSeconds(60)).Topic(topic).EnableBatching(true).Create(); // Upstream relies on the default of the Java client, which is batching
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        await producer.StateChangedTo(ProducerState.Connected, _cts.Token);
        _ = await producer.Send("beforetxn", _cts.Token);
        var messageBeforeTransaction = await TransactionFixtureBase.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token);
        messageBeforeTransaction.ShouldNotBeNull();
        var acknowledge = consumer.Acknowledge(messageBeforeTransaction.MessageId, transaction, _cts.Token).AsTask(); // Upstream does not wait for the acknowledgment
        _ = await producer.NewMessage().Transaction(transaction).Send("message", _cts.Token);

        if (afterUnload)
            await _fixture.UnloadNamespace(AuthenticatedTransactionFixture.SystemNamespace, _cts.Token);

        //Act
        var exception = await Record.ExceptionAsync(() => otherCoordinatorClient.Commit(transaction.Id, _cts.Token).AsTask());

        //Assert
        if (actor is AuthenticatedTransactionFixture.ClientRole or AuthenticatedTransactionFixture.AdminRole)
        {
            exception.ShouldBeNull();
            (await TransactionFixtureBase.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token))!.Value().ShouldBe("message");
        }
        else
        {
            exception.ShouldBeOfType<TransactionNotFoundException>();
            (await TransactionFixtureBase.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token)).ShouldBeNull();
            await transaction.Commit(_cts.Token);
            (await TransactionFixtureBase.ReceiveOrNull(consumer, TimeSpan.FromSeconds(5), _cts.Token))!.Value().ShouldBe("message");
        }

        _ = await Record.ExceptionAsync(() => acknowledge);
    }

    // Upstream: AuthenticatedTransactionProducerConsumerTest#testAddPartitionToTxn
    [Theory]
    [InlineData("client", true)]
    [InlineData("client", false)]
    [InlineData("client2", true)]
    [InlineData("client2", false)]
    [InlineData("admin", true)]
    [InlineData("admin", false)]
    public async Task AddPublishPartitionToTxn_GivenTransactionOfAnotherRole_ShouldOnlySucceedForOwnerOrSuperUser(string actor, bool afterUnload)
    {
        //Arrange
        var (_, topicName) = await CreateTopic(partitions: null);
        await using var owner = await CreateClient(AuthenticatedTransactionFixture.ClientRole);
        await using var other = await CreateClient(actor);
        var otherCoordinatorClient = await StartTransactionCoordinatorClient(other);
        var transaction = await owner.NewTransaction().Timeout(TimeSpan.FromSeconds(60)).Create(_cts.Token);

        if (afterUnload)
            await _fixture.UnloadNamespace(AuthenticatedTransactionFixture.SystemNamespace, _cts.Token);

        //Act
        var exception = await Record.ExceptionAsync(() => otherCoordinatorClient.AddPublishPartitionToTxn(transaction.Id, topicName, _cts.Token).AsTask());
        var metadata = await _fixture.GetTransactionMetadata(transaction.Id, _cts.Token);

        //Assert
        var producedPartitions = metadata.GetProperty("producedPartitions");

        if (actor is AuthenticatedTransactionFixture.ClientRole or AuthenticatedTransactionFixture.AdminRole)
        {
            exception.ShouldBeNull();
            GetPropertyNames(producedPartitions).ShouldBe([topicName]);
        }
        else
        {
            exception.ShouldBeOfType<TransactionNotFoundException>();
            GetPropertyNames(producedPartitions).ShouldBeEmpty();
        }
    }

    // Upstream: AuthenticatedTransactionProducerConsumerTest#testAddSubscriptionToTxn
    [Theory]
    [InlineData("client", true)]
    [InlineData("client", false)]
    [InlineData("client2", true)]
    [InlineData("client2", false)]
    [InlineData("admin", true)]
    [InlineData("admin", false)]
    public async Task AddSubscriptionToTxn_GivenTransactionOfAnotherRole_ShouldOnlySucceedForOwnerOrSuperUser(string actor, bool afterUnload)
    {
        //Arrange
        var (topic, topicName) = await CreateTopic(partitions: null);
        await using var owner = await CreateClient(AuthenticatedTransactionFixture.ClientRole);
        await using var other = await CreateClient(actor);
        var otherCoordinatorClient = await StartTransactionCoordinatorClient(other);
        await using var consumer = owner.NewConsumer(Schema.String).SubscriptionName(Subscription).Topic(topic).Create(); // The broker shows the subscriptions of a transaction only when they exist
        await consumer.StateChangedTo(ConsumerState.Active, _cts.Token);
        var transaction = await owner.NewTransaction().Timeout(TimeSpan.FromSeconds(60)).Create(_cts.Token);

        if (afterUnload)
            await _fixture.UnloadNamespace(AuthenticatedTransactionFixture.SystemNamespace, _cts.Token);

        //Act
        var exception = await Record.ExceptionAsync(() => otherCoordinatorClient.AddSubscriptionToTxn(transaction.Id, topicName, Subscription, _cts.Token).AsTask());
        var metadata = await _fixture.GetTransactionMetadata(transaction.Id, _cts.Token);

        //Assert
        var ackedPartitions = metadata.GetProperty("ackedPartitions");

        if (actor is AuthenticatedTransactionFixture.ClientRole or AuthenticatedTransactionFixture.AdminRole)
        {
            exception.ShouldBeNull();
            GetPropertyNames(ackedPartitions).Count.ShouldBe(1);
        }
        else
        {
            exception.ShouldBeOfType<TransactionNotFoundException>();
            GetPropertyNames(ackedPartitions).ShouldBeEmpty();
        }
    }

    // Upstream: AuthenticatedTransactionProducerConsumerTest#testNoAuth
    [Fact]
    public async Task Create_GivenClientWithoutAuthentication_ShouldThrowAuthenticationException()
    {
        //Arrange
        await using var client = _fixture.CreateClientWithoutAuthentication();

        //Act
        // Upstream fails when the client is built, because the Java client starts the transaction coordinator client then. DotPulsar starts it with the first transaction
        var exception = await Record.ExceptionAsync(() => client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(_cts.Token).AsTask());

        //Assert
        exception.ShouldNotBeNull();
        IsInChain<AuthenticationException>(exception).ShouldBeTrue(exception.ToString());
    }

    // The Java client starts its transaction coordinator client when the client is built
    private async Task<ITransactionCoordinatorClient> StartTransactionCoordinatorClient(IPulsarClient client)
    {
        var coordinatorClient = ((PulsarClient) client).TransactionCoordinatorClient!;
        await coordinatorClient.EnsureStarted(_cts.Token);
        return coordinatorClient;
    }

    private async Task<IPulsarClient> CreateClient(string role)
        => _fixture.CreateClientForRole(await _fixture.CreateToken(role, _cts.Token));

    // Upstream: AuthenticatedTransactionProducerConsumerTest#TOPIC. The short name ('tnx/ns1/txn-auth' upstream) is the name that the transaction coordinator gets
    private async Task<(string Topic, string TopicName)> CreateTopic(int? partitions)
    {
        var topicName = $"{TransactionFixtureBase.Namespace}/txn-auth-{Guid.NewGuid():N}";
        var topic = $"persistent://{topicName}";

        if (partitions is null)
            await _fixture.CreateTopic(topic, _cts.Token);
        else
            await _fixture.CreatePartitionedTopic(topic, partitions.Value, _cts.Token);

        return (topic, topicName);
    }

    private static List<string> GetPropertyNames(JsonElement element)
        => element.EnumerateObject().Select(property => property.Name).ToList();

    // Upstream: org.apache.pulsar.functions.utils.Exceptions#areExceptionsPresentInChain
    private static bool IsInChain<TException>(Exception? exception) where TException : Exception
    {
        for (; exception is not null; exception = exception.InnerException)
        {
            if (exception is TException)
                return true;
        }

        return false;
    }

    public void Dispose() => _cts.Dispose();
}
