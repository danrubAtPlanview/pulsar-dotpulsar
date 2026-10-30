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

using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.PulsarApi;
using ProducerAccessMode = DotPulsar.Internal.PulsarApi.ProducerAccessMode;

[Trait("Category", "Unit")]
public sealed class ProducerChannelFactoryTests
{
    [Fact]
    public async Task Create_GivenTransactionsEnabled_ShouldSetTxnEnabled()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, true);

        //Act
        await uut.Create(Current.CancellationToken);

        //Assert
        var command = connection.CommandProducers.Single();
        command.HasTxnEnabled.ShouldBeTrue();
        command.TxnEnabled.ShouldBeTrue();
    }

    [Fact]
    public async Task Create_GivenTransactionsDisabled_ShouldNotSetTxnEnabled()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        var uut = CreateFactory(connection, false);

        //Act
        await uut.Create(Current.CancellationToken);

        //Assert
        connection.CommandProducers.Single().HasTxnEnabled.ShouldBeFalse();
    }

    private static ProducerChannelFactory CreateFactory(FakeProducerConnection connection, bool transactionsEnabled)
        => new(
            Guid.NewGuid(),
            Substitute.For<IRegisterEvent>(),
            connection.Pool,
            "persistent://public/default/test",
            null,
            ProducerAccessMode.Shared,
            DotPulsar.Schema.ByteArray.SchemaInfo,
            null,
            null,
            transactionsEnabled,
            null,
            null,
            ProducerCryptoFailureAction.Fail);

    // Upstream: ProducerBuilderImpl#initialSubscriptionName
    [Fact]
    public async Task Create_GivenInitialSubscriptionName_ShouldSetItOnCommandProducer()
    {
        //Arrange
        var commands = new List<CommandProducer>();
        var uut = CreateFactory("my-initial-subscription", commands);

        //Act
        await using var channel = await uut.Create(Current.CancellationToken);

        //Assert
        commands.Single().HasInitialSubscriptionName.ShouldBeTrue();
        commands.Single().InitialSubscriptionName.ShouldBe("my-initial-subscription");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task Create_GivenNoInitialSubscriptionName_ShouldNotSetItOnCommandProducer(string? initialSubscriptionName)
    {
        //Arrange
        var commands = new List<CommandProducer>();
        var uut = CreateFactory(initialSubscriptionName, commands);

        //Act
        await using var channel = await uut.Create(Current.CancellationToken);

        //Assert
        commands.Single().HasInitialSubscriptionName.ShouldBeFalse();
    }

    private static ProducerChannelFactory CreateFactory(string? initialSubscriptionName, List<CommandProducer> commands)
    {
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Do<CommandProducer>(command => commands.Add(command.Clone())), Arg.Any<IChannel>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(new ProducerResponse(1, "producer", 0)));
        var connectionPool = Substitute.For<IConnectionPool>();
        connectionPool.FindConnectionForTopic(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<IConnection>(connection));

        return new ProducerChannelFactory(
            Guid.NewGuid(),
            Substitute.For<IRegisterEvent>(),
            connectionPool,
            "persistent://public/default/test",
            null,
            ProducerAccessMode.Shared,
            DotPulsar.Schema.ByteArray.SchemaInfo,
            null,
            null,
            false,
            initialSubscriptionName,
            null,
            ProducerCryptoFailureAction.Fail);
    }
}
