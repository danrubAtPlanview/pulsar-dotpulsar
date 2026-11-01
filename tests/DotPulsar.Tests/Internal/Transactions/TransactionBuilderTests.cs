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

namespace DotPulsar.Tests.Internal.Transactions;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal;

[Trait("Category", "Unit")]
public sealed class TransactionBuilderTests
{
    // Upstream: TransactionBuilderTest#checkClientEnableTransactionConfiguration
    [Fact]
    public async Task Create_GivenClientWithTransactionsDisabled_ShouldThrowConfigurationException()
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(new Uri("pulsar://localhost:6650")).EnableTransaction(false).Build();

        //Act
        var exception = await Record.ExceptionAsync(() => client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("Transactions are not enabled");
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenClientWithTransactionsDisabled_ShouldNotThrowBeforeTheTaskIsAwaited()
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(new Uri("pulsar://localhost:6650")).Build();

        //Act
        var create = client.NewTransaction().Timeout(TimeSpan.FromMinutes(1)).Create(Current.CancellationToken);

        //Assert
        (await Record.ExceptionAsync(() => create.AsTask())).ShouldBeOfType<ConfigurationException>();
    }

    // Additional coverage
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-60000)]
    public async Task Create_GivenZeroOrNegativeTimeout_ShouldThrowConfigurationException(int milliseconds)
    {
        //Arrange
        var client = Substitute.For<IPulsarClient, IExtendedPulsarClient>();
        var extendedClient = (IExtendedPulsarClient) client;
        var uut = new TransactionBuilder(client).Timeout(TimeSpan.FromMilliseconds(milliseconds));

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Create(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("TransactionOptions.Timeout must be greater than zero");
        await extendedClient.DidNotReceive().CreateTransaction(Arg.Any<TransactionOptions>(), Arg.Any<CancellationToken>());
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenInfiniteTimeout_ShouldThrowConfigurationException()
    {
        //Arrange
        var client = Substitute.For<IPulsarClient, IExtendedPulsarClient>();
        var extendedClient = (IExtendedPulsarClient) client;
        var uut = new TransactionBuilder(client).Timeout(System.Threading.Timeout.InfiniteTimeSpan);

        //Act
        var exception = await Record.ExceptionAsync(() => uut.Create(Current.CancellationToken).AsTask());

        //Assert
        exception.ShouldBeOfType<ConfigurationException>();
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenDefaultTimeout_ShouldCreateTransactionWithOneMinute()
    {
        //Arrange
        var client = Substitute.For<IPulsarClient, IExtendedPulsarClient>();
        var extendedClient = (IExtendedPulsarClient) client;
        extendedClient.CreateTransaction(Arg.Any<TransactionOptions>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<ITransaction>(Substitute.For<ITransaction>()));
        var uut = new TransactionBuilder(client);

        //Act
        await uut.Create(Current.CancellationToken);

        //Assert
        await extendedClient.Received(1).CreateTransaction(Arg.Is<TransactionOptions>(options => options.Timeout == TimeSpan.FromMinutes(1)), Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenTimeout_ShouldCreateTransactionWithTheTimeout()
    {
        //Arrange
        var client = Substitute.For<IPulsarClient, IExtendedPulsarClient>();
        var extendedClient = (IExtendedPulsarClient) client;
        extendedClient.CreateTransaction(Arg.Any<TransactionOptions>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<ITransaction>(Substitute.For<ITransaction>()));
        var uut = new TransactionBuilder(client).Timeout(TimeSpan.FromMilliseconds(200));

        //Act
        await uut.Create(Current.CancellationToken);

        //Assert
        await extendedClient.Received(1).CreateTransaction(Arg.Is<TransactionOptions>(options => options.Timeout == TimeSpan.FromMilliseconds(200)), Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public async Task Create_ShouldReturnTheTransactionOfTheClient()
    {
        //Arrange
        var transaction = Substitute.For<ITransaction>();
        var client = Substitute.For<IPulsarClient, IExtendedPulsarClient>();
        var extendedClient = (IExtendedPulsarClient) client;
        extendedClient.CreateTransaction(Arg.Any<TransactionOptions>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<ITransaction>(transaction));
        var uut = new TransactionBuilder(client);

        //Act
        var result = await uut.Create(Current.CancellationToken);

        //Assert
        result.ShouldBeSameAs(transaction);
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenClientThatFails_ShouldThrowTheException()
    {
        //Arrange
        var exception = new TransactionCoordinatorClientException("not available");
        var client = Substitute.For<IPulsarClient, IExtendedPulsarClient>();
        var extendedClient = (IExtendedPulsarClient) client;
        extendedClient.CreateTransaction(Arg.Any<TransactionOptions>(), Arg.Any<CancellationToken>()).Returns(_ => ValueTask.FromException<ITransaction>(exception));
        var uut = new TransactionBuilder(client);

        //Act
        var result = await Record.ExceptionAsync(() => uut.Create(Current.CancellationToken).AsTask());

        //Assert
        result.ShouldBeSameAs(exception);
    }

    // Additional coverage
    [Fact]
    public void Timeout_ShouldReturnTheSameBuilder()
    {
        //Arrange
        var uut = new TransactionBuilder(Substitute.For<IPulsarClient>());

        //Act
        var result = uut.Timeout(TimeSpan.FromSeconds(5));

        //Assert
        result.ShouldBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public async Task NewTransaction_ShouldReturnBuilderThatCreatesTransactionsWithTheClient()
    {
        //Arrange
        var client = Substitute.For<IPulsarClient, IExtendedPulsarClient>();
        var extendedClient = (IExtendedPulsarClient) client;
        extendedClient.CreateTransaction(Arg.Any<TransactionOptions>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<ITransaction>(Substitute.For<ITransaction>()));

        //Act
        var builder = client.NewTransaction();
        await builder.Timeout(TimeSpan.FromSeconds(30)).Create(Current.CancellationToken);

        //Assert
        builder.ShouldBeOfType<TransactionBuilder>();
        await extendedClient.Received(1).CreateTransaction(Arg.Is<TransactionOptions>(options => options.Timeout == TimeSpan.FromSeconds(30)), Current.CancellationToken);
    }
}
