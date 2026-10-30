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

using DotPulsar.Exceptions;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Exceptions;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Internal.Transactions;

[Trait("Category", "Unit")]
public sealed class TransactionMetaStoreOpTests
{
    [Fact]
    public void TryBeginSend_GivenSameConnectionTwice_ShouldReturnFalseTheSecondTime()
    {
        //Arrange
        var connection = Substitute.For<IConnection>();
        var uut = CreateOp();

        //Act
        var first = uut.TryBeginSend(connection);
        var second = uut.TryBeginSend(connection);

        //Assert
        first.ShouldBeTrue();
        second.ShouldBeFalse();
    }

    [Fact]
    public void TryBeginSend_GivenCompletedOperation_ShouldReturnFalse()
    {
        //Arrange
        var uut = CreateOp();
        _ = uut.TryClaimCompletion();

        //Act
        var result = uut.TryBeginSend(Substitute.For<IConnection>());

        //Assert
        result.ShouldBeFalse();
    }

    // Additional coverage
    // The response claims the completion while its own send still holds the connection. A resend that takes the connection after the send has ended (the reconnect resends
    // the operations that it found pending before the response completed this one) must not send it
    [Fact]
    public void TryBeginSend_GivenOperationCompletedByTheResponseOfTheSendThatHasEnded_ShouldReturnFalse()
    {
        //Arrange
        var connection = Substitute.For<IConnection>();
        var uut = CreateOp();
        _ = uut.TryBeginSend(connection);
        _ = uut.TryClaimCompletion();
        uut.EndSend(connection);

        //Act
        var result = uut.TryBeginSend(connection);

        //Assert
        result.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void TryBeginSend_GivenAnotherConnection_ShouldReturnTrue()
    {
        //Arrange
        var uut = CreateOp();
        _ = uut.TryBeginSend(Substitute.For<IConnection>());

        //Act
        var result = uut.TryBeginSend(Substitute.For<IConnection>());

        //Assert
        result.ShouldBeTrue();
    }

    [Fact]
    public void TryBeginSend_GivenSendThatHasEnded_ShouldReturnTrue()
    {
        //Arrange
        var connection = Substitute.For<IConnection>();
        var uut = CreateOp();
        _ = uut.TryBeginSend(connection);
        uut.EndSend(connection);

        //Act
        var result = uut.TryBeginSend(connection);

        //Assert
        result.ShouldBeTrue();
    }

    [Fact]
    public void EndSend_GivenAnotherConnection_ShouldNotEndTheSend()
    {
        //Arrange
        var connection = Substitute.For<IConnection>();
        var uut = CreateOp();
        _ = uut.TryBeginSend(connection);

        //Act
        uut.EndSend(Substitute.For<IConnection>());

        //Assert
        uut.TryBeginSend(connection).ShouldBeFalse();
    }

    [Fact]
    public void TryScheduleRetry_GivenRetryAlreadyScheduled_ShouldReturnFalseUntilRetryElapsed()
    {
        //Arrange
        var uut = CreateOp();

        //Act
        var first = uut.TryScheduleRetry();
        var second = uut.TryScheduleRetry();
        uut.RetryElapsed();
        var third = uut.TryScheduleRetry();

        //Assert
        first.ShouldBeTrue();
        second.ShouldBeFalse();
        third.ShouldBeTrue();
    }

    [Fact]
    public void NextRetryDelay_ShouldDoubleFromHundredMillisecondsToSixSeconds()
    {
        //Arrange
        var uut = CreateOp();
        var expected = new[] { 100, 200, 400, 800, 1600, 3200, 6000, 6000 };

        //Act
        var delays = expected.Select(_ => uut.NextRetryDelay().TotalMilliseconds).ToArray();

        //Assert
        for (var i = 0; i < expected.Length; ++i)
        {
            delays[i].ShouldBeGreaterThanOrEqualTo(expected[i] * 0.94);
            delays[i].ShouldBeLessThanOrEqualTo(expected[i] * 1.06);
        }
    }

    [Fact]
    public void TryClaimCompletion_ShouldSucceedOnce()
    {
        //Arrange
        var uut = CreateOp();

        //Act
        var first = uut.TryClaimCompletion();
        var second = uut.TryClaimCompletion();

        //Assert
        first.ShouldBeTrue();
        second.ShouldBeFalse();
    }

    [Fact]
    public async Task SetResult_ShouldCompleteTheOperationAndCancelItsToken()
    {
        //Arrange
        var uut = CreateOp();
        var response = TransactionResponses.EndTxn();

        //Act
        uut.SetResult(response);

        //Assert
        (await uut.Completion).ShouldBeSameAs(response);
        uut.CancellationToken.IsCancellationRequested.ShouldBeTrue();
    }

    [Fact]
    public async Task SetException_ShouldFaultTheOperationAndCancelItsToken()
    {
        //Arrange
        var uut = CreateOp();
        var exception = new TransactionNotFoundException("not found");

        //Act
        uut.SetException(exception);

        //Assert
        (await Record.ExceptionAsync(() => uut.Completion)).ShouldBeSameAs(exception);
        uut.CancellationToken.IsCancellationRequested.ShouldBeTrue();
    }

    [Fact]
    public async Task Send_GivenEachCommand_ShouldSendItOnTheMatchingOverload()
    {
        //Arrange
        var connection = Substitute.For<IConnection>();
        var newTxn = new CommandNewTxn();
        var addPartition = new CommandAddPartitionToTxn();
        var addSubscription = new CommandAddSubscriptionToTxn();
        var endTxn = new CommandEndTxn();
        using var cts = new CancellationTokenSource();

        //Act
        _ = TransactionMetaStoreOp.ForNewTxn(newTxn, "new").Send(connection, cts.Token);
        _ = TransactionMetaStoreOp.ForAddPartitionToTxn(addPartition, "partition").Send(connection, cts.Token);
        _ = TransactionMetaStoreOp.ForAddSubscriptionToTxn(addSubscription, "subscription").Send(connection, cts.Token);
        _ = TransactionMetaStoreOp.ForEndTxn(endTxn, "end").Send(connection, cts.Token);

        //Assert
        await connection.Received(1).Send(newTxn, cts.Token);
        await connection.Received(1).Send(addPartition, cts.Token);
        await connection.Received(1).Send(addSubscription, cts.Token);
        await connection.Received(1).Send(endTxn, cts.Token);
    }

    [Fact]
    public void TryGetError_GivenResponseWithError_ShouldReturnErrorAndMessage()
    {
        //Arrange
        var operations = new[]
        {
            (TransactionMetaStoreOp.ForNewTxn(new CommandNewTxn(), "new"), TransactionResponses.NewTxn(ServerError.TransactionNotFound, "message")),
            (TransactionMetaStoreOp.ForAddPartitionToTxn(new CommandAddPartitionToTxn(), "partition"), TransactionResponses.AddPartitionToTxn(ServerError.TransactionNotFound, "message")),
            (TransactionMetaStoreOp.ForAddSubscriptionToTxn(new CommandAddSubscriptionToTxn(), "subscription"), TransactionResponses.AddSubscriptionToTxn(ServerError.TransactionNotFound, "message")),
            (TransactionMetaStoreOp.ForEndTxn(new CommandEndTxn(), "end"), TransactionResponses.EndTxn(ServerError.TransactionNotFound, "message"))
        };

        foreach (var (uut, response) in operations)
        {
            //Act
            var hasError = uut.TryGetError(response, out var error, out var message);

            //Assert
            hasError.ShouldBeTrue();
            error.ShouldBe(ServerError.TransactionNotFound);
            message.ShouldBe("message");
        }
    }

    [Fact]
    public void TryGetError_GivenResponseWithoutError_ShouldReturnFalse()
    {
        //Arrange
        var operations = new[]
        {
            (TransactionMetaStoreOp.ForNewTxn(new CommandNewTxn(), "new"), TransactionResponses.NewTxn(1, 2)),
            (TransactionMetaStoreOp.ForAddPartitionToTxn(new CommandAddPartitionToTxn(), "partition"), TransactionResponses.AddPartitionToTxn()),
            (TransactionMetaStoreOp.ForAddSubscriptionToTxn(new CommandAddSubscriptionToTxn(), "subscription"), TransactionResponses.AddSubscriptionToTxn()),
            (TransactionMetaStoreOp.ForEndTxn(new CommandEndTxn(), "end"), TransactionResponses.EndTxn())
        };

        foreach (var (uut, response) in operations)
        {
            //Act
            var hasError = uut.TryGetError(response, out _, out _);

            //Assert
            hasError.ShouldBeFalse();
        }
    }

    [Fact]
    public void TryGetError_GivenResponseOfAnotherType_ShouldThrowUnexpectedResponseException()
    {
        //Arrange
        var uut = TransactionMetaStoreOp.ForNewTxn(new CommandNewTxn(), "new");

        //Act
        var exception = Record.Exception(() => uut.TryGetError(TransactionResponses.TcClientConnect(), out _, out _));

        //Assert
        exception.ShouldBeOfType<UnexpectedResponseException>();
    }

    [Fact]
    public void ResponseType_GivenEachOperation_ShouldBeTheTypeOfTheMatchingResponse()
    {
        //Act
        var newTxn = TransactionMetaStoreOp.ForNewTxn(new CommandNewTxn(), "new");
        var addPartition = TransactionMetaStoreOp.ForAddPartitionToTxn(new CommandAddPartitionToTxn(), "partition");
        var addSubscription = TransactionMetaStoreOp.ForAddSubscriptionToTxn(new CommandAddSubscriptionToTxn(), "subscription");
        var endTxn = TransactionMetaStoreOp.ForEndTxn(new CommandEndTxn(), "end");

        //Assert
        newTxn.ResponseType.ShouldBe(BaseCommand.Types.Type.NewTxnResponse);
        addPartition.ResponseType.ShouldBe(BaseCommand.Types.Type.AddPartitionToTxnResponse);
        addSubscription.ResponseType.ShouldBe(BaseCommand.Types.Type.AddSubscriptionToTxnResponse);
        endTxn.ResponseType.ShouldBe(BaseCommand.Types.Type.EndTxnResponse);
        newTxn.Description.ShouldBe("new");
    }

    private static TransactionMetaStoreOp CreateOp()
        => TransactionMetaStoreOp.ForEndTxn(new CommandEndTxn(), "End [COMMIT] TXN (0,0)");
}
