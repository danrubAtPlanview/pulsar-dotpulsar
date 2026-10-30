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
using DotPulsar.Internal.Transactions;

[Trait("Category", "Unit")]
public sealed class TransactionOperationTests
{
    private static readonly TransactionId _id = new(0, 5);

    [Fact]
    public async Task Commit_GivenBeginOpWithoutEndOp_ShouldWaitForEndOpBeforeEndTxn()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        uut.BeginOp();

        //Act
        var commit = uut.Commit(Current.CancellationToken).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var isCompletedBeforeEndOp = commit.IsCompleted;
        await tcClient.DidNotReceive().Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
        uut.EndOp(false);
        await commit.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Assert
        isCompletedBeforeEndOp.ShouldBeFalse();
        await tcClient.Received(1).Commit(_id, CancellationToken.None);
        uut.CurrentState.ShouldBe(TransactionState.Committed);
    }

    [Fact]
    public async Task Commit_GivenSeveralOps_ShouldWaitForTheLastEndOp()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        uut.BeginOp();
        uut.BeginOp();
        uut.BeginOp();
        var commit = uut.Commit(Current.CancellationToken).AsTask();

        //Act
        uut.EndOp(false);
        uut.EndOp(false);
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var isCompletedBeforeLastEndOp = commit.IsCompleted;
        uut.EndOp(false);
        await commit.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Assert
        isCompletedBeforeLastEndOp.ShouldBeFalse();
        uut.CurrentState.ShouldBe(TransactionState.Committed);
    }

    [Fact]
    public async Task Commit_GivenFailedOp_ShouldAbortAndThrowTransactionHasOperationFailedException()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        uut.BeginOp();
        uut.BeginOp();
        uut.EndOp(true);
        uut.EndOp(false);

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Commit(Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<TransactionHasOperationFailedException>();
        uut.CurrentState.ShouldBe(TransactionState.Aborted);
        await tcClient.DidNotReceive().Commit(Arg.Any<TransactionId>(), Arg.Any<CancellationToken>());
        await tcClient.Received(1).Abort(_id, CancellationToken.None);
    }

    [Fact]
    public async Task Abort_GivenBeginOpWithoutEndOp_ShouldWaitForEndOpBeforeEndTxn()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        uut.BeginOp();

        //Act
        var abort = uut.Abort(Current.CancellationToken).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var isCompletedBeforeEndOp = abort.IsCompleted;
        uut.EndOp(true);
        await abort.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Assert
        isCompletedBeforeEndOp.ShouldBeFalse();
        uut.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    [Fact]
    public async Task EndOp_GivenConcurrentOps_ShouldCompleteWhenAllHaveEnded()
    {
        //Arrange
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var uut = CreateTransaction(tcClient);
        const int numberOfOps = 1000;

        for (var i = 0; i < numberOfOps; ++i)
            uut.BeginOp();

        var commit = uut.Commit(Current.CancellationToken).AsTask();

        //Act
        await Task.WhenAll(Enumerable.Range(0, numberOfOps).Select(_ => Task.Run(() => uut.EndOp(false), Current.CancellationToken)));
        await commit.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Assert
        uut.CurrentState.ShouldBe(TransactionState.Committed);
    }

    private static Transaction CreateTransaction(ITransactionCoordinatorClient tcClient)
        => new(tcClient, _id, TimeSpan.FromMinutes(1));
}
