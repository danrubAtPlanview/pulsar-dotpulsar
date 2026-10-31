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

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Transactions;
using DotPulsar.Tests.Internal.Transactions;

[Trait("Category", "Unit")]
public sealed class ProducerSendTimeoutTests
{
    private const string _topic = "persistent://public/default/test";
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _sendTimeout = TimeSpan.FromMilliseconds(300);

    // For the tests that check what was written before the timeout: a machine that is so busy that the dispatcher has not written the message within the send timeout lets the timeout remove the message unsent
    private static readonly TimeSpan _longSendTimeout = TimeSpan.FromSeconds(2);

    // Additional coverage
    [Fact]
    public async Task Send_GivenNoReceiptWithinSendTimeout_ShouldThrowOperationTimeoutException()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.HoldResponses();
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        var act = async () => await producer.Send(new MessageMetadata(), [1], Current.CancellationToken);

        //Assert
        await Should.ThrowAsync<OperationTimeoutException>(act);
        producer.State.IsFinalState().ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenEarlierSendTimedOut_ShouldKeepProducerConnectedAndIgnoreTheLateReceipt()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.HoldResponses();
        await using var producer = await CreateConnectedProducer(connection, sendTimeout: _longSendTimeout);
        _ = await Record.ExceptionAsync(async () => await producer.Send(new MessageMetadata(), [1], Current.CancellationToken));

        //Act
        connection.ReleaseResponses();
        var messageId = await producer.Send(new MessageMetadata(), [2], Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        messageId.ShouldNotBeNull();
        producer.State.IsFinalState().ShouldBeFalse();
        connection.Sent.Count.ShouldBe(2);
        connection.CommandProducers.ShouldHaveSingleItem("the producer did not reconnect");
    }

    // Additional coverage
    [Fact]
    public async Task Send_GivenBatchingAndNoReceiptWithinSendTimeout_ShouldFailEveryMessageOfTheBatch()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.HoldResponses();
        await using var producer = await CreateConnectedProducer(connection, enableBatching: true, sendTimeout: _longSendTimeout);

        //Act
        var sends = Enumerable.Range(0, 3).Select(i => producer.Send(new MessageMetadata(), [(byte) i], Current.CancellationToken).AsTask()).ToArray();

        //Assert
        foreach (var send in sends)
            await Should.ThrowAsync<OperationTimeoutException>(send.WaitAsync(_timeout, Current.CancellationToken));

        connection.Sent.ShouldHaveSingleItem().NumMessagesInBatch.ShouldBe(3);
    }

    // Additional coverage
    [Fact]
    public async Task Commit_GivenTransactionalSendThatTimedOut_ShouldAbortTheTransaction()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.HoldResponses();
        var tcClient = Substitute.For<ITransactionCoordinatorClient>();
        var transaction = new Transaction(tcClient, new TransactionId(0, 5), TimeSpan.FromMinutes(1));
        await using var producer = await CreateConnectedProducer(connection);

        //Act
        var sendException = await Record.ExceptionAsync(async () => await producer.Send(new MessageMetadata { Transaction = transaction }, [1], Current.CancellationToken));
        var commitException = await Record.ExceptionAsync(async () => await transaction.Commit(Current.CancellationToken));

        //Assert
        sendException.ShouldBeOfType<OperationTimeoutException>();
        commitException.ShouldBeOfType<TransactionHasOperationFailedException>();
        transaction.CurrentState.ShouldBe(TransactionState.Aborted);
    }

    // Additional coverage
    [Fact]
    public async Task SendChannel_GivenNoReceiptWithinSendTimeout_ShouldNotCallTheCallbackAndCompleteTheChannel()
    {
        //Arrange
        var connection = new FakeProducerConnection();
        connection.HoldResponses();
        await using var producer = await CreateConnectedProducer(connection);
        var called = false;

        //Act
        await producer.SendChannel.Send(new MessageMetadata(), [1], _ =>
        {
            called = true;
            return ValueTask.CompletedTask;
        }, Current.CancellationToken);
        await producer.SendChannel.Completion(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(200), Current.CancellationToken);

        //Assert
        called.ShouldBeFalse();
    }

    private static async Task<Producer<byte[]>> CreateConnectedProducer(FakeProducerConnection connection, bool enableBatching = false, TimeSpan? sendTimeout = null)
    {
        var options = new ProducerOptions<byte[]>(_topic, DotPulsar.Schema.ByteArray)
        {
            MaxPendingMessages = 500,
            SendTimeout = sendTimeout ?? _sendTimeout,
            EnableBatching = enableBatching,
            BatchingMaxMessages = 3,
            BatchingMaxPublishDelay = TimeSpan.FromHours(1)
        };

        var producer = new Producer<byte[]>(new Uri("pulsar://localhost:6650"), options, new DefaultExceptionHandler(), connection.Pool, null, true);
        await producer.State.OnStateChangeTo(ProducerState.Connected, Current.CancellationToken);
        return producer;
    }
}
