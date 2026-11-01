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
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Transactions;

[Trait("Category", "Unit")]
public sealed class RequestResponseHandlerTests
{
    // Additional coverage
    [Fact]
    public void OutgoingWithResponse_GivenCommandAck_ShouldAssignIncreasingRequestIds()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var first = new CommandAck();
        var second = new CommandAck();

        //Act
        _ = uut.OutgoingWithResponse(first);
        _ = uut.OutgoingWithResponse(second);

        //Assert
        second.RequestId.ShouldBeGreaterThan(first.RequestId);
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenAckResponseWithMatchingRequestId_ShouldCompleteRequestWithMatchingRequestId()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var other = uut.OutgoingWithResponse(new CommandAck());
        var command = new CommandAck();
        var task = uut.OutgoingWithResponse(command);
        var response = CreateAckResponse(command.RequestId);

        //Act
        uut.Incoming(response);

        //Assert
        (await task).ShouldBeSameAs(response);
        other.IsCompleted.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Incoming_GivenAckResponseWithUnknownRequestId_ShouldNotThrow()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var task = uut.OutgoingWithResponse(new CommandAck());

        //Act
        var exception = Record.Exception(() => uut.Incoming(CreateAckResponse(1000)));

        //Assert
        exception.ShouldBeNull();
        task.IsCompleted.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Incoming_GivenAckResponseWithoutRequestId_ShouldNotThrow()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var task = uut.OutgoingWithResponse(new CommandAck());
        var response = new BaseCommand { Type = BaseCommand.Types.Type.AckResponse, AckResponse = new CommandAckResponse { ConsumerId = 1 } };

        //Act
        var exception = Record.Exception(() => uut.Incoming(response));

        //Assert
        exception.ShouldBeNull();
        task.IsCompleted.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Incoming_GivenCloseConsumer_ShouldNotCancelPendingAckRequests()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var task = uut.OutgoingWithResponse(new CommandAck { ConsumerId = 1 });

        //Act
        uut.Incoming(new CommandCloseConsumer { ConsumerId = 1 });

        //Assert
        task.IsCompleted.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenAckResponseWithConsumerNotFoundAfterCloseConsumer_ShouldCompleteTaskWithErrorResponse()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var command = new CommandAck { ConsumerId = 1 };
        var task = uut.OutgoingWithResponse(command);
        uut.Incoming(new CommandCloseConsumer { ConsumerId = 1 });
        var response = CreateAckResponse(command.RequestId, ServerError.ConsumerNotFound);

        //Act
        uut.Incoming(response);

        //Assert
        var actual = await task;
        actual.AckResponse.HasError.ShouldBeTrue();
        actual.AckResponse.Error.ShouldBe(ServerError.ConsumerNotFound);
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenNewTxnResponse_ShouldCompleteRequestWithMatchingRequestId()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var command = new CommandNewTxn();
        var task = uut.Outgoing(command);
        var response = new BaseCommand { Type = BaseCommand.Types.Type.NewTxnResponse, NewTxnResponse = new CommandNewTxnResponse { RequestId = command.RequestId } };

        //Act
        uut.Incoming(response);

        //Assert
        (await task).ShouldBeSameAs(response);
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenAddPartitionToTxnResponse_ShouldCompleteRequestWithMatchingRequestId()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var command = new CommandAddPartitionToTxn();
        var task = uut.Outgoing(command);
        var response = new BaseCommand { Type = BaseCommand.Types.Type.AddPartitionToTxnResponse, AddPartitionToTxnResponse = new CommandAddPartitionToTxnResponse { RequestId = command.RequestId } };

        //Act
        uut.Incoming(response);

        //Assert
        (await task).ShouldBeSameAs(response);
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenAddSubscriptionToTxnResponse_ShouldCompleteRequestWithMatchingRequestId()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var command = new CommandAddSubscriptionToTxn();
        var task = uut.Outgoing(command);
        var response = new BaseCommand { Type = BaseCommand.Types.Type.AddSubscriptionToTxnResponse, AddSubscriptionToTxnResponse = new CommandAddSubscriptionToTxnResponse { RequestId = command.RequestId } };

        //Act
        uut.Incoming(response);

        //Assert
        (await task).ShouldBeSameAs(response);
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenEndTxnResponse_ShouldCompleteRequestWithMatchingRequestId()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var command = new CommandEndTxn();
        var task = uut.Outgoing(command);
        var response = new BaseCommand { Type = BaseCommand.Types.Type.EndTxnResponse, EndTxnResponse = new CommandEndTxnResponse { RequestId = command.RequestId } };

        //Act
        uut.Incoming(response);

        //Assert
        (await task).ShouldBeSameAs(response);
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenTcClientConnectResponse_ShouldCompleteRequestWithMatchingRequestId()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var command = new CommandTcClientConnectRequest();
        var task = uut.Outgoing(command);
        var response = new BaseCommand { Type = BaseCommand.Types.Type.TcClientConnectResponse, TcClientConnectResponse = new CommandTcClientConnectResponse { RequestId = command.RequestId } };

        //Act
        uut.Incoming(response);

        //Assert
        (await task).ShouldBeSameAs(response);
    }

    // Additional coverage
    [Fact]
    public void Incoming_GivenUnregisteredCommandType_ShouldIgnoreIt()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var task = uut.OutgoingWithResponse(new CommandAck());
        var command = new BaseCommand { Type = BaseCommand.Types.Type.TopicMigrated, TopicMigrated = new CommandTopicMigrated() };

        //Act
        var exception = Record.Exception(() => uut.Incoming(command));

        //Assert
        exception.ShouldBeNull();
        task.IsCompleted.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Cancel_GivenPendingRequest_ShouldCancelOnlyThatRequest()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var command = new CommandAck();
        var task = uut.OutgoingWithResponse(command);
        var other = uut.OutgoingWithResponse(new CommandAck());

        //Act
        uut.Cancel(command.RequestId);

        //Assert
        task.IsCanceled.ShouldBeTrue();
        other.IsCompleted.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenBatchCommandSend_ShouldCompleteOnSendErrorWithHighestSequenceId()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var tcs = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, HighestSequenceId = 9 }, tcs);
        var response = CreateSendError(3, 9);

        //Act
        uut.Incoming(response);

        //Assert
        (await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken)).ShouldBeSameAs(response);
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenBatchCommandSend_ShouldCompleteOnReceiptWithSequenceId()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var tcs = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, HighestSequenceId = 9 }, tcs);
        var response = CreateSendReceipt(3, 5);

        //Act
        uut.Incoming(response);

        //Assert
        (await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken)).ShouldBeSameAs(response);
    }

    // Additional coverage
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Outgoing_GivenBatchCommandSend_ShouldRemoveBothRequestsWhenOneIsCompleted(bool completeWithReceipt)
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var tcs = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, HighestSequenceId = 9 }, tcs);
        var numberOfRequestsBefore = GetNumberOfRequests(uut);

        //Act
        uut.Incoming(completeWithReceipt ? CreateSendReceipt(3, 5) : CreateSendError(3, 9));
        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Assert
        numberOfRequestsBefore.ShouldBe(2);
        await WaitFor.Until(() => GetNumberOfRequests(uut) == 0, "both requests are removed");
    }

    // Additional coverage
    [Fact]
    public async Task Outgoing_GivenBatchCommandSendThatIsCancelled_ShouldRemoveBothRequests()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var tcs = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, HighestSequenceId = 9 }, tcs);

        //Act
        tcs.SetCanceled(Current.CancellationToken);

        //Assert
        await WaitFor.Until(() => GetNumberOfRequests(uut) == 0, "both requests are removed");
    }

    // Additional coverage
    [Theory]
    [InlineData(false, 0UL)]
    [InlineData(true, 5UL)]
    public void Outgoing_GivenCommandSendWithoutADifferentHighestSequenceId_ShouldRegisterOneRequest(bool hasHighestSequenceId, ulong highestSequenceId)
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var command = new CommandSend { ProducerId = 3, SequenceId = 5 };
        if (hasHighestSequenceId)
            command.HighestSequenceId = highestSequenceId;

        //Act
        uut.Outgoing(command, new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously));

        //Assert
        GetNumberOfRequests(uut).ShouldBe(1);
    }

    // Upstream: ProducerImpl#ackReceived
    // The chunks of a message are sent with the same sequence id, and the broker answers them in the order they were sent
    [Fact]
    public async Task Incoming_GivenReceiptsForChunksWithTheSameSequenceId_ShouldCompleteTheChunksInTheOrderTheyWereSent()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var chunks = Enumerable.Range(0, 3).Select(_ => new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        var receipts = Enumerable.Range(0, 3).Select(_ => CreateSendReceipt(3, 5)).ToArray();
        foreach (var chunk in chunks)
        {
            uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, IsChunk = true }, chunk);
        }

        //Act
        uut.Incoming(receipts[0]);
        var firstCompleted = chunks.Select(chunk => chunk.Task.IsCompleted).ToArray();
        uut.Incoming(receipts[1]);
        uut.Incoming(receipts[2]);

        //Assert
        firstCompleted.ShouldBe([true, false, false]);
        for (var i = 0; i < 3; ++i)
        {
            (await chunks[i].Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken)).ShouldBeSameAs(receipts[i]);
        }
    }

    // Additional coverage
    [Fact]
    public void Outgoing_GivenSecondChunkWithTheSameSequenceId_ShouldNotCancelTheFirstChunk()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var first = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, IsChunk = true }, first);

        //Act
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, IsChunk = true }, new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously));

        //Assert
        first.Task.IsCompleted.ShouldBeFalse();
        GetNumberOfRequests(uut).ShouldBe(0, "the chunks are not registered with the requests that are replaced by a request with the same key");
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenSendErrorForChunk_ShouldCompleteTheOldestChunk()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var first = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, IsChunk = true }, first);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, IsChunk = true }, second);
        var response = CreateSendError(3, 5);

        //Act
        uut.Incoming(response);

        //Assert
        (await first.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken)).ShouldBeSameAs(response);
        second.Task.IsCompleted.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenReceiptForAnotherSequenceIdOrProducer_ShouldNotCompleteTheChunks()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var chunk = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        var plain = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, IsChunk = true }, chunk);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 6 }, plain);

        //Act
        uut.Incoming(CreateSendReceipt(3, 6));
        uut.Incoming(CreateSendReceipt(4, 5));

        //Assert
        await plain.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);
        chunk.Task.IsCompleted.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenMoreReceiptsThanChunks_ShouldIgnoreTheOthers()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var chunk = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, IsChunk = true }, chunk);
        uut.Incoming(CreateSendReceipt(3, 5));
        await chunk.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);

        //Act
        var exception = Record.Exception(() => uut.Incoming(CreateSendReceipt(3, 5)));

        //Assert
        exception.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public async Task Incoming_GivenCloseProducer_ShouldCancelTheChunksOfThatProducerOnly()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var closed = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        var other = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, IsChunk = true }, closed);
        uut.Outgoing(new CommandSend { ProducerId = 4, SequenceId = 5, IsChunk = true }, other);

        //Act
        uut.Incoming(new CommandCloseProducer { ProducerId = 3 });

        //Assert
        await Should.ThrowAsync<TaskCanceledException>(closed.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken));
        other.Task.IsCompleted.ShouldBeFalse();
        uut.Incoming(CreateSendReceipt(4, 5));
        await other.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);
    }

    // Additional coverage
    [Fact]
    public async Task Dispose_GivenPendingChunks_ShouldCancelThem()
    {
        //Arrange
        var uut = new RequestResponseHandler();
        var chunk = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, IsChunk = true }, chunk);

        //Act
        uut.Dispose();

        //Assert
        await Should.ThrowAsync<TaskCanceledException>(chunk.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken));
    }

    // Additional coverage (followup 0011 item 5)
    // The chunks are matched to the answers of the broker by their order, so a request whose answer never comes shifts every later answer by one: the next message of a producer that has the same id and the same sequence id
    // would never get the answer to its last chunk. A producer has the same id on every connection, so a command that registers the producer is the moment when what is left of an earlier channel is known to be stale.
    [Fact]
    public async Task Outgoing_GivenCommandProducerForProducerWithPendingChunks_ShouldCancelThemSoTheyCanNotTakeAnAnswerOfTheNextChannel()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var stale = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        var live = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, IsChunk = true }, stale);
        var receipt = CreateSendReceipt(3, 5);

        //Act
        _ = uut.Outgoing(new CommandProducer { ProducerId = 3, Topic = "persistent://public/default/test" });
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, IsChunk = true }, live);
        uut.Incoming(receipt);

        //Assert
        stale.Task.IsCanceled.ShouldBeTrue();
        (await live.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken)).ShouldBeSameAs(receipt);
    }

    // Additional coverage (followup 0011 item 5)
    [Fact]
    public async Task Outgoing_GivenCommandProducer_ShouldKeepTheChunksOfOtherProducers()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var other = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 4, SequenceId = 5, IsChunk = true }, other);

        //Act
        _ = uut.Outgoing(new CommandProducer { ProducerId = 3, Topic = "persistent://public/default/test" });
        uut.Incoming(CreateSendReceipt(4, 5));

        //Assert
        other.Task.IsCanceled.ShouldBeFalse();
        await other.Task.WaitAsync(TimeSpan.FromSeconds(10), Current.CancellationToken);
    }

    private static int GetNumberOfRequests(RequestResponseHandler handler)
    {
        var field = typeof(RequestResponseHandler).GetField("_requests", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        var requests = (Awaiter<DotPulsar.Internal.Abstractions.IRequest, BaseCommand>) field.GetValue(handler)!;
        return requests.Keys.Count();
    }

    private static BaseCommand CreateSendReceipt(ulong producerId, ulong sequenceId)
        => new() { Type = BaseCommand.Types.Type.SendReceipt, SendReceipt = new CommandSendReceipt { ProducerId = producerId, SequenceId = sequenceId, MessageId = new MessageIdData() } };

    private static BaseCommand CreateSendError(ulong producerId, ulong sequenceId)
        => new() { Type = BaseCommand.Types.Type.SendError, SendError = new CommandSendError { ProducerId = producerId, SequenceId = sequenceId, Error = ServerError.NotAllowedError, Message = "error" } };

    private static BaseCommand CreateAckResponse(ulong requestId, ServerError? error = null)
    {
        var response = new CommandAckResponse { ConsumerId = 1, RequestId = requestId };

        if (error.HasValue)
        {
            response.Error = error.Value;
            response.Message = "error";
        }

        return new BaseCommand { Type = BaseCommand.Types.Type.AckResponse, AckResponse = response };
    }
}
