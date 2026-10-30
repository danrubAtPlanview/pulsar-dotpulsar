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

    [Fact]
    public async Task Outgoing_GivenBatchCommandSendThatIsCancelled_ShouldRemoveBothRequests()
    {
        //Arrange
        using var uut = new RequestResponseHandler();
        var tcs = new TaskCompletionSource<BaseCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        uut.Outgoing(new CommandSend { ProducerId = 3, SequenceId = 5, HighestSequenceId = 9 }, tcs);

        //Act
        tcs.SetCanceled();

        //Assert
        await WaitFor.Until(() => GetNumberOfRequests(uut) == 0, "both requests are removed");
    }

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
