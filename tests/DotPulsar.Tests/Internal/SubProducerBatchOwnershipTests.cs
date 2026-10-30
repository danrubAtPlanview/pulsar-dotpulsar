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
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Batching;
using DotPulsar.Internal.Events;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Transactions;
using System.Buffers;
using System.Text;

// Who returns the batch of a producer to the pool when the batch can not be written. The Java container owns the buffer of a batch until its payload is handed to the command, and the tests of the Java container
// count the releases of the buffer. The batch of a DotPulsar producer is rented from the pool that all producers share, and the producer returns it when it ends the messages of the batch. The tests look at the
// state of the batch, so they do not run in parallel with other tests (see SharedBatchPoolCollection). A batch can only be returned once, because PreparedBatch.Return does nothing after the first call, which is
// tested in PreparedBatchTests, so the tests look at what the pool gives out afterwards: a batch that was returned twice would be given out twice if that was not so, and one that was not returned is not given out.
[Collection("SharedBatchPool")]
[Trait("Category", "Unit")]
public sealed class SubProducerBatchOwnershipTests
{
    private const string _topic = "persistent://public/default/test";
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    // Upstream: BatchMessageContainerImplTest#oversizedBatchReleasesThePayloadExactlyOnce (the batch is not written because it is too large, and its buffer is released by the one that ends the messages)
    // Upstream: BatchMessageContainerImplTest#testOversizedBatchReleasesItsPayloadExactlyOnce
    // The compression and the encryption that make the Java rows differ are done by the channel and do not change who owns the batch (see BatchSendThroughProducerChannelTests).
    [Fact]
    public async Task Dispatcher_GivenBatchThatIsTooLarge_ShouldFailTheMessagesAndReturnTheBatchToThePoolExactlyOnce()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var failedOps = CreateSendOps(1, 3);
        PreparedBatch? batch = null;
        var channel = new FakeProducerChannel
        {
            Fail = entry =>
            {
                if (!entry.IsBatch)
                    return null;

                batch ??= failedOps[0].Batch;
                return new TooLargeMessageException(6000000, 5000000);
            }
        };
        await using var sut = await CreateSubProducer(channel, registerEvent, batchingMaxMessages: 3);

        //Act
        await Send(sut, failedOps);
        await WaitFor.Until(() => failedOps.All(sendOp => sendOp.ReceiptTcs.Task.IsCompleted), "the messages of the batch have failed");
        var returned = batch!.IsReturned;
        var rented = RentEverythingThatThePoolHas();

        //Assert
        batch.ShouldNotBeNull();
        returned.ShouldBeTrue("the producer returns the batch when it fails the messages");
        rented.Count(rentedBatch => ReferenceEquals(rentedBatch, batch)).ShouldBe(1, "the pool gives out the batch once");
        failedOps.ShouldAllBe(sendOp => sendOp.Batch == null);
        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted);
    }

    // Upstream: BatchMessageContainerImplTest#oversizedSingleMessageReleasesTheBatchBufferExactlyOnce
    // Upstream: BatchMessageContainerImplTest#testOversizedSingleMessageBatchReleasesItsPayloadExactlyOnce
    // The Java container writes a single message to the buffer of its batch, and then hands that buffer to the command, so the release of the command is the release of the buffer of the container. A single message
    // of DotPulsar is not written to a buffer of the pool: it is written as it is, so there is no batch that anything could return, and nothing in the pool is touched.
    [Fact]
    public async Task Dispatcher_GivenSingleMessageThatIsTooLargeAndBatching_ShouldFailItWithoutRentingABatchAndWriteTheNextMessage()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var tooLargeOp = CreateSendOp(1, 5000);
        var nextOp = CreateSendOp(2);
        var channel = new FakeProducerChannel { MaxMessageSize = 1000 };
        var batchesWhenWritten = new List<PreparedBatch?>();
        channel.Fail = entry =>
        {
            batchesWhenWritten.Add(entry.SequenceId == 1 ? tooLargeOp.Batch : nextOp.Batch);
            return entry.Payload.Length > channel.MaxMessageSize ? new TooLargeMessageException(entry.Payload.Length, channel.MaxMessageSize) : null;
        };
        await using var sut = await CreateSubProducer(channel, registerEvent, batchingMaxBytes: 1024);

        //Act
        await sut.Send(tooLargeOp, Current.CancellationToken);
        await WaitFor.Until(() => tooLargeOp.ReceiptTcs.Task.IsCompleted, "the message has failed");
        await sut.Send(nextOp, Current.CancellationToken);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        await Should.ThrowAsync<TooLargeMessageException>(tooLargeOp.ReceiptTcs.Task);
        await nextOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        batchesWhenWritten.ShouldBe([null, null]);
        channel.Entries.ShouldHaveSingleItem().IsBatch.ShouldBeFalse();
        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted || argument is ChannelDisconnected);
    }

    // Upstream: BatchMessageContainerImplTest#discardReleasesTheBatchBufferWhenTheCallbackThrows
    // The Java container calls the callbacks of the messages when it discards a batch, they are application code, and the release of the buffer must not be skipped when one of them throws. The receipts of the
    // DotPulsar messages are completed after the batch was returned, and a continuation that throws is captured in its own task, so it can not skip anything. The continuations run synchronously here,
    // like the continuation of an awaiting caller can.
    [Fact]
    public async Task Dispatcher_GivenBatchThatFailsAndContinuationsOfTheMessagesThatThrow_ShouldStillReturnTheBatchAndNotFaultTheProducer()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var failedOps = Enumerable.Range(1, 3).Select(i => CreateSendOp((ulong) i, runContinuationsAsynchronously: false)).ToArray();
        var continuations = failedOps.Select(sendOp => sendOp.ReceiptTcs.Task.ContinueWith(
            _ => throw new InvalidOperationException("mocked application callback failure"),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default)).ToArray();
        PreparedBatch? batch = null;
        var channel = new FakeProducerChannel
        {
            Fail = entry =>
            {
                if (!entry.IsBatch)
                    return null;

                batch ??= failedOps[0].Batch;
                return new TooLargeMessageException(6000000, 5000000);
            }
        };
        await using var sut = await CreateSubProducer(channel, registerEvent, batchingMaxMessages: 3);
        var nextOp = CreateSendOp(4);

        //Act
        await Send(sut, failedOps);
        var callbackExceptions = await Task.WhenAll(continuations.Select(continuation => Record.ExceptionAsync(() => continuation).AsTask()).ToArray()).WaitAsync(_timeout, Current.CancellationToken);
        var returned = batch!.IsReturned;
        await sut.Send(nextOp, Current.CancellationToken);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        callbackExceptions.ShouldAllBe(exception => exception is InvalidOperationException, "the continuations ran, and they threw");
        returned.ShouldBeTrue("the producer returns the batch although the continuations of the messages throw");
        failedOps.ShouldAllBe(sendOp => sendOp.ReceiptTcs.Task.IsFaulted);
        await nextOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        failedOps.ShouldAllBe(sendOp => sendOp.Batch == null);
        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted || argument is ChannelDisconnected);
    }

    // The batches that are in the pool are the batches that were returned and not rented since. The pool keeps at most DefaultMaximumRetained of them.
    private static List<PreparedBatch> RentEverythingThatThePoolHas()
        => Enumerable.Range(0, PreparedBatchPool.DefaultMaximumRetained + 2).Select(_ => PreparedBatchPool.Shared.Get()).ToList();

    private static async Task Send(SubProducer sut, IEnumerable<SendOp> sendOps)
    {
        foreach (var sendOp in sendOps)
        {
            await sut.Send(sendOp, Current.CancellationToken);
        }
    }

    private static SendOp[] CreateSendOps(ulong firstSequenceId, int count)
        => Enumerable.Range(0, count).Select(i => CreateSendOp(firstSequenceId + (ulong) i)).ToArray();

    private static SendOp CreateSendOp(ulong sequenceId, int payloadSize = 0, bool runContinuationsAsynchronously = true)
    {
        var payload = payloadSize == 0 ? Encoding.UTF8.GetBytes($"payload-{sequenceId}") : new byte[payloadSize];
        var receiptTcs = new TaskCompletionSource<MessageId>(runContinuationsAsynchronously ? TaskCreationOptions.RunContinuationsAsynchronously : TaskCreationOptions.None);

        return new SendOp(new MessageMetadata { SequenceId = sequenceId }, new ReadOnlySequence<byte>(payload), receiptTcs);
    }

    private static async Task<SubProducer> CreateSubProducer(
        FakeProducerChannel channel,
        IRegisterEvent registerEvent,
        uint batchingMaxMessages = 1000,
        uint batchingMaxBytes = 128 * 1024)
    {
        var correlationId = Guid.NewGuid();
        var factory = Substitute.For<IProducerChannelFactory>();
        factory.Create(Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult<IProducerChannel>(channel));

        var sut = new SubProducer(
            correlationId,
            registerEvent,
            Substitute.For<IProducerChannel>(),
            new Executor(correlationId, registerEvent, new DefaultExceptionHandler()),
            Substitute.For<IState<ProducerState>>(),
            factory,
            0,
            1000,
            _topic,
            true,
            BatcherType.Default,
            batchingMaxMessages,
            batchingMaxBytes,
            TimeSpan.FromHours(1),
            1,
            TimeSpan.Zero);

        await sut.EstablishNewChannel(Current.CancellationToken);

        return sut;
    }
}
