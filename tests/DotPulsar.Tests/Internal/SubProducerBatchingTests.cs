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
using DotPulsar.Internal.Events;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Transactions;
using System.Buffers;
using System.Text;

[Trait("Category", "Unit")]
public sealed class SubProducerBatchingTests
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _longDelay = TimeSpan.FromHours(1);

    [Fact]
    public async Task Dispatcher_GivenBatchingMaxMessages3AndSixMessages_ShouldSendTwoEntries()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, batchingMaxMessages: 3);
        var sendOps = CreateSendOps(1, 6);

        //Act
        await Send(sut, sendOps);
        await WhenCompleted(sendOps);

        //Assert
        var entries = channel.Entries;
        entries.Count.ShouldBe(2);
        entries[0].MessageSequenceIds.ShouldBe([1UL, 2UL, 3UL]);
        entries[1].MessageSequenceIds.ShouldBe([4UL, 5UL, 6UL]);
        entries.ShouldAllBe(entry => entry.IsBatch && entry.NumberOfMessages == 3);
    }

    [Fact]
    public async Task Dispatcher_GivenBatch_ShouldSetSequenceIdToTheLowestAndHighestSequenceIdToTheLastAndNumMessagesInBatch()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, batchingMaxMessages: 4);
        var sendOps = CreateSendOps(10, 4);

        //Act
        await Send(sut, sendOps);
        await WhenCompleted(sendOps);

        //Assert
        var entry = channel.Entries.ShouldHaveSingleItem();
        entry.SequenceId.ShouldBe(10UL);
        entry.Metadata.HighestSequenceId.ShouldBe(13UL);
        entry.Metadata.NumMessagesInBatch.ShouldBe(4);
    }

    [Fact]
    public async Task Dispatcher_GivenSlowProducer_ShouldSendAfterBatchingMaxPublishDelay()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, batchingMaxPublishDelay: TimeSpan.FromMilliseconds(400));
        var sendOps = CreateSendOps(1, 2);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        //Act
        await Send(sut, sendOps);
        await WhenCompleted(sendOps);

        //Assert
        stopwatch.Elapsed.ShouldBeGreaterThan(TimeSpan.FromMilliseconds(300));
        channel.Entries.ShouldHaveSingleItem().NumberOfMessages.ShouldBe(2);
    }

    [Fact]
    public async Task Dispatcher_GivenBatchOfThree_ShouldCompleteEachMessageWithBatchIndex()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, batchingMaxMessages: 3, partition: 4);
        var sendOps = CreateSendOps(1, 3);

        //Act
        await Send(sut, sendOps);
        await WhenCompleted(sendOps);

        //Assert
        var messageIds = sendOps.Select(sendOp => sendOp.ReceiptTcs.Task.Result).ToArray();
        messageIds.Select(messageId => messageId.BatchIndex).ShouldBe([0, 1, 2]);
        messageIds.ShouldAllBe(messageId => messageId.LedgerId == 1 && messageId.EntryId == messageIds[0].EntryId && messageId.Partition == 4 && messageId.Topic == _topic);
    }

    [Fact]
    public async Task Dispatcher_GivenSingleMessageBatch_ShouldSendItAsItIsAndCompleteWithReceiptBatchIndex()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = entry => FakeProducerChannel.CreateReceipt(entry, 5, 7) };
        await using var sut = await CreateSubProducer(channel, batchingMaxPublishDelay: TimeSpan.FromMilliseconds(50));
        var sendOp = CreateSendOp(1);

        //Act
        await sut.Send(sendOp, Current.CancellationToken);
        var messageId = await sendOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var entry = channel.Entries.ShouldHaveSingleItem();
        entry.IsBatch.ShouldBeFalse();
        entry.Metadata.HasHighestSequenceId.ShouldBeFalse();
        entry.Payload.ShouldBe(Encoding.UTF8.GetBytes("payload-1"));
        messageId.BatchIndex.ShouldBe(7);
    }

    [Fact]
    public async Task Dispatcher_GivenTransactionalThenPlainMessage_ShouldSendTwoEntries()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = new[] { CreateSendOp(1, transaction: (7, 13)), CreateSendOp(2) };

        //Act
        await Send(sut, sendOps);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);
        await WhenCompleted(sendOps);

        //Assert
        var entries = channel.Entries;
        entries.Count.ShouldBe(2);
        entries[0].MessageSequenceIds.ShouldBe([1UL]);
        entries[0].Metadata.TxnidMostBits.ShouldBe(7UL);
        entries[0].Metadata.TxnidLeastBits.ShouldBe(13UL);
        entries[1].MessageSequenceIds.ShouldBe([2UL]);
        entries[1].Metadata.HasTxnidMostBits.ShouldBeFalse();
    }

    [Fact]
    public async Task Dispatcher_GivenPlainThenTransactionalMessage_ShouldSendTwoEntries()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = new[] { CreateSendOp(1), CreateSendOp(2, transaction: (0, 0)) };

        //Act
        await Send(sut, sendOps);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);
        await WhenCompleted(sendOps);

        //Assert
        var entries = channel.Entries;
        entries.Count.ShouldBe(2);
        entries[0].Metadata.HasTxnidMostBits.ShouldBeFalse();
        entries[1].Metadata.HasTxnidMostBits.ShouldBeTrue("a transaction with the ids (0, 0) is a transaction");
        entries[1].Metadata.HasTxnidLeastBits.ShouldBeTrue();
    }

    [Fact]
    public async Task Dispatcher_GivenMessagesOfTwoTransactions_ShouldSendTwoEntries()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = new[] { CreateSendOp(1, transaction: (7, 13)), CreateSendOp(2, transaction: (7, 13)), CreateSendOp(3, transaction: (7, 14)), CreateSendOp(4, transaction: (7, 14)) };

        //Act
        await Send(sut, sendOps);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);
        await WhenCompleted(sendOps);

        //Assert
        var entries = channel.Entries;
        entries.Count.ShouldBe(2);
        entries[0].MessageSequenceIds.ShouldBe([1UL, 2UL]);
        entries[0].Metadata.TxnidLeastBits.ShouldBe(13UL);
        entries[1].MessageSequenceIds.ShouldBe([3UL, 4UL]);
        entries[1].Metadata.TxnidLeastBits.ShouldBe(14UL);
    }

    [Fact]
    public async Task Dispatcher_GivenMessagesOfTheSameTransaction_ShouldSendOneBatchWithTheTransactionIds()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = CreateSendOps(1, 3, transaction: (7, 13));

        //Act
        await Send(sut, sendOps);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);
        await WhenCompleted(sendOps);

        //Assert
        var entry = channel.Entries.ShouldHaveSingleItem();
        entry.NumberOfMessages.ShouldBe(3);
        entry.Metadata.TxnidMostBits.ShouldBe(7UL);
        entry.Metadata.TxnidLeastBits.ShouldBe(13UL);
    }

    [Fact]
    public async Task Dispatcher_GivenDeliverAtTimeMessageBetweenMessages_ShouldSendItAlone()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = new[] { CreateSendOp(1), CreateSendOp(2), CreateSendOp(3, deliverAt: 123456789), CreateSendOp(4), CreateSendOp(5) };

        //Act
        await Send(sut, sendOps);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);
        await WhenCompleted(sendOps);

        //Assert
        var entries = channel.Entries;
        entries.Count.ShouldBe(3);
        entries[0].MessageSequenceIds.ShouldBe([1UL, 2UL]);
        entries[1].IsBatch.ShouldBeFalse();
        entries[1].Metadata.DeliverAtTime.ShouldBe(123456789);
        entries[1].SequenceId.ShouldBe(3UL);
        entries[2].MessageSequenceIds.ShouldBe([4UL, 5UL]);
    }

    [Fact]
    public async Task Dispatcher_GivenCancelledMessageBeforeGrouping_ShouldNotSendIt()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var cancelledOp = CreateSendOp(2);
        _ = cancelledOp.ReceiptTcs.TrySetCanceled(Current.CancellationToken);
        var sendOps = new[] { CreateSendOp(1), cancelledOp, CreateSendOp(3) };

        //Act
        await Send(sut, sendOps);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        await WhenCompleted([sendOps[0], sendOps[2]]);
        channel.Entries.ShouldHaveSingleItem().MessageSequenceIds.ShouldBe([1UL, 3UL]);
        cancelledOp.ReceiptTcs.Task.IsCanceled.ShouldBeTrue();
    }

    [Fact]
    public async Task Dispatcher_GivenCancelledFirstMessage_ShouldNotSendItAndBatchTheOthers()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var cancelledOp = CreateSendOp(1);
        _ = cancelledOp.ReceiptTcs.TrySetCanceled(Current.CancellationToken);
        var sendOps = new[] { cancelledOp, CreateSendOp(2), CreateSendOp(3) };

        //Act
        await Send(sut, sendOps);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        await WhenCompleted([sendOps[1], sendOps[2]]);
        channel.Entries.ShouldHaveSingleItem().MessageSequenceIds.ShouldBe([2UL, 3UL]);
    }

    [Fact]
    public async Task Dispatcher_GivenCancelledMessageAfterGrouping_ShouldStillSendIt()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, batchingMaxMessages: 3);
        var sendOps = new[] { CreateSendOp(1), CreateSendOp(2), CreateSendOp(3) };
        await Send(sut, sendOps);
        await WaitFor.Until(() => channel.Entries.Count == 1, "the batch is sent");

        //Act
        _ = sendOps[1].ReceiptTcs.TrySetCanceled(Current.CancellationToken);
        await sut.EstablishNewChannel(Current.CancellationToken);
        await WaitFor.Until(() => channel.Entries.Count == 2, "the batch is sent again");

        //Assert
        channel.Entries[1].MessageSequenceIds.ShouldBe([1UL, 2UL, 3UL]);
    }

    [Fact]
    public async Task Dispatcher_GivenReconnect_ShouldResendIdenticalBatches()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, batchingMaxMessages: 3);
        var sendOps = CreateSendOps(1, 7);
        await Send(sut, sendOps);
        await WaitFor.Until(() => channel.Entries.Count == 2, "the first two batches are sent");

        //Act
        await sut.EstablishNewChannel(Current.CancellationToken);
        await WaitFor.Until(() => channel.Entries.Count >= 4, "the batches are sent again");

        //Assert
        var entries = channel.Entries;
        entries[2].SequenceId.ShouldBe(entries[0].SequenceId);
        entries[2].Metadata.HighestSequenceId.ShouldBe(entries[0].Metadata.HighestSequenceId);
        entries[2].Payload.ShouldBe(entries[0].Payload);
        entries[3].SequenceId.ShouldBe(entries[1].SequenceId);
        entries[3].Metadata.HighestSequenceId.ShouldBe(entries[1].Metadata.HighestSequenceId);
        entries[3].Payload.ShouldBe(entries[1].Payload);
    }

    [Fact]
    public async Task Dispatcher_GivenReconnectWhileBatchIsOpen_ShouldPutTheMessagesInBatchesAgain()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = CreateSendOps(1, 3);
        await Send(sut, sendOps);
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);
        channel.Entries.ShouldBeEmpty("the batch waits for its delay");

        //Act
        await sut.EstablishNewChannel(Current.CancellationToken);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);
        await WhenCompleted(sendOps);

        //Assert
        channel.Entries.ShouldHaveSingleItem().MessageSequenceIds.ShouldBe([1UL, 2UL, 3UL]);
    }

    [Fact]
    public async Task Dispatcher_GivenExplicitSequenceIdLowerThanLastPushed_ShouldSendItInItsOwnEntry()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = new[] { CreateSendOp(10), CreateSendOp(11), CreateSendOp(5), CreateSendOp(12), CreateSendOp(13) };

        //Act
        await Send(sut, sendOps);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);
        await WhenCompleted(sendOps);

        //Assert
        var entries = channel.Entries;
        entries.Count.ShouldBe(3);
        entries[0].MessageSequenceIds.ShouldBe([10UL, 11UL]);
        entries[1].MessageSequenceIds.ShouldBe([5UL]);
        entries[2].MessageSequenceIds.ShouldBe([12UL, 13UL]);
    }

    [Fact]
    public async Task Dispatcher_GivenKeyBased_ShouldReorderQueueAndMatchReceiptsInSendOrder()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, batcherType: BatcherType.KeyBased);
        var sendOps = new[] { CreateSendOp(1, key: "A"), CreateSendOp(2, key: "B"), CreateSendOp(3, key: "B"), CreateSendOp(4, key: "A"), CreateSendOp(5, key: "A") };

        //Act
        await Send(sut, sendOps);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);
        await WhenCompleted(sendOps);
        var messageIds = await Task.WhenAll(sendOps.Select(sendOp => sendOp.ReceiptTcs.Task));

        //Assert
        var entries = channel.Entries;
        entries.Count.ShouldBe(2);
        entries[0].MessageSequenceIds.ShouldBe([2UL, 3UL]);
        entries[0].SequenceId.ShouldBe(3UL, "the sequence id of a batch of the key based batcher is its highest sequence id");
        entries[1].MessageSequenceIds.ShouldBe([1UL, 4UL, 5UL]);
        entries[1].SequenceId.ShouldBe(5UL);
        messageIds[1].BatchIndex.ShouldBe(0);
        messageIds[2].BatchIndex.ShouldBe(1);
        messageIds[0].BatchIndex.ShouldBe(0);
        messageIds[3].BatchIndex.ShouldBe(1);
        messageIds[4].BatchIndex.ShouldBe(2);
        messageIds[1].EntryId.ShouldBe(messageIds[2].EntryId);
        messageIds[1].EntryId.ShouldNotBe(messageIds[0].EntryId);
    }

    [Fact]
    public async Task Dispatcher_GivenKeyBasedAndReconnect_ShouldResendTheBatchesInTheSameOrder()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, batcherType: BatcherType.KeyBased);
        var sendOps = new[] { CreateSendOp(1, key: "A"), CreateSendOp(2, key: "B"), CreateSendOp(3, key: "B"), CreateSendOp(4, key: "A") };
        await Send(sut, sendOps);
        _ = sut.Flush();
        await WaitFor.Until(() => channel.Entries.Count == 2, "both batches are sent");

        //Act
        await sut.EstablishNewChannel(Current.CancellationToken);
        await WaitFor.Until(() => channel.Entries.Count == 4, "both batches are sent again");

        //Assert
        var entries = channel.Entries;
        entries[2].SequenceId.ShouldBe(entries[0].SequenceId);
        entries[3].SequenceId.ShouldBe(entries[1].SequenceId);
        entries[2].Payload.ShouldBe(entries[0].Payload);
        entries[3].Payload.ShouldBe(entries[1].Payload);
    }

    [Fact]
    public async Task ResponseProcessor_GivenNotAllowedSendErrorForBatch_ShouldFailAllMessagesOfBatchAndKeepProducerConnected()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeProducerChannel();
        channel.Respond = entry => entry.SequenceId == 1 ? FakeProducerChannel.CreateSendError(1, ServerError.NotAllowedError, "Transaction [(7,13)] has been ended") : channel.CreateReceipt(entry);
        await using var sut = await CreateSubProducer(channel, registerEvent, batchingMaxMessages: 3);
        var failedOps = CreateSendOps(1, 3, transaction: (7, 13));
        var plainOps = CreateSendOps(4, 3);

        //Act
        await Send(sut, failedOps);
        await Send(sut, plainOps);
        await WhenCompleted(plainOps);

        //Assert
        var exceptions = new List<Exception>();
        foreach (var failedOp in failedOps)
        {
            exceptions.Add(await Should.ThrowAsync<NotAllowedException>(failedOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken)));
        }

        exceptions.ShouldAllBe(exception => ReferenceEquals(exception, exceptions[0]));
        exceptions[0].Message.ShouldBe("Transaction [(7,13)] has been ended");
        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted || argument is SendReceiptWrongOrdering);
    }

    [Fact]
    public async Task ResponseProcessor_GivenNotAllowedSendErrorWithHighestSequenceId_ShouldMatchHead()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeProducerChannel();
        channel.Respond = entry => entry.SequenceId == 1 ? FakeProducerChannel.CreateSendError(3, ServerError.NotAllowedError, "Not allowed") : channel.CreateReceipt(entry);
        await using var sut = await CreateSubProducer(channel, registerEvent, batchingMaxMessages: 3);
        var failedOps = CreateSendOps(1, 3);
        var plainOps = CreateSendOps(4, 3);

        //Act
        await Send(sut, failedOps);
        await Send(sut, plainOps);
        await WhenCompleted(plainOps);

        //Assert
        foreach (var failedOp in failedOps)
        {
            await Should.ThrowAsync<NotAllowedException>(failedOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        }

        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted || argument is SendReceiptWrongOrdering);
    }

    [Fact]
    public async Task ResponseProcessor_GivenNotAllowedSendErrorForBatchThatIsNotTheHead_ShouldReconnectInsteadOfFailingTheWrongMessages()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeProducerChannel { Respond = entry => entry.SequenceId == 4 ? FakeProducerChannel.CreateSendError(4, ServerError.NotAllowedError, "Not allowed") : null };
        await using var sut = await CreateSubProducer(channel, registerEvent, batchingMaxMessages: 3);
        var headOps = CreateSendOps(1, 3);
        var notAllowedOps = CreateSendOps(4, 3);

        //Act
        await Send(sut, headOps);
        await Send(sut, notAllowedOps);

        //Assert
        await WaitFor.Until(() => registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).Any(argument => argument is SendReceiptWrongOrdering), "the producer reconnects");
        headOps.Concat(notAllowedOps).ShouldAllBe(sendOp => !sendOp.ReceiptTcs.Task.IsCompleted);
    }

    [Fact]
    public async Task ResponseProcessor_GivenReceiptWithOtherHighestSequenceIdThanTheBatch_ShouldReconnect()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeProducerChannel();
        channel.Respond = entry =>
        {
            var receipt = channel.CreateReceipt(entry);
            receipt.SendReceipt.HighestSequenceId = entry.HighestSequenceId + 1;
            return receipt;
        };
        await using var sut = await CreateSubProducer(channel, registerEvent, batchingMaxMessages: 3);
        var sendOps = CreateSendOps(1, 3);

        //Act
        await Send(sut, sendOps);

        //Assert
        await WaitFor.Until(() => registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).Any(argument => argument is SendReceiptWrongOrdering), "the producer reconnects");
        sendOps.ShouldAllBe(sendOp => !sendOp.ReceiptTcs.Task.IsCompleted);
    }

    [Fact]
    public async Task ResponseProcessor_GivenReceiptWithTheHighestSequenceIdOfTheBatch_ShouldCompleteTheMessages()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        channel.Respond = entry =>
        {
            var receipt = channel.CreateReceipt(entry);
            receipt.SendReceipt.HighestSequenceId = entry.HighestSequenceId;
            return receipt;
        };
        await using var sut = await CreateSubProducer(channel, batchingMaxMessages: 3);
        var sendOps = CreateSendOps(1, 3);

        //Act
        await Send(sut, sendOps);
        await WhenCompleted(sendOps);

        //Assert
        sendOps.Select(sendOp => sendOp.ReceiptTcs.Task.Result.BatchIndex).ShouldBe([0, 1, 2]);
    }

    [Fact]
    public async Task Dispatcher_GivenBatchLargerThanMaxMessageSize_ShouldFailAllMessagesWithTooLargeMessageExceptionAndReturnTheBatch()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeProducerChannel { Fail = entry => entry.IsBatch ? new TooLargeMessageException(6000000, 5000000) : null };
        await using var sut = await CreateSubProducer(channel, registerEvent, batchingMaxMessages: 3);
        var failedOps = CreateSendOps(1, 3);
        var plainOp = CreateSendOp(4);

        //Act
        await Send(sut, failedOps);
        await WaitFor.Until(() => failedOps.All(sendOp => sendOp.ReceiptTcs.Task.IsCompleted), "the messages of the batch have failed");
        await sut.Send(plainOp, Current.CancellationToken);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        var exceptions = new List<Exception>();
        foreach (var failedOp in failedOps)
        {
            exceptions.Add(await Should.ThrowAsync<TooLargeMessageException>(failedOp.ReceiptTcs.Task));
        }

        exceptions.ShouldAllBe(exception => ReferenceEquals(exception, exceptions[0]));
        await plainOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted);
        failedOps.ShouldAllBe(sendOp => sendOp.Batch == null);
    }

    [Fact]
    public async Task Flush_GivenLongPublishDelay_ShouldSendImmediatelyAndAwaitLastMessage()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = CreateSendOps(1, 5);
        await Send(sut, sendOps);

        //Act
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        sendOps.ShouldAllBe(sendOp => sendOp.ReceiptTcs.Task.IsCompletedSuccessfully);
        channel.Entries.ShouldHaveSingleItem().NumberOfMessages.ShouldBe(5);
    }

    [Fact]
    public async Task Flush_GivenNoMessages_ShouldBeCompleted()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);

        //Act
        var flush = sut.Flush();

        //Assert
        flush.IsCompletedSuccessfully.ShouldBeTrue();
    }

    [Fact]
    public async Task Flush_GivenLastMessageFails_ShouldFailWithItsException()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        channel.Respond = entry => FakeProducerChannel.CreateSendError(entry.SequenceId, ServerError.NotAllowedError, "Not allowed");
        await using var sut = await CreateSubProducer(channel);
        var sendOps = CreateSendOps(1, 2);
        await Send(sut, sendOps);

        //Act
        var act = () => sut.Flush().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        await Should.ThrowAsync<NotAllowedException>(act);
    }

    [Fact]
    public async Task Flush_GivenLastMessageAlreadySent_ShouldNotSendALaterBatchBeforeItsDelay()
    {
        //Arrange
        var channel = new FakeProducerChannel { Respond = _ => null };
        await using var sut = await CreateSubProducer(channel, batchingMaxMessages: 2);
        var firstBatch = CreateSendOps(1, 2);
        await Send(sut, firstBatch);
        await WaitFor.Until(() => channel.Entries.Count == 1, "the first batch is sent");
        _ = sut.Flush();
        var laterOp = CreateSendOp(3);

        //Act
        await sut.Send(laterOp, Current.CancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(300), Current.CancellationToken);
        var sentBeforeFlush = channel.Entries.Count;
        _ = sut.Flush();
        await WaitFor.Until(() => channel.Entries.Count == 2, "the later batch is sent by the flush");

        //Assert
        sentBeforeFlush.ShouldBe(1);
        channel.Entries[1].MessageSequenceIds.ShouldBe([3UL]);
    }

    [Fact]
    public async Task WaitForSendQueueEmpty_GivenLongPublishDelay_ShouldNotWaitForTheDelay()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = CreateSendOps(1, 3);
        await Send(sut, sendOps);

        //Act
        await sut.WaitForSendQueueEmpty(Current.CancellationToken).AsTask().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        channel.Entries.ShouldHaveSingleItem().NumberOfMessages.ShouldBe(3);
    }

    [Fact]
    public async Task Dispatcher_GivenRegistrationGateOfLaterMessageNotCompleted_ShouldWaitForItAndBatchItWithTheOthers()
    {
        //Arrange
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, batchingMaxMessages: 3);
        var sendOps = new[] { CreateSendOp(1), CreateSendOp(2, registrationGate: gate.Task), CreateSendOp(3) };
        await Send(sut, sendOps);
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);
        var sentBeforeGate = channel.Entries.Count;

        //Act
        gate.SetResult();
        await WhenCompleted(sendOps);

        //Assert
        sentBeforeGate.ShouldBe(0);
        channel.Entries.ShouldHaveSingleItem().MessageSequenceIds.ShouldBe([1UL, 2UL, 3UL]);
    }

    [Fact]
    public async Task Dispatcher_GivenRegistrationGateNotCompletedWithinPublishDelay_ShouldSendTheBatchWithoutTheMessage()
    {
        //Arrange
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel, batchingMaxPublishDelay: TimeSpan.FromMilliseconds(100));
        var sendOps = new[] { CreateSendOp(1), CreateSendOp(2, registrationGate: gate.Task), CreateSendOp(3) };

        //Act
        await Send(sut, sendOps);
        await sendOps[0].ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        var sentBeforeGate = channel.Entries.ToArray();
        gate.SetResult();
        await WhenCompleted(sendOps);

        //Assert
        sentBeforeGate.ShouldHaveSingleItem().MessageSequenceIds.ShouldBe([1UL]);
        channel.Entries[1].MessageSequenceIds.ShouldBe([2UL, 3UL]);
    }

    [Fact]
    public async Task Dispatcher_GivenRegistrationGateOfLaterMessageFails_ShouldFailOnlyThatMessageAndBatchTheOthers()
    {
        //Arrange
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = new[] { CreateSendOp(1), CreateSendOp(2, registrationGate: gate.Task), CreateSendOp(3) };
        await Send(sut, sendOps);
        await Task.Delay(TimeSpan.FromMilliseconds(250), Current.CancellationToken);

        //Act
        gate.SetException(new TransactionCoordinatorClientException("The registration failed"));
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        await Should.ThrowAsync<TransactionCoordinatorClientException>(sendOps[1].ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        await WhenCompleted([sendOps[0], sendOps[2]]);
        channel.Entries.ShouldHaveSingleItem().MessageSequenceIds.ShouldBe([1UL, 3UL]);
    }

    [Fact]
    public async Task Dispatcher_GivenRegistrationGateAlreadyFailedForLaterMessage_ShouldFailOnlyThatMessageAndBatchTheOthers()
    {
        //Arrange
        var channel = new FakeProducerChannel();
        await using var sut = await CreateSubProducer(channel);
        var sendOps = new[] { CreateSendOp(1), CreateSendOp(2, registrationGate: Task.FromException(new TransactionCoordinatorClientException("The registration failed"))), CreateSendOp(3) };

        //Act
        await Send(sut, sendOps);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        await Should.ThrowAsync<TransactionCoordinatorClientException>(sendOps[1].ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        await WhenCompleted([sendOps[0], sendOps[2]]);
        channel.Entries.ShouldHaveSingleItem().MessageSequenceIds.ShouldBe([1UL, 3UL]);
    }

    private const string _topic = "persistent://public/default/test";

    private static async Task Send(SubProducer sut, IEnumerable<SendOp> sendOps)
    {
        foreach (var sendOp in sendOps)
        {
            await sut.Send(sendOp, Current.CancellationToken);
        }
    }

    private static async Task WhenCompleted(IEnumerable<SendOp> sendOps)
        => await Task.WhenAll(sendOps.Select(sendOp => sendOp.ReceiptTcs.Task)).WaitAsync(_timeout, Current.CancellationToken);

    private static SendOp[] CreateSendOps(ulong firstSequenceId, int count, (ulong Most, ulong Least)? transaction = null)
        => Enumerable.Range(0, count).Select(i => CreateSendOp(firstSequenceId + (ulong) i, transaction: transaction)).ToArray();

    private static SendOp CreateSendOp(
        ulong sequenceId,
        string? key = null,
        (ulong Most, ulong Least)? transaction = null,
        long? deliverAt = null,
        Task? registrationGate = null)
    {
        var metadata = new MessageMetadata { SequenceId = sequenceId };

        if (key is not null)
            metadata.PartitionKey = key;

        if (transaction is not null)
        {
            metadata.TxnidMostBits = transaction.Value.Most;
            metadata.TxnidLeastBits = transaction.Value.Least;
        }

        if (deliverAt is not null)
            metadata.DeliverAtTime = deliverAt.Value;

        var payload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes($"payload-{sequenceId}"));
        var receiptTcs = new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously);

        return new SendOp(metadata, payload, receiptTcs, registrationGate: registrationGate);
    }

    private static async Task<SubProducer> CreateSubProducer(
        FakeProducerChannel channel,
        IRegisterEvent? registerEvent = null,
        bool enableBatching = true,
        BatcherType batcherType = BatcherType.Default,
        uint batchingMaxMessages = 1000,
        uint batchingMaxBytes = 128 * 1024,
        TimeSpan? batchingMaxPublishDelay = null,
        int partition = 0,
        TimeSpan? sendTimeout = null)
    {
        registerEvent ??= Substitute.For<IRegisterEvent>();
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
            partition,
            1000,
            _topic,
            enableBatching,
            batcherType,
            batchingMaxMessages,
            batchingMaxBytes,
            batchingMaxPublishDelay ?? _longDelay,
            1,
            sendTimeout ?? TimeSpan.Zero);

        await sut.EstablishNewChannel(Current.CancellationToken);

        return sut;
    }
}
