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

namespace DotPulsar.Tests.Internal.Crypto;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.Events;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Transactions;
using System.Buffers;

// What the sub producer does when a message can not be encrypted, and the message crypto it owns, with a channel that records what it is asked to write
[Trait("Category", "Unit")]
public sealed class SubProducerEncryptionTests
{
    private const string _topic = "persistent://public/default/test";
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    // Upstream: ProducerImpl#sendAsync (PulsarClientException)
    [Fact]
    public async Task Dispatcher_GivenCryptoExceptionForAMessage_ShouldFailOnlyThatMessageAndNotFaultTheProducer()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var cryptoException = new CryptoException("Failed to encrypt the message");
        var channel = new FakeProducerChannel { Fail = entry => entry.SequenceId == 2 ? cryptoException : null };
        await using var sut = await CreateSubProducer(channel, registerEvent: registerEvent);
        var sendOps = new[] { CreateSendOp(1), CreateSendOp(2), CreateSendOp(3) };

        //Act
        foreach (var sendOp in sendOps)
        {
            await sut.Send(sendOp, Current.CancellationToken);
        }

        //Assert
        await sendOps[0].ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        await sendOps[2].ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        (await Should.ThrowAsync<CryptoException>(sendOps[1].ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken))).ShouldBeSameAs(cryptoException);
        channel.Entries.Select(entry => entry.SequenceId).ShouldBe([1UL, 3UL]);
        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted || argument is ChannelDisconnected);
    }

    [Fact]
    public async Task Send_GivenCryptoExceptionForAMessage_ShouldAcceptTheNextMessage()
    {
        //Arrange
        var channel = new FakeProducerChannel { Fail = entry => entry.SequenceId == 1 ? new CryptoException("Failed to encrypt the message") : null };
        await using var sut = await CreateSubProducer(channel);
        var failed = CreateSendOp(1);
        await sut.Send(failed, Current.CancellationToken);
        _ = await Should.ThrowAsync<CryptoException>(failed.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        var next = CreateSendOp(2);

        //Act
        var exception = await Record.ExceptionAsync(async () => await sut.Send(next, Current.CancellationToken));

        //Assert
        exception.ShouldBeNull();
        (await next.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken)).EntryId.ShouldBe(1UL);
    }

    // Upstream: BatchMessageContainerImpl#createOpSendMsg (the callbacks of the messages of a batch that can not be encrypted fail)
    [Fact]
    public async Task Dispatcher_GivenCryptoExceptionForABatch_ShouldFailAllTheMessagesOfTheBatchWithItAndContinue()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var cryptoException = new CryptoException("Failed to encrypt the batch");
        var channel = new FakeProducerChannel { Fail = entry => entry.IsBatch ? cryptoException : null };
        await using var sut = await CreateSubProducer(channel, registerEvent: registerEvent, enableBatching: true, batchingMaxMessages: 3);
        var failedOps = new[] { CreateSendOp(1), CreateSendOp(2), CreateSendOp(3) };
        var plainOp = CreateSendOp(4);

        //Act
        foreach (var sendOp in failedOps)
        {
            await sut.Send(sendOp, Current.CancellationToken);
        }

        await WaitFor.Until(() => failedOps.All(sendOp => sendOp.ReceiptTcs.Task.IsCompleted), "the messages of the batch have failed");
        await sut.Send(plainOp, Current.CancellationToken);
        await sut.Flush().WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        foreach (var failedOp in failedOps)
        {
            (await Should.ThrowAsync<CryptoException>(failedOp.ReceiptTcs.Task)).ShouldBeSameAs(cryptoException);
        }

        await plainOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        failedOps.ShouldAllBe(sendOp => sendOp.Batch == null);
        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted || argument is ChannelDisconnected);
    }

    // Upstream: ProducerImpl#sendAsync (PulsarClientException): the chunks that follow the chunk that failed are not written, and the producer is not faulted
    [Fact]
    public async Task Dispatcher_GivenCryptoExceptionForAChunk_ShouldFailTheMessageWriteNoMoreChunksOfItAndContinue()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var cryptoException = new CryptoException("Failed to encrypt the chunk");
        var channel = new FakeProducerChannel { Fail = entry => entry.SequenceId == 1 && entry.Metadata.ChunkId == 1 ? cryptoException : null };
        await using var sut = await CreateSubProducer(channel, registerEvent: registerEvent, enableChunking: true);
        var chunkedOp = CreateSendOp(1, 250);
        var nextOp = CreateSendOp(2, 10);

        //Act
        await sut.Send(chunkedOp, Current.CancellationToken);
        await sut.Send(nextOp, Current.CancellationToken);

        //Assert
        (await Should.ThrowAsync<CryptoException>(chunkedOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken))).ShouldBeSameAs(cryptoException);
        await nextOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken);
        channel.Entries.Select(entry => (entry.SequenceId, entry.Metadata.HasChunkId ? entry.Metadata.ChunkId : -1)).ShouldBe([(1UL, 0), (2UL, -1)]);
        chunkedOp.Chunking.ShouldBeNull();
        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted || argument is ChannelDisconnected);
    }

    // The answer of the broker to the chunk that was written before the message failed is not the answer to the message that is written after it
    [Fact]
    public async Task Dispatcher_GivenCryptoExceptionForAChunkAndTheAnswerToAnEarlierChunkAfterwards_ShouldIgnoreItAndNotCompleteTheNextMessageWithIt()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeProducerChannel
        {
            Fail = entry => entry.SequenceId == 1 && entry.Metadata.ChunkId == 1 ? new CryptoException("Failed to encrypt the chunk") : null,
            Respond = _ => null
        };
        await using var sut = await CreateSubProducer(channel, registerEvent: registerEvent, enableChunking: true);
        var chunkedOp = CreateSendOp(1, 250);
        var nextOp = CreateSendOp(2, 10);
        await sut.Send(chunkedOp, Current.CancellationToken);
        _ = await Should.ThrowAsync<CryptoException>(chunkedOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken));
        await sut.Send(nextOp, Current.CancellationToken);
        await WaitFor.Until(() => channel.Entries.Count == 2, "the next message is written");
        var firstChunk = channel.Entries[0];
        var nextEntry = channel.Entries[1];

        //Act
        firstChunk.Response.TrySetResult(FakeProducerChannel.CreateReceipt(firstChunk, 10));
        await Task.Delay(TimeSpan.FromMilliseconds(200), Current.CancellationToken);
        var completedByTheEarlierChunk = nextOp.ReceiptTcs.Task.IsCompleted;
        nextEntry.Response.TrySetResult(FakeProducerChannel.CreateReceipt(nextEntry, 50));

        //Assert
        completedByTheEarlierChunk.ShouldBeFalse();
        (await nextOp.ReceiptTcs.Task.WaitAsync(_timeout, Current.CancellationToken)).EntryId.ShouldBe(50UL);
        registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).ShouldNotContain(argument => argument is ExecutorFaulted || argument is SendReceiptWrongOrdering || argument is ChannelDisconnected);
    }

    [Fact]
    public async Task Dispatcher_GivenExceptionThatIsNotACryptoException_ShouldStillFaultTheProducer()
    {
        //Arrange
        var registerEvent = Substitute.For<IRegisterEvent>();
        var channel = new FakeProducerChannel { Fail = entry => entry.SequenceId == 1 ? new InvalidOperationException("The channel is broken") : null };
        await using var sut = await CreateSubProducer(channel, registerEvent: registerEvent);

        //Act
        await sut.Send(CreateSendOp(1), Current.CancellationToken);

        //Assert
        await WaitFor.Until(() => registerEvent.ReceivedCalls().Select(call => call.GetArguments()[0]).Any(argument => argument is ExecutorFaulted { Exception: InvalidOperationException }), "the producer is faulted");
    }

    // Upstream: ProducerImpl#closeAsync (the key generator task is cancelled)
    [Fact]
    public async Task DisposeAsync_GivenMessageCrypto_ShouldDisposeIt()
    {
        //Arrange
        var messageCrypto = CreateCrypto();
        var sut = await CreateSubProducer(new FakeProducerChannel(), messageCrypto: messageCrypto);

        //Act
        await sut.DisposeAsync();

        //Assert
        AssertDisposed(messageCrypto);
    }

    [Fact]
    public async Task ChannelFaulted_GivenMessageCrypto_ShouldDisposeIt()
    {
        //Arrange
        var messageCrypto = CreateCrypto();
        await using var sut = await CreateSubProducer(new FakeProducerChannel(), messageCrypto: messageCrypto);

        //Act
        await sut.ChannelFaulted(new InvalidOperationException("The channel faulted"));

        //Assert
        AssertDisposed(messageCrypto);
    }

    [Fact]
    public async Task DisposeAsync_GivenMessageCryptoAndAMessageThatIsBeingSent_ShouldNotDisposeItBeforeTheDispatcherIsDone()
    {
        //Arrange
        var messageCrypto = CreateCrypto();
        var writeGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channel = new FakeProducerChannel { WriteGate = writeGate.Task };
        var sut = await CreateSubProducer(channel, messageCrypto: messageCrypto);
        await sut.Send(CreateSendOp(1), Current.CancellationToken);
        await WaitFor.Until(() => channel.Entries.Count == 1, "the message is being written");

        //Act
        var dispose = sut.DisposeAsync().AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(100), Current.CancellationToken);
        var disposedWhileBeingWritten = IsDisposed(messageCrypto);
        writeGate.SetResult();
        await dispose.WaitAsync(_timeout, Current.CancellationToken);

        //Assert
        disposedWhileBeingWritten.ShouldBeFalse();
        AssertDisposed(messageCrypto);
    }

    [Fact]
    public async Task DisposeAsync_GivenNoMessageCrypto_ShouldNotThrow()
    {
        //Arrange
        var sut = await CreateSubProducer(new FakeProducerChannel());

        //Act
        var exception = await Record.ExceptionAsync(async () => await sut.DisposeAsync());

        //Assert
        exception.ShouldBeNull();
    }

    private static MessageCrypto CreateCrypto()
    {
        var crypto = new MessageCrypto(true);
        crypto.AddPublicKeyCipher(["rsa"], new InMemoryCryptoKeyReader().WithPublicKey("rsa", CryptoTestKeys.Public("ClientRsa")));
        return crypto;
    }

    // A message crypto that is disposed can not encrypt anymore
    private static void AssertDisposed(MessageCrypto messageCrypto)
        => IsDisposed(messageCrypto).ShouldBeTrue();

    private static bool IsDisposed(MessageCrypto messageCrypto)
        => Record.Exception(() => messageCrypto.Encrypt(new MessageMetadata(), new ReadOnlySequence<byte>(new byte[1]))) is ObjectDisposedException;

    private static SendOp CreateSendOp(ulong sequenceId, int length = 10)
        => new(new MessageMetadata { SequenceId = sequenceId }, new ReadOnlySequence<byte>(new byte[length]), new TaskCompletionSource<MessageId>(TaskCreationOptions.RunContinuationsAsynchronously));

    private static async Task<SubProducer> CreateSubProducer(
        FakeProducerChannel channel,
        IRegisterEvent? registerEvent = null,
        bool enableBatching = false,
        uint batchingMaxMessages = 1000,
        bool enableChunking = false,
        MessageCrypto? messageCrypto = null)
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
            -1,
            1000,
            _topic,
            enableBatching,
            BatcherType.Default,
            batchingMaxMessages,
            ProducerOptions<string>.DefaultBatchingMaxBytes,
            TimeSpan.FromHours(1),
            1,
            TimeSpan.Zero,
            enableChunking,
            100,
            messageCrypto);

        await sut.EstablishNewChannel(Current.CancellationToken);

        return sut;
    }
}
