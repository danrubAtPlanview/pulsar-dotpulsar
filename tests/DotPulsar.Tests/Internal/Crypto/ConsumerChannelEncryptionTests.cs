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
using DotPulsar.Extensions;
using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using DotPulsar.Tests.Internal.Chunking;
using System.Buffers;
using System.Text;

[Trait("Category", "Unit")]
public sealed class ConsumerChannelEncryptionTests
{
    private const ulong Ledger = 1;
    private const string KeyName = "rsa";

    // Upstream: ConsumerImpl#decryptPayloadIfNeeded
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenEncryptedMessage_ShouldDeliverTheDecryptedPayloadAndTheEncryptionContext()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        await using var uut = CreateUut(CreateConsumerKeyReader());
        var metadata = CreateMetadata();
        uut.Enqueue(CreateEncryptedPackage(producer, metadata, 1, "Hello"));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(actual).ShouldBe("Hello");
        var context = actual.GetEncryptionContext().ShouldNotBeNull();
        context.IsEncrypted.ShouldBeFalse();
        context.Keys.Keys.ShouldBe([KeyName]);
        context.Keys[KeyName].KeyValue.Length.ShouldBe(256);
        context.Param.ShouldBe(metadata.EncryptionParam.ToByteArray());
        context.Param.Length.ShouldBe(12);
        context.Algorithm.ShouldBeNull();
        context.CompressionType.ShouldBe(DotPulsar.CompressionType.None);
        context.BatchSize.ShouldBeNull();
        uut.Acks.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenMessageThatIsNotEncrypted_ShouldDeliverItWithoutEncryptionContextAndNotUseTheKeyReader()
    {
        //Arrange
        var keyReader = Substitute.For<ICryptoKeyReader>();
        await using var uut = CreateUut(keyReader);
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 1)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.GetEncryptionContext().ShouldBeNull();
        keyReader.ReceivedCalls().ShouldBeEmpty();
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenMessageThatIsNotEncryptedAndNoCryptoKeyReader_ShouldDeliverItWhateverTheFailureAction()
    {
        //Arrange
        await using var uut = CreateUut(null, ConsumerCryptoFailureAction.Discard);
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 1)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.GetEncryptionContext().ShouldBeNull();
        uut.Acks.ShouldBeEmpty();
    }

    // Upstream: ConsumerImpl#messageReceived (the payload is decrypted before it is decompressed)
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenEncryptedAndCompressedMessage_ShouldDecryptBeforeItDecompresses()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        byte[] compressed = [9, 8, 7];
        var decompressed = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("decompressed"));
        var decompressor = CreateDecompressor(input => input.ToArray().SequenceEqual(compressed) ? decompressed : throw new InvalidOperationException("The decompressor was given the payload that was not decrypted"));
        await using var uut = CreateUut(CreateConsumerKeyReader(), decompressor: decompressor);
        var metadata = CreateMetadata();
        metadata.Compression = CompressionType.Lz4;
        metadata.UncompressedSize = 12;
        uut.Enqueue(CreateEncryptedPackage(producer, metadata, 1, compressed));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(actual).ShouldBe("decompressed");
        actual.GetEncryptionContext()!.CompressionType.ShouldBe(DotPulsar.CompressionType.Lz4);
        actual.GetEncryptionContext()!.UncompressedMessageSize.ShouldBe(12U);
        uut.Acks.ShouldBeEmpty();
    }

    // Upstream: ConsumerImpl#messageReceived (a batch is decrypted as a whole)
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenEncryptedBatch_ShouldUnpackItAndGiveEveryMessageTheSameEncryptionContext()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        await using var uut = CreateUut(CreateConsumerKeyReader());
        uut.Enqueue(CreateEncryptedBatchPackage(producer, 1, 3));

        //Act
        var first = await uut.Channel.Receive(Current.CancellationToken);
        var second = await uut.Channel.Receive(Current.CancellationToken);
        var third = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        first.Data.ToArray().ShouldBe(new byte[] { 0 });
        second.Data.ToArray().ShouldBe(new byte[] { 1 });
        third.Data.ToArray().ShouldBe(new byte[] { 2 });
        second.MessageId.BatchIndex.ShouldBe(1);
        first.GetEncryptionContext().ShouldNotBeNull().IsEncrypted.ShouldBeFalse();
        first.GetEncryptionContext()!.BatchSize.ShouldBe(3);
        second.GetEncryptionContext().ShouldBeSameAs(first.GetEncryptionContext());
        third.GetEncryptionContext().ShouldBeSameAs(first.GetEncryptionContext());
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenSecondEncryptedMessageOfTheProducer_ShouldNotReadTheKeyAgain()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        var keyReader = Substitute.For<ICryptoKeyReader>();
        keyReader.GetPrivateKey(KeyName, Arg.Any<IReadOnlyDictionary<string, string>>()).Returns(new EncryptionKeyInfo(CryptoTestKeys.Private("ClientRsa")));
        await using var uut = CreateUut(keyReader);
        uut.Enqueue(CreateEncryptedPackage(producer, CreateMetadata(), 1, "one"));
        uut.Enqueue(CreateEncryptedPackage(producer, CreateMetadata(), 2, "two"));

        //Act
        var first = await uut.Channel.Receive(Current.CancellationToken);
        var second = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(first).ShouldBe("one");
        Text(second).ShouldBe("two");
        keyReader.Received(1).GetPrivateKey(KeyName, Arg.Any<IReadOnlyDictionary<string, string>>());
    }

    // Upstream: ConsumerImpl#handleCryptoFailure (CONSUME)
    [Theory(Timeout = 30_000)]
    [InlineData(Cause.NoCryptoKeyReader)]
    [InlineData(Cause.WrongKey)]
    public async Task Receive_GivenUndecryptableMessageAndConsume_ShouldDeliverTheEncryptedPayloadAndTheEncryptionContext(Cause cause)
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        await using var uut = CreateUut(CreateUndecryptableKeyReader(cause), ConsumerCryptoFailureAction.Consume);
        var metadata = CreateMetadata();
        var package = CreateEncryptedPackage(producer, metadata, 1, "Hello");
        uut.Enqueue(package);

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.Data.Length.ShouldBe("Hello".Length + 16);
        actual.Data.ToArray().ShouldBe(GetPayload(package));
        var context = actual.GetEncryptionContext().ShouldNotBeNull();
        context.IsEncrypted.ShouldBeTrue();
        context.Keys.Keys.ShouldBe([KeyName]);
        context.Param.ShouldBe(metadata.EncryptionParam.ToByteArray());
        uut.Acks.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenUndecryptableCompressedMessageAndConsume_ShouldNotDecompressIt()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        var decompressor = CreateDecompressor(_ => throw new InvalidOperationException("The payload is encrypted"));
        await using var uut = CreateUut(null, ConsumerCryptoFailureAction.Consume, decompressor: decompressor);
        var metadata = CreateMetadata();
        metadata.Compression = CompressionType.Lz4;
        metadata.UncompressedSize = 99;
        uut.Enqueue(CreateEncryptedPackage(producer, metadata, 1, [1, 2, 3]));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.Data.Length.ShouldBe(3 + 16);
        actual.GetEncryptionContext()!.IsEncrypted.ShouldBeTrue();
        actual.GetEncryptionContext()!.CompressionType.ShouldBe(DotPulsar.CompressionType.Lz4);
        actual.GetEncryptionContext()!.UncompressedMessageSize.ShouldBe(99U);
    }

    // Upstream: ConsumerImpl#messageReceived (isMessageUndecryptable, the batch is not unpacked)
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenUndecryptableBatchAndConsume_ShouldDeliverItAsOneMessage()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        await using var uut = CreateUut(null, ConsumerCryptoFailureAction.Consume);
        var package = CreateEncryptedBatchPackage(producer, 1, 3);
        uut.Enqueue(package);
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 2)));

        //Act
        var batch = await uut.Channel.Receive(Current.CancellationToken);
        var next = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        batch.Data.ToArray().ShouldBe(GetPayload(package));
        batch.MessageId.BatchIndex.ShouldBe(-1);
        batch.GetEncryptionContext()!.IsEncrypted.ShouldBeTrue();
        batch.GetEncryptionContext()!.BatchSize.ShouldBe(3);
        next.MessageId.EntryId.ShouldBe(2UL);
    }

    // Upstream: ConsumerImpl#handleCryptoFailure (DISCARD), ConsumerImpl#discardMessage
    [Theory(Timeout = 30_000)]
    [InlineData(Cause.NoCryptoKeyReader)]
    [InlineData(Cause.WrongKey)]
    public async Task Receive_GivenUndecryptableMessageAndDiscard_ShouldSendAckWithDecryptionErrorAndNotDeliverIt(Cause cause)
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        await using var uut = CreateUut(CreateUndecryptableKeyReader(cause), ConsumerCryptoFailureAction.Discard);
        uut.Enqueue(CreateEncryptedPackage(producer, CreateMetadata(), 1, "Hello"));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 2)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(2UL);
        var ack = uut.Acks.ShouldHaveSingleItem();
        ack.AckType.ShouldBe(CommandAck.Types.AckType.Individual);
        ack.ValidationError.ShouldBe(CommandAck.Types.ValidationError.DecryptionError);
        ack.ConsumerId.ShouldBe(1UL);
        ack.MessageId.ShouldHaveSingleItem().EntryId.ShouldBe(1UL);
    }

    // Upstream: ConsumerImpl#handleCryptoFailure (FAIL)
    [Theory(Timeout = 30_000)]
    [InlineData(Cause.NoCryptoKeyReader)]
    [InlineData(Cause.WrongKey)]
    public async Task Receive_GivenUndecryptableMessageAndFail_ShouldNeitherAcknowledgeNorDeliverItAndTrackIt(Cause cause)
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        using var tracker = CreateTracker();
        await using var uut = CreateUut(CreateUndecryptableKeyReader(cause), ConsumerCryptoFailureAction.Fail, tracker);
        uut.Enqueue(CreateEncryptedPackage(producer, CreateMetadata(), 5, "Hello"));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 6)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(6UL);
        uut.Acks.ShouldBeEmpty();
        tracker.Count.ShouldBe(1);
        tracker.Remove(Ledger, 5).ShouldBeTrue();
    }

    // Upstream: ConsumerImpl#handleCryptoFailure (FAIL, the redelivery count that the message is tracked with)
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenUndecryptableMessageWithRedeliveryCountAndFail_ShouldTrackItWithTheRedeliveryCount()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        var tracker = Substitute.For<IUnAckedMessageTracker>();
        await using var uut = CreateUut(CreateUndecryptableKeyReader(Cause.NoCryptoKeyReader), ConsumerCryptoFailureAction.Fail, tracker);
        uut.Enqueue(CreateEncryptedPackage(producer, CreateMetadata(), 5, "Hello", 3));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 6)));

        //Act
        _ = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        tracker.Received(1).Add(Arg.Is<MessageId>(id => id.LedgerId == Ledger && id.EntryId == 5), 3u);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenUndecryptableMessageAndFailWithoutAckTimeout_ShouldNeitherAcknowledgeNorDeliverIt()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        await using var uut = CreateUut(null);
        uut.Enqueue(CreateEncryptedPackage(producer, CreateMetadata(), 1, "Hello"));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 2)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(2UL);
        uut.Acks.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenUndecryptableBatchAndFail_ShouldTrackTheEntry()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        using var tracker = CreateTracker();
        await using var uut = CreateUut(null, tracker: tracker);
        uut.Enqueue(CreateEncryptedBatchPackage(producer, 4, 3));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 5)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(5UL);
        tracker.Count.ShouldBe(1);
        tracker.Remove(Ledger, 4).ShouldBeTrue();
    }

    // Upstream: ConsumerImpl#messageReceived (increaseAvailablePermits), ConsumerImpl#discardMessage
    // The prefetch count of 10 is halved by the first flow, so the second flow is due after 5 messages
    [Theory(Timeout = 30_000)]
    [InlineData(ConsumerCryptoFailureAction.Fail)]
    [InlineData(ConsumerCryptoFailureAction.Discard)]
    [InlineData(ConsumerCryptoFailureAction.Consume)]
    public async Task Receive_GivenUndecryptableBatchOfFiveMessages_ShouldCountEveryMessageOfTheBatchAsReceived(ConsumerCryptoFailureAction action)
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        await using var uut = CreateUut(null, action, prefetchCount: 10);
        uut.Enqueue(CreateEncryptedBatchPackage(producer, 1, 5));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 2)));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 3)));

        //Act
        var first = await uut.Channel.Receive(Current.CancellationToken);
        var flowsAfterBatch = CountFlows(uut);
        var second = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        if (action == ConsumerCryptoFailureAction.Consume)
        {
            first.MessageId.EntryId.ShouldBe(1UL);
            second.MessageId.EntryId.ShouldBe(2UL);
        }
        else
        {
            first.MessageId.EntryId.ShouldBe(2UL);
            second.MessageId.EntryId.ShouldBe(3UL);
        }

        flowsAfterBatch.ShouldBe(action == ConsumerCryptoFailureAction.Consume ? 1 : 2);
        CountFlows(uut).ShouldBe(2);
    }

    // The prefetch count of 20 is halved by the first flow, so the second flow is due after 10 messages. The batch counts as 5 of them.
    // Additional coverage
    [Theory(Timeout = 30_000)]
    [InlineData(ConsumerCryptoFailureAction.Fail)]
    [InlineData(ConsumerCryptoFailureAction.Discard)]
    public async Task Receive_GivenUndecryptableBatchOfFiveMessages_ShouldNotCountMoreThanTheMessagesOfTheBatchAsReceived(ConsumerCryptoFailureAction action)
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        await using var uut = CreateUut(null, action, prefetchCount: 20);
        uut.Enqueue(CreateEncryptedBatchPackage(producer, 1, 5));

        for (ulong entryId = 2; entryId <= 7; ++entryId)
        {
            uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, entryId)));
        }

        //Act
        for (var i = 0; i < 5; ++i)
        {
            await uut.Channel.Receive(Current.CancellationToken);
        }

        var flowsBefore = CountFlows(uut);
        await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        flowsBefore.ShouldBe(1);
        CountFlows(uut).ShouldBe(2);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenUndecryptableMessageThatIsNotABatch_ShouldCountItAsOneMessageReceived()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        await using var uut = CreateUut(null, ConsumerCryptoFailureAction.Discard, prefetchCount: 10);
        uut.Enqueue(CreateEncryptedPackage(producer, CreateMetadata(), 1, "Hello"));
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 2)));

        //Act
        await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        CountFlows(uut).ShouldBe(1);
    }

    // Upstream: ConsumerImpl#doProcessMessageChunk (every chunk is decrypted before it is assembled)
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenEncryptedChunks_ShouldDecryptEveryChunkBeforeAssemblingThem()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        await using var uut = CreateUut(CreateConsumerKeyReader());
        var lastMetadata = EnqueueEncryptedChunks(uut, producer, "a", 3, 10);

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        Text(actual).ShouldBe("chunk-a-0|chunk-a-1|chunk-a-2|");
        actual.MessageId.GetChunkMessageIds()!.Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL, 12UL);
        var context = actual.GetEncryptionContext().ShouldNotBeNull();
        context.IsEncrypted.ShouldBeFalse();
        context.Param.ShouldBe(lastMetadata.EncryptionParam.ToByteArray());
        uut.Acks.ShouldBeEmpty();
        uut.Assembler.PendingCount.ShouldBe(0);
    }

    // The chunks of a message are encrypted on their own, so the assembler cannot make a message of them as long as they are encrypted. A chunk is handled as if the action was 'Fail'.
    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenUndecryptableChunksAndConsume_ShouldTrackEveryChunkAndDeliverNone()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        using var tracker = CreateTracker();
        await using var uut = CreateUut(null, ConsumerCryptoFailureAction.Consume, tracker);
        EnqueueEncryptedChunks(uut, producer, "a", 3, 10);
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 20)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(20UL);
        actual.GetEncryptionContext().ShouldBeNull();
        uut.Acks.ShouldBeEmpty();
        tracker.Count.ShouldBe(3);
        uut.Assembler.PendingCount.ShouldBe(0);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenUndecryptableChunksAndDiscard_ShouldRejectEveryChunk()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        await using var uut = CreateUut(null, ConsumerCryptoFailureAction.Discard);
        EnqueueEncryptedChunks(uut, producer, "a", 3, 10);
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 20)));

        //Act
        var actual = await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        actual.MessageId.EntryId.ShouldBe(20UL);
        uut.Acks.ShouldAllBe(ack => ack.ValidationError == CommandAck.Types.ValidationError.DecryptionError);
        uut.Acks.SelectMany(ack => ack.MessageId).Select(id => id.EntryId).ShouldBeSequence(10UL, 11UL, 12UL);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenUndecryptableChunksAndFail_ShouldTrackEveryChunkAndAcknowledgeNone()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        using var tracker = CreateTracker();
        await using var uut = CreateUut(null, tracker: tracker);
        EnqueueEncryptedChunks(uut, producer, "a", 3, 10);
        uut.Enqueue(BatchTestData.CreateSinglePackage(BatchTestData.CreateEntryId(Ledger, 20)));

        //Act
        await uut.Channel.Receive(Current.CancellationToken);

        //Assert
        uut.Acks.ShouldBeEmpty();
        tracker.Count.ShouldBe(3);
    }

    // Additional coverage
    [Fact(Timeout = 30_000)]
    public async Task Receive_GivenDisposedMessageCrypto_ShouldThrowObjectDisposedException()
    {
        //Arrange
        using var producer = CreateProducerCrypto();
        var messageCrypto = new MessageCrypto(false);
        await using var uut = CreateUut(CreateConsumerKeyReader(), messageCrypto: messageCrypto);
        messageCrypto.Dispose();
        uut.Enqueue(CreateEncryptedPackage(producer, CreateMetadata(), 1, "Hello"));

        //Act
        var exception = await Record.ExceptionAsync(async () => await uut.Channel.Receive(Current.CancellationToken));

        //Assert
        exception.ShouldBeOfType<ObjectDisposedException>();
    }

    public enum Cause
    {
        NoCryptoKeyReader,
        WrongKey
    }

    private static ICryptoKeyReader CreateConsumerKeyReader()
        => new InMemoryCryptoKeyReader().WithPrivateKey(KeyName, CryptoTestKeys.Private("ClientRsa"));

    // A reader with the key of another producer, or no reader at all, makes the messages undecryptable
    private static ICryptoKeyReader? CreateUndecryptableKeyReader(Cause cause)
        => cause == Cause.NoCryptoKeyReader ? null : new InMemoryCryptoKeyReader().WithPrivateKey(KeyName, CryptoTestKeys.Private("ClientRsaPkcs8"));

    private static MessageCrypto CreateProducerCrypto()
    {
        var producer = new MessageCrypto(true);
        producer.AddPublicKeyCipher([KeyName], new InMemoryCryptoKeyReader().WithPublicKey(KeyName, CryptoTestKeys.Public("ClientRsa")));
        return producer;
    }

    private static MessageMetadata CreateMetadata()
        => new() { ProducerName = "producer", SequenceId = 1, PublishTime = 1 };

    private static MessagePackage CreateEncryptedPackage(MessageCrypto producer, MessageMetadata metadata, ulong entryId, string text, uint redeliveryCount = 0)
        => CreateEncryptedPackage(producer, metadata, entryId, Encoding.UTF8.GetBytes(text), redeliveryCount);

    private static MessagePackage CreateEncryptedPackage(MessageCrypto producer, MessageMetadata metadata, ulong entryId, byte[] payload, uint redeliveryCount = 0)
    {
        var encrypted = producer.Encrypt(metadata, new ReadOnlySequence<byte>(payload));
        return BatchTestData.CreatePackage(BatchTestData.CreateEntryId(Ledger, entryId), metadata, encrypted.ToArray(), redeliveryCount: redeliveryCount);
    }

    private static MessagePackage CreateEncryptedBatchPackage(MessageCrypto producer, ulong entryId, int numberOfMessages)
        => CreateEncryptedPackage(producer, BatchTestData.CreateMetadata(numberOfMessages), entryId, BatchTestData.CreateBatchPayload(numberOfMessages));

    // The chunks have their own IV, and total_chunk_msg_size is the size before they are encrypted. Returns the metadata of the last chunk.
    private static MessageMetadata EnqueueEncryptedChunks(Context uut, MessageCrypto producer, string uuid, int numberOfChunks, ulong firstEntryId)
    {
        var payloads = Enumerable.Range(0, numberOfChunks).Select(chunkId => Encoding.UTF8.GetBytes(ChunkTestData.CreateChunkText(uuid, chunkId))).ToArray();
        var totalSize = payloads.Sum(payload => payload.Length);
        MessageMetadata? metadata = null;

        for (var chunkId = 0; chunkId < numberOfChunks; ++chunkId)
        {
            metadata = ChunkTestData.CreateChunkMetadata(uuid, chunkId, numberOfChunks, totalSize);
            uut.Enqueue(CreateEncryptedPackage(producer, metadata, firstEntryId + (ulong) chunkId, payloads[chunkId]));
        }

        return metadata!;
    }

    private static byte[] GetPayload(MessagePackage package)
    {
        var data = package.Data.ToArray();
        var metadataSize = (int) System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(6, 4));
        return data.AsSpan(10 + metadataSize).ToArray();
    }

    private static Context CreateUut(
        ICryptoKeyReader? cryptoKeyReader,
        ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.Fail,
        IUnAckedMessageTracker? tracker = null,
        IDecompressorFactory? decompressor = null,
        uint prefetchCount = 100,
        MessageCrypto? messageCrypto = null)
    {
        var assembler = ChunkTestData.CreateAssembler();
        var acks = new List<CommandAck>();
        var connection = Substitute.For<IConnection>();
        connection.Send(Arg.Do<CommandAck>(ack => acks.Add(ack.Clone())), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var queue = new AsyncQueue<MessagePackage>();
        var messageFactory = new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray);
        var batchHandler = new BatchHandler<byte[]>(true, messageFactory, BatchTestData.Topic, null);
        IDecompressorFactory[] decompressors = decompressor is null ? [] : [decompressor];
        messageCrypto ??= cryptoKeyReader is null ? null : new MessageCrypto(false);
        var channel = new ConsumerChannel<byte[]>(1, prefetchCount, queue, connection, batchHandler, messageFactory, decompressors, BatchTestData.Topic, false, false, null, tracker, null, assembler, messageCrypto, cryptoKeyReader, cryptoFailureAction);

        return new Context(channel, queue, connection, acks, assembler, messageCrypto);
    }

    private static UnAckedMessageTracker CreateTracker()
        => new(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(1), (_, _) => default);

    private static IDecompressorFactory CreateDecompressor(Func<ReadOnlySequence<byte>, ReadOnlySequence<byte>> decompress)
    {
        var decompressor = Substitute.For<IDecompress>();
        decompressor.Decompress(Arg.Any<ReadOnlySequence<byte>>(), Arg.Any<int>()).Returns(callInfo => decompress(callInfo.Arg<ReadOnlySequence<byte>>()));
        var factory = Substitute.For<IDecompressorFactory>();
        factory.CompressionType.Returns(CompressionType.Lz4);
        factory.Create().Returns(decompressor);
        return factory;
    }

    private static string Text(IMessage<byte[]> message)
        => Encoding.UTF8.GetString(message.Data.ToArray());

    private static int CountFlows(Context uut)
        => uut.Connection.ReceivedCalls().Count(call => call.GetArguments().FirstOrDefault() is CommandFlow);

    private sealed class Context : IAsyncDisposable
    {
        private readonly AsyncQueue<MessagePackage> _queue;
        private readonly MessageCrypto? _messageCrypto;

        public Context(ConsumerChannel<byte[]> channel, AsyncQueue<MessagePackage> queue, IConnection connection, List<CommandAck> acks, ChunkedMessageAssembler assembler, MessageCrypto? messageCrypto)
        {
            Channel = channel;
            _queue = queue;
            Connection = connection;
            Acks = acks;
            Assembler = assembler;
            _messageCrypto = messageCrypto;
        }

        public ConsumerChannel<byte[]> Channel { get; }
        public IConnection Connection { get; }
        public List<CommandAck> Acks { get; }
        public ChunkedMessageAssembler Assembler { get; }

        public void Enqueue(MessagePackage messagePackage)
            => _queue.Enqueue(messagePackage);

        public async ValueTask DisposeAsync()
        {
            await Channel.DisposeAsync();
            Assembler.Dispose();
            _messageCrypto?.Dispose();
        }
    }
}
