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

namespace DotPulsar.Tests.Internal.Batching;

using DotPulsar.Internal;
using DotPulsar.Internal.Abstractions;
using DotPulsar.Internal.Batching;
using DotPulsar.Internal.Compression;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;

// The batch of the container is sent by the ProducerChannel as it is, which also compresses the whole payload of the batch
[Trait("Category", "Unit")]
public sealed class BatchSendThroughProducerChannelTests
{
    private const int MaxMessageSize = 5 * 1024 * 1024;

    [Fact]
    public async Task Send_GivenBatchAndCompression_ShouldGiveTheConnectionTheBatchMetadataAndTheCompressedPayload()
    {
        //Arrange
        var compressorFactory = CompressionFactories.CompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        var decompressorFactory = CompressionFactories.DecompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        var sent = new List<(MessageMetadata Metadata, byte[] Payload, ulong CommandSequenceId)>();
        var connection = CreateConnection(sent);
        var uut = new ProducerChannel(5, "producer", connection, compressorFactory, null, null, ProducerCryptoFailureAction.Fail);
        var batch = CreateBatch(20, (7, 13));
        var uncompressed = batch.Payload.ToArray();

        //Act
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);

        //Assert
        sent.Count.ShouldBe(1);
        var metadata = sent[0].Metadata;
        metadata.ProducerName.ShouldBe("producer");
        metadata.NumMessagesInBatch.ShouldBe(20);
        metadata.SequenceId.ShouldBe(0ul);
        metadata.HighestSequenceId.ShouldBe(19ul);
        metadata.TxnidMostBits.ShouldBe(7ul);
        metadata.TxnidLeastBits.ShouldBe(13ul);
        metadata.Compression.ShouldBe(CompressionType.Zlib);
        metadata.UncompressedSize.ShouldBe((uint) uncompressed.Length);
        sent[0].CommandSequenceId.ShouldBe(0ul);

        using var decompressor = decompressorFactory.Create();
        var decompressed = decompressor.Decompress(new ReadOnlySequence<byte>(sent[0].Payload), uncompressed.Length).ToArray();
        decompressed.ShouldBe(uncompressed);
        BatchPayloadReader.Read(decompressed, 20).Count.ShouldBe(20);

        //Annihilate
        batch.Return();
    }

    [Fact]
    public async Task Send_GivenBatchThatIsSentTwice_ShouldSendTheSamePayloadAndMetadataBothTimes()
    {
        //Arrange
        var compressorFactory = CompressionFactories.CompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        var sent = new List<(MessageMetadata Metadata, byte[] Payload, ulong CommandSequenceId)>();
        var uut = new ProducerChannel(5, "producer", CreateConnection(sent), compressorFactory, null, null, ProducerCryptoFailureAction.Fail);
        var batch = CreateBatch(5, null);

        //Act
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);

        //Assert
        sent.Count.ShouldBe(2);
        sent[1].Payload.ShouldBe(sent[0].Payload);
        sent[1].Metadata.Compression.ShouldBe(sent[0].Metadata.Compression);
        sent[1].Metadata.UncompressedSize.ShouldBe(sent[0].Metadata.UncompressedSize);
        sent[1].Metadata.NumMessagesInBatch.ShouldBe(5);
        sent[1].Metadata.SequenceId.ShouldBe(sent[0].Metadata.SequenceId);
        sent[1].Metadata.HighestSequenceId.ShouldBe(sent[0].Metadata.HighestSequenceId);

        //Annihilate
        batch.Return();
    }

    [Fact]
    public async Task Send_GivenBatchWithoutCompression_ShouldGiveTheConnectionTheUncompressedPayload()
    {
        //Arrange
        var sent = new List<(MessageMetadata Metadata, byte[] Payload, ulong CommandSequenceId)>();
        var uut = new ProducerChannel(5, "producer", CreateConnection(sent), null, null, null, ProducerCryptoFailureAction.Fail);
        var batch = CreateBatch(3, null);
        var expected = batch.Payload.ToArray();

        //Act
        await uut.Send(batch.Metadata, batch.Payload, new TaskCompletionSource<BaseCommand>(), CancellationToken.None);

        //Assert
        sent[0].Payload.ShouldBe(expected);
        sent[0].Metadata.HasCompression.ShouldBeFalse();
        sent[0].Metadata.HasTxnidMostBits.ShouldBeFalse();

        //Annihilate
        batch.Return();
    }

    private static IConnection CreateConnection(List<(MessageMetadata Metadata, byte[] Payload, ulong CommandSequenceId)> sent)
    {
        var connection = Substitute.For<IConnection>();
        connection
            .When(c => c.Send(Arg.Any<SendPackage>(), Arg.Any<TaskCompletionSource<BaseCommand>>(), Arg.Any<CancellationToken>()))
            .Do(call =>
            {
                var package = call.Arg<SendPackage>();
                sent.Add((package.Metadata!.Clone(), package.Payload.ToArray(), package.Command!.SequenceId));
            });

        return connection;
    }

    private static PreparedBatch CreateBatch(int numberOfMessages, (ulong Most, ulong Least)? transaction)
    {
        var container = new BatchMessageContainer<TestBatchItem>(1000, 128 * 1024, TimeSpan.FromMilliseconds(1));
        var groups = new List<PreparedBatchGroup>();

        for (var i = 0; i < numberOfMessages; ++i)
        {
            container.Add(TestBatchItem.Create((ulong) i, $"payload-{i}-{new string('x', 30)}", transaction: transaction), MaxMessageSize);
        }

        container.CreateBatches(groups, MaxMessageSize);

        return groups[0].Batch!;
    }
}
