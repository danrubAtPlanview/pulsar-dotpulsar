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
using DotPulsar.Internal.Batching;
using DotPulsar.Internal.Compression;
using DotPulsar.Internal.PulsarApi;
using System.Buffers;
using System.Text;

// The producer compresses the whole payload of a batch when it sends it (ProducerChannel#Send), like the Java client does
[Trait("Category", "Unit")]
public sealed class BatchCompressionTests
{
    private const int MaxMessageSize = 5 * 1024 * 1024;

    // Additional coverage
    [Theory]
    [InlineData(CompressionType.Lz4, BatcherType.Default)]
    [InlineData(CompressionType.Snappy, BatcherType.Default)]
    [InlineData(CompressionType.Zlib, BatcherType.Default)]
    [InlineData(CompressionType.Zstd, BatcherType.Default)]
    [InlineData(CompressionType.Lz4, BatcherType.KeyBased)]
    [InlineData(CompressionType.Snappy, BatcherType.KeyBased)]
    [InlineData(CompressionType.Zlib, BatcherType.KeyBased)]
    [InlineData(CompressionType.Zstd, BatcherType.KeyBased)]
    public void Compression_GivenBatchPayload_ShouldDecompressToTheMessagesOfTheBatch(CompressionType compressionType, BatcherType batcherType)
    {
        //Arrange
        var compressorFactory = CompressionFactories.CompressorFactories().Single(factory => factory.CompressionType == compressionType);
        var decompressorFactory = CompressionFactories.DecompressorFactories().Single(factory => factory.CompressionType == compressionType);
        var container = BatchMessageContainerFactory.Create<TestBatchItem>(batcherType, 1000, 128 * 1024, TimeSpan.FromMilliseconds(1));
        var groups = new List<PreparedBatchGroup>();
        for (var i = 0; i < 100; ++i)
        {
            container.Add(TestBatchItem.Create((ulong) i, $"payload-{i}-{new string('x', 50)}", key: "key"), MaxMessageSize);
        }

        container.CreateBatches(groups, MaxMessageSize);
        var batch = groups[0].Batch!;
        var uncompressed = batch.Payload.ToArray();

        //Act
        using var compressor = compressorFactory.Create();
        var compressed = compressor.Compress(batch.Payload);
        batch.Metadata.Compression = compressorFactory.CompressionType;
        batch.Metadata.UncompressedSize = (uint) uncompressed.Length;
        using var decompressor = decompressorFactory.Create();
        var decompressed = decompressor.Decompress(compressed, (int) batch.Metadata.UncompressedSize).ToArray();

        //Assert
        compressed.Length.ShouldBeLessThan(uncompressed.Length);
        decompressed.ShouldBe(uncompressed);
        var messages = BatchPayloadReader.Read(decompressed, batch.Metadata.NumMessagesInBatch);
        messages.Count.ShouldBe(100);
        for (var i = 0; i < messages.Count; ++i)
        {
            messages[i].Metadata.SequenceId.ShouldBe((ulong) i);
            Encoding.UTF8.GetString(messages[i].Payload).ShouldBe($"payload-{i}-{new string('x', 50)}");
        }
    }

    // Additional coverage
    [Fact]
    public void Compression_GivenBatchThatWasCompressedAndSentOnce_ShouldBeCompressedAgainWhenResent()
    {
        //Arrange
        var compressorFactory = CompressionFactories.CompressorFactories().Single(factory => factory.CompressionType == CompressionType.Zlib);
        var container = new BatchMessageContainer<TestBatchItem>(1000, 128 * 1024, TimeSpan.FromMilliseconds(1));
        var groups = new List<PreparedBatchGroup>();
        for (var i = 0; i < 10; ++i)
        {
            container.Add(TestBatchItem.Create((ulong) i), MaxMessageSize);
        }

        container.CreateBatches(groups, MaxMessageSize);
        var batch = groups[0].Batch!;
        var payloadBefore = batch.Payload.ToArray();

        //Act
        using var compressor = compressorFactory.Create();
        var first = compressor.Compress(batch.Payload).ToArray();
        var second = compressor.Compress(batch.Payload).ToArray();

        //Assert
        batch.Payload.ToArray().ShouldBe(payloadBefore);
        second.ShouldBe(first);
    }
}
