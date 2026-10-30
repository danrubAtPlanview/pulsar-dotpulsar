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

using DotPulsar.Internal.Batching;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using System.Buffers;

// The expected bytes were produced by the Java client, see JavaKnownAnswer
[Trait("Category", "Unit")]
public sealed class BatchMessageContainerKnownAnswerTests
{
    private const int MaxMessageSize = 5 * 1024 * 1024;

    // Upstream: Commands#serializeSingleMessageInBatchWithPayload
    [Fact]
    public void CreateBatches_GivenKnownMessages_ShouldProduceTheSamePayloadAsTheJavaClient()
    {
        //Arrange
        var uut = new BatchMessageContainer<TestBatchItem>(1000, 128 * 1024, TimeSpan.FromMilliseconds(1));
        var groups = new List<PreparedBatchGroup>();
        foreach (var item in JavaKnownAnswer.CreateItems())
        {
            uut.Add(item, MaxMessageSize);
        }

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        BatchPayloadReader.ToHex(groups[0].Batch!.Payload.ToArray()).ShouldBe(JavaKnownAnswer.Payload);
        groups[0].Batch!.Payload.Length.ShouldBe(JavaKnownAnswer.PayloadSize);
    }

    // Upstream: Commands#initBatchMessageMetadata
    [Fact]
    public void CreateBatches_GivenKnownTransactionalMessagesAndCompression_ShouldProduceTheSameMetadataAsTheJavaClient()
    {
        //Arrange
        var uut = new BatchMessageContainer<TestBatchItem>(1000, 128 * 1024, TimeSpan.FromMilliseconds(1));
        var groups = new List<PreparedBatchGroup>();
        foreach (var item in JavaKnownAnswer.CreateItems((7, 13)))
        {
            uut.Add(item, MaxMessageSize);
        }

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        // The producer name, the publish time and the compression are set by ProducerChannel when the batch is sent
        var metadata = groups[0].Batch!.Metadata;
        metadata.ProducerName = JavaKnownAnswer.ProducerName;
        metadata.PublishTime = JavaKnownAnswer.PublishTime;
        metadata.Compression = CompressionType.Zlib;
        metadata.UncompressedSize = JavaKnownAnswer.PayloadSize;
        BatchPayloadReader.ToHex(metadata.ToByteArray()).ShouldBe(JavaKnownAnswer.TransactionalCompressedMetadata);
    }

    // Upstream: Commands#initBatchMessageMetadata
    [Fact]
    public void CreateBatches_GivenKnownPlainMessages_ShouldProduceTheSameMetadataAsTheJavaClient()
    {
        //Arrange
        var uut = new BatchMessageContainer<TestBatchItem>(1000, 128 * 1024, TimeSpan.FromMilliseconds(1));
        var groups = new List<PreparedBatchGroup>();
        foreach (var item in JavaKnownAnswer.CreateItems())
        {
            uut.Add(item, MaxMessageSize);
        }

        //Act
        uut.CreateBatches(groups, MaxMessageSize);

        //Assert
        var metadata = groups[0].Batch!.Metadata;
        metadata.ProducerName = JavaKnownAnswer.ProducerName;
        metadata.PublishTime = JavaKnownAnswer.PublishTime;
        BatchPayloadReader.ToHex(metadata.ToByteArray()).ShouldBe(JavaKnownAnswer.PlainMetadata);
    }
}
