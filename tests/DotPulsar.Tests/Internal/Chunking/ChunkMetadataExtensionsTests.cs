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

namespace DotPulsar.Tests.Internal.Chunking;

using DotPulsar.Internal.Chunking;
using DotPulsar.Internal.PulsarApi;

[Trait("Category", "Unit")]
public sealed class ChunkMetadataExtensionsTests
{
    // Upstream: Commands#newSend (L639-L641)
    [Theory]
    [InlineData(0, false)]
    [InlineData(1, false)]
    [InlineData(2, true)]
    [InlineData(100, true)]
    public void IsChunk_GivenTotalChunkMsgSize_ShouldBeTrueOnlyAboveOne(int totalChunkMsgSize, bool expected)
    {
        //Arrange
        var metadata = new MessageMetadata { TotalChunkMsgSize = totalChunkMsgSize };

        //Act
        var actual = metadata.IsChunk();

        //Assert
        actual.ShouldBe(expected);
    }

    // Additional coverage
    [Fact]
    public void IsChunk_GivenNoChunkFields_ShouldBeFalse()
    {
        //Arrange
        var metadata = ChunkTestData.CreateMetadata();

        //Act
        var actual = metadata.IsChunk();

        //Assert
        actual.ShouldBeFalse();
    }

    // Upstream: ConsumerImpl#messageReceived (L1566-L1567), ConsumerImpl.cc#messageReceived (L731-L741)
    [Theory]
    [InlineData(0, false)]
    [InlineData(1, false)]
    [InlineData(2, true)]
    [InlineData(10, true)]
    public void IsChunkedMessage_GivenNumberOfChunks_ShouldBeTrueOnlyAboveOne(int numberOfChunks, bool expected)
    {
        //Arrange
        var metadata = new MessageMetadata { NumChunksFromMsg = numberOfChunks };

        //Act
        var actual = metadata.IsChunkedMessage();

        //Assert
        actual.ShouldBe(expected);
    }

    // Additional coverage
    [Fact]
    public void IsChunkedMessage_GivenNumberOfMessagesInBatch_ShouldBeFalse()
    {
        //Arrange
        var metadata = new MessageMetadata { NumChunksFromMsg = 2, NumMessagesInBatch = 5 };

        //Act
        var actual = metadata.IsChunkedMessage();

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void IsChunkedMessage_GivenMetadataOfAChunk_ShouldBeTrue()
    {
        //Arrange
        var metadata = ChunkTestData.CreateChunkMetadata("uuid", 1, 3);

        //Act
        var actual = metadata.IsChunkedMessage();

        //Assert
        actual.ShouldBeTrue();
    }
}
