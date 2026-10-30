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
using System.Buffers.Binary;

[Trait("Category", "Unit")]
public sealed class BatchPayloadWriterTests
{
    // Upstream: Commands#serializeSingleMessageInBatchWithPayload
    [Fact]
    public void Write_GivenKnownMessages_ShouldWriteTheSameBytesAsTheJavaClient()
    {
        //Arrange
        var uut = new BatchPayloadWriter();
        using var output = new TestOutput();

        //Act
        foreach (var item in JavaKnownAnswer.CreateItems())
        {
            uut.Write(output.Writer, item.Metadata, item.Data);
        }

        //Assert
        BatchPayloadReader.ToHex(output.Writer.WrittenSpan.ToArray()).ShouldBe(JavaKnownAnswer.Payload);
    }

    [Fact]
    public void Write_GivenMessage_ShouldWriteBigEndianSizeThenMetadataThenPayload()
    {
        //Arrange
        var uut = new BatchPayloadWriter();
        using var output = new TestOutput();
        var item = TestBatchItem.Create(7, "hello", key: "k");
        var expectedMetadata = new SingleMessageMetadata { PartitionKey = "k", PartitionKeyB64Encoded = false, PayloadSize = 5, SequenceId = 7 }.ToByteArray();

        //Act
        uut.Write(output.Writer, item.Metadata, item.Data);

        //Assert
        var actual = output.Writer.WrittenSpan.ToArray();
        BinaryPrimitives.ReadUInt32BigEndian(actual.AsSpan(0, 4)).ShouldBe((uint) expectedMetadata.Length);
        actual.AsSpan(4, expectedMetadata.Length).ToArray().ShouldBe(expectedMetadata);
        actual.AsSpan(4 + expectedMetadata.Length).ToArray().ShouldBe("hello"u8.ToArray());
    }

    [Fact]
    public void Write_GivenMessageWithPartitionKey_ShouldAlwaysWriteThePartitionKeyB64EncodedFlag()
    {
        //Arrange
        var uut = new BatchPayloadWriter();
        using var output = new TestOutput();
        var item = TestBatchItem.Create(1, "x", key: "k");
        item.Metadata.HasPartitionKeyB64Encoded.ShouldBeFalse();

        //Act
        uut.Write(output.Writer, item.Metadata, item.Data);

        //Assert
        var message = BatchPayloadReader.Read(output.Writer.WrittenSpan.ToArray(), 1)[0];
        message.Metadata.HasPartitionKeyB64Encoded.ShouldBeTrue();
        message.Metadata.PartitionKeyB64Encoded.ShouldBeFalse();
    }

    [Fact]
    public void Write_GivenMessageWithoutFieldsAfterMessageWithAllFields_ShouldNotLeakFieldsOfThePreviousMessage()
    {
        //Arrange
        var uut = new BatchPayloadWriter();
        using var output = new TestOutput();
        var full = JavaKnownAnswer.CreateItems()[0];
        full.Metadata.OrderingKey = ByteString.CopyFrom(1);
        full.Metadata.NullValue = true;
        full.Metadata.NullPartitionKey = true;
        var empty = new TestBatchItem(new MessageMetadata(), new(new byte[] { 9 }));

        //Act
        uut.Write(output.Writer, full.Metadata, full.Data);
        uut.Write(output.Writer, empty.Metadata, empty.Data);

        //Assert
        var messages = BatchPayloadReader.Read(output.Writer.WrittenSpan.ToArray(), 2);
        messages[1].Metadata.ShouldBe(new SingleMessageMetadata { PayloadSize = 1 });
        messages[1].Payload.ShouldBe([9]);
    }

    [Fact]
    public void Write_GivenMessageWithoutSequenceId_ShouldNotWriteASequenceId()
    {
        //Arrange
        var uut = new BatchPayloadWriter();
        using var output = new TestOutput();
        var item = new TestBatchItem(new MessageMetadata(), new(new byte[] { 1 }));

        //Act
        uut.Write(output.Writer, item.Metadata, item.Data);

        //Assert
        BatchPayloadReader.Read(output.Writer.WrittenSpan.ToArray(), 1)[0].Metadata.HasSequenceId.ShouldBeFalse();
    }

    [Fact]
    public void Write_GivenMessageWithProperties_ShouldNotChangeTheProperties()
    {
        //Arrange
        var uut = new BatchPayloadWriter();
        using var output = new TestOutput();
        var item = JavaKnownAnswer.CreateItems()[0];

        //Act
        uut.Write(output.Writer, item.Metadata, item.Data);
        uut.Write(output.Writer, new MessageMetadata(), default);

        //Assert
        item.Metadata.Properties.Count.ShouldBe(2);
        item.Metadata.Properties[0].Key.ShouldBe("k1");
        item.Metadata.Properties[1].Value.ShouldBe("v2");
    }

    [Fact]
    public void Write_GivenMessageLargerThanTheInitialBuffer_ShouldGrowTheBuffer()
    {
        //Arrange
        var uut = new BatchPayloadWriter();
        using var output = new TestOutput();
        var payload = new byte[100_000];
        new Random(1).NextBytes(payload);
        var item = TestBatchItem.Create(1, payload);

        //Act
        uut.Write(output.Writer, item.Metadata, item.Data);

        //Assert
        var message = BatchPayloadReader.Read(output.Writer.WrittenSpan.ToArray(), 1)[0];
        message.Payload.ShouldBe(payload);
    }

    private sealed class TestOutput : IDisposable
    {
        public TestOutput()
            => Writer = new PooledBufferWriter();

        public PooledBufferWriter Writer { get; }

        public void Dispose()
            => Writer.Reset();
    }
}
