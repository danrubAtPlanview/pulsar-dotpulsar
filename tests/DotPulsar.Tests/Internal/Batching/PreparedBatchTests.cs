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
using Google.Protobuf.Reflection;
using System.Buffers;
using System.Collections;

[Trait("Category", "Unit")]
public sealed class PreparedBatchTests
{
    // Additional coverage
    [Fact]
    public void Reset_GivenEveryFieldOfTheMetadataIsSet_ShouldMakeTheMetadataEqualToNewMetadata()
    {
        //Arrange
        var uut = new PreparedBatchPool(1).Get();
        foreach (var field in MessageMetadata.Descriptor.Fields.InFieldNumberOrder())
        {
            SetField(uut.Metadata, field);
        }

        uut.Metadata.ShouldNotBe(new MessageMetadata());

        //Act
        uut.Reset();

        //Assert
        uut.Metadata.ShouldBe(new MessageMetadata());
    }

    // Additional coverage
    [Fact]
    public void Reset_GivenWrittenPayloadAndSequenceIds_ShouldClearThem()
    {
        //Arrange
        var uut = new PreparedBatchPool(1).Get();
        uut.Buffer.GetSpan(10)[0] = 1;
        uut.Buffer.Advance(1);
        uut.Count = 3;
        uut.SequenceId = 4;
        uut.HighestSequenceId = 6;

        //Act
        uut.Reset();

        //Assert
        uut.Payload.Length.ShouldBe(0);
        uut.Buffer.Capacity.ShouldBe(0);
        uut.Count.ShouldBe(0);
        uut.SequenceId.ShouldBe(0ul);
        uut.HighestSequenceId.ShouldBe(0ul);
    }

    // Additional coverage
    [Fact]
    public void Payload_GivenWrittenBytes_ShouldReturnTheWrittenBytes()
    {
        //Arrange
        var uut = new PreparedBatchPool(1).Get();
        uut.Buffer.Initialize(100);
        new byte[] { 1, 2, 3 }.CopyTo(uut.Buffer.GetSpan(3));
        uut.Buffer.Advance(3);

        //Act
        var actual = uut.Payload;

        //Assert
        actual.ToArray().ShouldBe([1, 2, 3]);
        actual.IsSingleSegment.ShouldBeTrue();

        //Annihilate
        uut.Return();
    }

    // Additional coverage
    [Fact]
    public void Return_GivenBatchInUse_ShouldResetItAndMakeItAvailableFromThePool()
    {
        //Arrange
        var pool = new PreparedBatchPool(4);
        var uut = pool.Get();
        uut.Metadata.SequenceId = 5;
        uut.Count = 2;
        uut.Buffer.GetSpan(10)[0] = 1;
        uut.Buffer.Advance(1);

        //Act
        uut.Return();
        var actual = pool.Get();

        //Assert
        actual.ShouldBeSameAs(uut);
        actual.IsReturned.ShouldBeFalse();
        actual.Count.ShouldBe(0);
        actual.Payload.Length.ShouldBe(0);
        actual.Metadata.HasSequenceId.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Return_GivenCalledTwice_ShouldBeGivenToTheNextCallersOnlyOnce()
    {
        //Arrange
        var pool = new PreparedBatchPool(4);
        var uut = pool.Get();

        //Act
        uut.Return();
        uut.Return();
        var isReturned = uut.IsReturned;
        var first = pool.Get();
        var second = pool.Get();

        //Assert
        isReturned.ShouldBeTrue();
        first.ShouldBeSameAs(uut);
        second.ShouldNotBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public void Return_GivenBatchThatIsBeingWritten_ShouldWaitForTheEndOfTheWrite()
    {
        //Arrange
        var pool = new PreparedBatchPool(1);
        var uut = pool.Get();
        uut.BeginWrite();

        //Act
        uut.Return();
        var returnedDuringTheWrite = uut.IsReturned;
        var other = pool.Get();
        uut.EndWrite();
        var returnedAfterTheWrite = uut.IsReturned;
        var actual = pool.Get();

        //Assert
        returnedDuringTheWrite.ShouldBeFalse();
        other.ShouldNotBeSameAs(uut);
        returnedAfterTheWrite.ShouldBeTrue();
        actual.ShouldBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public void EndWrite_GivenNoReturn_ShouldNotReturnTheBatch()
    {
        //Arrange
        var pool = new PreparedBatchPool(1);
        var uut = pool.Get();
        uut.BeginWrite();

        //Act
        uut.EndWrite();

        //Assert
        uut.IsReturned.ShouldBeFalse();
        pool.Get().ShouldNotBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public void Return_GivenBatchThatIsBeingWrittenAndReturnedTwice_ShouldBeGivenToTheNextCallersOnlyOnce()
    {
        //Arrange
        var pool = new PreparedBatchPool(4);
        var uut = pool.Get();
        uut.BeginWrite();

        //Act
        uut.Return();
        uut.Return();
        uut.EndWrite();
        uut.Return();
        var first = pool.Get();
        var second = pool.Get();

        //Assert
        first.ShouldBeSameAs(uut);
        second.ShouldNotBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public void Return_GivenBatchThatWasWrittenAndRentedAgain_ShouldNotKeepTheStateOfTheEarlierWrite()
    {
        //Arrange
        var pool = new PreparedBatchPool(1);
        var uut = pool.Get();
        uut.BeginWrite();
        uut.Return();
        uut.EndWrite();
        var rentedAgain = pool.Get();
        rentedAgain.ShouldBeSameAs(uut);

        //Act
        rentedAgain.Return();

        //Assert
        rentedAgain.IsReturned.ShouldBeTrue();
        pool.Get().ShouldBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public void Return_GivenBatchThatWasRentedAgain_ShouldBeReturnedAgain()
    {
        //Arrange
        var pool = new PreparedBatchPool(4);
        var uut = pool.Get();
        uut.Return();
        var rentedAgain = pool.Get();
        rentedAgain.ShouldBeSameAs(uut);

        //Act
        rentedAgain.Return();
        var actual = pool.Get();

        //Assert
        actual.ShouldBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public void Get_GivenPoolWithoutBatches_ShouldCreateBatchesThatReturnToThePool()
    {
        //Arrange
        var pool = new PreparedBatchPool(4);

        //Act
        var first = pool.Get();
        var second = pool.Get();

        //Assert
        first.ShouldNotBeSameAs(second);
        first.IsReturned.ShouldBeFalse();
        second.IsReturned.ShouldBeFalse();
    }

    // Additional coverage
    [Fact]
    public void Shared_GivenAccessedTwice_ShouldReturnTheSameInstance()
    {
        //Act
        var first = PreparedBatchPool.Shared;
        var second = PreparedBatchPool.Shared;

        //Assert
        first.ShouldBeSameAs(second);
    }

    private static void SetField(MessageMetadata metadata, FieldDescriptor field)
    {
        var accessor = field.Accessor;

        if (field.IsRepeated)
        {
            ((IList) accessor.GetValue(metadata)).Add(CreateValue(field));
            return;
        }

        accessor.SetValue(metadata, CreateValue(field));
    }

    private static object CreateValue(FieldDescriptor field)
        => field.FieldType switch
        {
            FieldType.String => "value",
            FieldType.Bytes => ByteString.CopyFrom(1, 2),
            FieldType.UInt64 => 1UL,
            FieldType.UInt32 => 1U,
            FieldType.Int64 => 1L,
            FieldType.Int32 => 1,
            FieldType.Bool => true,
            FieldType.Enum => CompressionType.Lz4,
            FieldType.Message => Activator.CreateInstance(field.MessageType.ClrType)!,
            _ => throw new NotSupportedException($"The type {field.FieldType} of {field.Name} is not supported by the test")
        };
}
