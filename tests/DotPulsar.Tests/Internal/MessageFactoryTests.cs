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

using DotPulsar.Internal;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;
using System.Buffers;

[Trait("Category", "Unit")]
public sealed class MessageFactoryTests
{
    private readonly MessageFactory<byte[]> _uut;

    public MessageFactoryTests()
        => _uut = new MessageFactory<byte[]>(DotPulsar.Schema.ByteArray);

    [Fact]
    public void Create_GivenBatchedMessageWithOrderingKeyInSingleMetadata_ShouldUseTheOrderingKeyOfTheSingleMessage()
    {
        //Arrange
        var metadata = CreateMetadata("batch-key");
        var singleMetadata = new SingleMessageMetadata { PayloadSize = 0, OrderingKey = ByteString.CopyFromUtf8("single-key") };

        //Act
        var actual = _uut.Create(new MessageId(1, 2, 0, 0), 0, ReadOnlySequence<byte>.Empty, metadata, singleMetadata);

        //Assert
        actual.HasOrderingKey.ShouldBeTrue();
        actual.OrderingKey.ShouldBe("single-key"u8.ToArray());
    }

    [Fact]
    public void Create_GivenBatchedMessageWithoutOrderingKeyInSingleMetadata_ShouldNotUseTheOrderingKeyOfTheBatch()
    {
        //Arrange
        var metadata = CreateMetadata("batch-key");
        var singleMetadata = new SingleMessageMetadata { PayloadSize = 0 };

        //Act
        var actual = _uut.Create(new MessageId(1, 2, 0, 0), 0, ReadOnlySequence<byte>.Empty, metadata, singleMetadata);

        //Assert
        actual.HasOrderingKey.ShouldBeFalse();
        actual.OrderingKey.ShouldBeNull();
    }

    [Fact]
    public void Create_GivenUnbatchedMessageWithOrderingKey_ShouldUseTheOrderingKeyOfTheMetadata()
    {
        //Arrange
        var metadata = CreateMetadata("message-key");

        //Act
        var actual = _uut.Create(new MessageId(1, 2, 0, -1), 0, ReadOnlySequence<byte>.Empty, metadata);

        //Assert
        actual.OrderingKey.ShouldBe("message-key"u8.ToArray());
    }

    private static MessageMetadata CreateMetadata(string orderingKey)
        => new()
        {
            ProducerName = "test",
            PublishTime = 1,
            SequenceId = 1,
            OrderingKey = ByteString.CopyFromUtf8(orderingKey)
        };
}
