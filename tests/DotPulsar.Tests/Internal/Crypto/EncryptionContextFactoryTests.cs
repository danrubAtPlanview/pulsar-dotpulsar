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

using DotPulsar.Internal.Crypto;
using DotPulsar.Internal.PulsarApi;
using Google.Protobuf;

[Trait("Category", "Unit")]
public sealed class EncryptionContextFactoryTests
{
    private static MessageMetadata CreateMetadata()
    {
        var metadata = new MessageMetadata { ProducerName = "producer", SequenceId = 1, PublishTime = 1 };
        metadata.EncryptionParam = ByteString.CopyFrom(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        return metadata;
    }

    [Fact]
    public void Create_GivenEncryptedMetadata_ShouldCreateTheContext()
    {
        //Arrange
        var metadata = CreateMetadata();
        var key = new EncryptionKeys { Key = "key-a", Value = ByteString.CopyFrom(9, 8, 7) };
        key.Metadata.Add(new KeyValue { Key = "version", Value = "1.0" });
        metadata.EncryptionKeys.Add(key);
        metadata.EncryptionKeys.Add(new EncryptionKeys { Key = "key-b", Value = ByteString.CopyFrom(6, 5) });
        metadata.EncryptionAlgo = "algorithm";
        metadata.Compression = CompressionType.Lz4;
        metadata.UncompressedSize = 1234;
        metadata.NumMessagesInBatch = 5;

        //Act
        var actual = EncryptionContextFactory.Create(metadata, true);

        //Assert
        actual.Keys.Keys.OrderBy(k => k, StringComparer.Ordinal).ShouldBe(["key-a", "key-b"]);
        actual.Keys["key-a"].KeyValue.ShouldBe([9, 8, 7]);
        actual.Keys["key-a"].Metadata.ShouldBe(new Dictionary<string, string> { ["version"] = "1.0" });
        actual.Keys["key-b"].KeyValue.ShouldBe([6, 5]);
        actual.Keys["key-b"].Metadata.Count.ShouldBe(0);
        actual.Param.ShouldBe([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        actual.Algorithm.ShouldBe("algorithm");
        actual.CompressionType.ShouldBe(DotPulsar.CompressionType.Lz4);
        actual.UncompressedMessageSize.ShouldBe(1234u);
        actual.BatchSize.ShouldBe(5);
        actual.IsEncrypted.ShouldBeTrue();
    }

    [Fact]
    public void Create_GivenMetadataWithoutAlgorithmAndBatch_ShouldReturnNulls()
    {
        //Arrange
        var metadata = CreateMetadata();
        metadata.EncryptionKeys.Add(new EncryptionKeys { Key = "key", Value = ByteString.CopyFrom(1) });

        //Act
        var actual = EncryptionContextFactory.Create(metadata, false);

        //Assert
        actual.Algorithm.ShouldBeNull();
        actual.BatchSize.ShouldBeNull();
        actual.CompressionType.ShouldBe(DotPulsar.CompressionType.None);
        actual.IsEncrypted.ShouldBeFalse();
    }

    [Fact]
    public void Create_GivenDuplicateKeyName_ShouldKeepTheLast()
    {
        //Arrange
        var metadata = CreateMetadata();
        metadata.EncryptionKeys.Add(new EncryptionKeys { Key = "key", Value = ByteString.CopyFrom(1) });
        metadata.EncryptionKeys.Add(new EncryptionKeys { Key = "key", Value = ByteString.CopyFrom(2) });

        //Act
        var actual = EncryptionContextFactory.Create(metadata, true);

        //Assert
        actual.Keys.Count.ShouldBe(1);
        actual.Keys["key"].KeyValue.ShouldBe([2]);
    }

    [Fact]
    public void Create_GivenMetadataThatIsChangedAfterwards_ShouldNotChangeTheContext()
    {
        //Arrange
        var metadata = CreateMetadata();
        metadata.EncryptionKeys.Add(new EncryptionKeys { Key = "key", Value = ByteString.CopyFrom(1) });
        var uut = EncryptionContextFactory.Create(metadata, true);

        //Act
        metadata.EncryptionKeys.Clear();
        metadata.ClearEncryptionParam();

        //Assert
        uut.Keys.Count.ShouldBe(1);
        uut.Param.Length.ShouldBe(12);
    }
}
