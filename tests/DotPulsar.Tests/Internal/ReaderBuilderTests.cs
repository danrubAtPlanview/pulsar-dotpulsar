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

using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using DotPulsar.Internal;
using DotPulsar.Tests.Internal.Crypto;

[Trait("Category", "Unit")]
public sealed class ReaderBuilderTests
{
    // Additional coverage
    [Fact]
    public void Create_GivenNoChunkSettings_ShouldUseTheDefaults()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.Create();

        //Assert
        options().MaxPendingChunkedMessage.ShouldBe(10U);
        options().AutoAckOldestChunkedMessageOnQueueFull.ShouldBeFalse();
        options().ExpireTimeOfIncompleteChunkedMessage.ShouldBe(TimeSpan.FromMinutes(1));
        ReaderOptions<string>.DefaultMaxPendingChunkedMessage.ShouldBe(10U);
        ReaderOptions<string>.DefaultAutoAckOldestChunkedMessageOnQueueFull.ShouldBeFalse();
        ReaderOptions<string>.DefaultExpireTimeOfIncompleteChunkedMessage.ShouldBe(TimeSpan.FromMinutes(1));
    }

    // Upstream: MessageChunkingTest#testReaderChunkingConfiguration (adapted: the built options are asserted instead of the consumer configuration)
    [Fact]
    public void Create_GivenChunkSettings_ShouldPassThemOnInTheOptions()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.MaxPendingChunkedMessage(12).AutoAckOldestChunkedMessageOnQueueFull(true).ExpireTimeOfIncompleteChunkedMessage(TimeSpan.FromMilliseconds(12)).Create();

        //Assert
        options().MaxPendingChunkedMessage.ShouldBe(12U);
        options().AutoAckOldestChunkedMessageOnQueueFull.ShouldBeTrue();
        options().ExpireTimeOfIncompleteChunkedMessage.ShouldBe(TimeSpan.FromMilliseconds(12));
    }

    // Additional coverage
    [Fact]
    public void Builder_GivenChunkSettings_ShouldReturnTheBuilder()
    {
        //Arrange
        var (uut, _) = CreateBuilder();

        //Act & Assert
        uut.MaxPendingChunkedMessage(1).ShouldBeSameAs(uut);
        uut.AutoAckOldestChunkedMessageOnQueueFull(true).ShouldBeSameAs(uut);
        uut.ExpireTimeOfIncompleteChunkedMessage(TimeSpan.Zero).ShouldBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public void ReaderOptions_GivenNoChanges_ShouldHaveTheDefaultChunkSettings()
    {
        //Act
        var uut = new ReaderOptions<string>(MessageId.Earliest, "persistent://public/default/test", Schema.String);

        //Assert
        uut.MaxPendingChunkedMessage.ShouldBe(ReaderOptions<string>.DefaultMaxPendingChunkedMessage);
        uut.AutoAckOldestChunkedMessageOnQueueFull.ShouldBe(ReaderOptions<string>.DefaultAutoAckOldestChunkedMessageOnQueueFull);
        uut.ExpireTimeOfIncompleteChunkedMessage.ShouldBe(ReaderOptions<string>.DefaultExpireTimeOfIncompleteChunkedMessage);
    }

    // Upstream: ConsumerBuilderImpl#cryptoFailureAction (the default is FAIL)
    [Fact]
    public void Create_GivenNoCryptoSettings_ShouldUseTheDefaults()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.Create();

        //Assert
        options().CryptoKeyReader.ShouldBeNull();
        options().CryptoFailureAction.ShouldBe(ConsumerCryptoFailureAction.Fail);
        ReaderOptions<string>.DefaultCryptoFailureAction.ShouldBe(ConsumerCryptoFailureAction.Fail);
    }

    // Upstream: ReaderBuilderImpl#cryptoKeyReader, ReaderBuilderImpl#cryptoFailureAction
    [Theory]
    [InlineData(ConsumerCryptoFailureAction.Fail)]
    [InlineData(ConsumerCryptoFailureAction.Discard)]
    [InlineData(ConsumerCryptoFailureAction.Consume)]
    public void Create_GivenCryptoKeyReaderAndFailureAction_ShouldPassThemOnInTheOptions(ConsumerCryptoFailureAction cryptoFailureAction)
    {
        //Arrange
        var (uut, options) = CreateBuilder();
        var cryptoKeyReader = Substitute.For<ICryptoKeyReader>();

        //Act
        uut.CryptoKeyReader(cryptoKeyReader).CryptoFailureAction(cryptoFailureAction).Create();

        //Assert
        options().CryptoKeyReader.ShouldBeSameAs(cryptoKeyReader);
        options().CryptoFailureAction.ShouldBe(cryptoFailureAction);
    }

    // Additional coverage
    [Fact]
    public void Builder_GivenCryptoSettings_ShouldReturnTheBuilder()
    {
        //Arrange
        var (uut, _) = CreateBuilder();

        //Act & Assert
        uut.CryptoKeyReader(Substitute.For<ICryptoKeyReader>()).ShouldBeSameAs(uut);
        uut.CryptoFailureAction(ConsumerCryptoFailureAction.Consume).ShouldBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public void ReaderOptions_GivenNoChanges_ShouldHaveTheDefaultCryptoSettings()
    {
        //Act
        var uut = new ReaderOptions<string>(MessageId.Earliest, "persistent://public/default/test", Schema.String);

        //Assert
        uut.CryptoKeyReader.ShouldBeNull();
        uut.CryptoFailureAction.ShouldBe(ReaderOptions<string>.DefaultCryptoFailureAction);
    }

    // Upstream: ReaderBuilderImpl#defaultCryptoKeyReader(String)
    [Fact]
    public void Create_GivenDefaultCryptoKeyReaderWithPrivateKey_ShouldPassAReaderThatReadsItForEveryKeyName()
    {
        //Arrange
        var (uut, options) = CreateBuilder();
        var privateKey = CryptoTestKeys.Private("ClientRsa");

        //Act
        uut.DefaultCryptoKeyReader(CreateDataUri(privateKey)).Create();

        //Assert
        var cryptoKeyReader = options().CryptoKeyReader.ShouldBeOfType<DefaultCryptoKeyReader>();
        cryptoKeyReader.GetPrivateKey("one", new Dictionary<string, string>())!.Key.ShouldBe(privateKey);
        cryptoKeyReader.GetPrivateKey("two", new Dictionary<string, string>())!.Key.ShouldBe(privateKey);
        cryptoKeyReader.GetPublicKey("one", new Dictionary<string, string>()).ShouldBeNull();
    }

    // Upstream: ReaderBuilderImpl#defaultCryptoKeyReader(Map)
    [Fact]
    public void Create_GivenDefaultCryptoKeyReaderWithPrivateKeysByName_ShouldPassAReaderThatReadsThemByKeyName()
    {
        //Arrange
        var (uut, options) = CreateBuilder();
        var privateKey = CryptoTestKeys.Private("ClientRsa");

        //Act
        uut.DefaultCryptoKeyReader(new Dictionary<string, string> { ["one"] = CreateDataUri(privateKey) }).Create();

        //Assert
        var cryptoKeyReader = options().CryptoKeyReader.ShouldBeOfType<DefaultCryptoKeyReader>();
        cryptoKeyReader.GetPrivateKey("one", new Dictionary<string, string>())!.Key.ShouldBe(privateKey);
        cryptoKeyReader.GetPrivateKey("two", new Dictionary<string, string>()).ShouldBeNull();
    }

    private static string CreateDataUri(byte[] key)
        => "data:application/x-pem-file;base64," + Convert.ToBase64String(key);

    private static (IReaderBuilder<string> Builder, Func<ReaderOptions<string>> Options) CreateBuilder()
    {
        ReaderOptions<string>? captured = null;
        var client = Substitute.For<IPulsarClient>();
        client.CreateReader(Arg.Do<ReaderOptions<string>>(options => captured = options)).Returns(Substitute.For<IReader<string>>());

        var builder = new ReaderBuilder<string>(client, Schema.String)
            .StartMessageId(MessageId.Earliest)
            .Topic("persistent://public/default/test");

        return (builder, () => captured ?? throw new InvalidOperationException("The reader was not created"));
    }
}
