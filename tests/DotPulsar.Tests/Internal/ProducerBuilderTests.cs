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
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using DotPulsar.Internal;
using DotPulsar.Tests.Internal.Crypto;

[Trait("Category", "Unit")]
public sealed class ProducerBuilderTests
{
    [Fact]
    public async Task Create_GivenMaxPendingMessagesIsZero_ShouldThrowConfigurationException()
    {
        // Arrange
        await using var client = CreateClient();

        // Act
        var act = () => client
            .NewProducer(Schema.String)
            .Topic("persistent://public/default/my-topic")
            .MaxPendingMessages(0)
            .Create();

        // Assert
        var exception = act.ShouldThrow<ConfigurationException>();
        exception.Message.ShouldBe("ProducerOptions.MaxPendingMessages must be greater than 0");
    }

    // Upstream: ProducerBuilderImplTest#testProducerBuilderImplWhenBatchingMaxPublishDelayPropertyIsNegative
    // Java throws when the delay is set, and DotPulsar when the producer is created.
    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    public void Create_GivenBatchingMaxPublishDelayBelowOneMillisecond_ShouldThrowConfigurationException(int milliseconds)
    {
        // Arrange
        var uut = new ProducerBuilder<string>(Substitute.For<IPulsarClient>(), Schema.String);

        // Act
        var act = () => uut
            .Topic("persistent://public/default/my-topic")
            .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(milliseconds))
            .Create();

        // Assert
        var exception = act.ShouldThrow<ConfigurationException>();
        exception.Message.ShouldBe("ProducerOptions.BatchingMaxPublishDelay must be at least 1 millisecond");
    }

    // Additional coverage
    [Fact]
    public async Task Create_GivenBatchingMaxPublishDelayOfOneMillisecond_ShouldNotThrow()
    {
        // Arrange
        await using var client = CreateClient();

        // Act
        var act = () => client
            .NewProducer(Schema.String)
            .Topic("persistent://public/default/my-topic")
            .BatchingMaxPublishDelay(TimeSpan.FromMilliseconds(1))
            .Create();

        // Assert
        await using var producer = act.ShouldNotThrow();
    }

    // Additional coverage
    [Fact]
    public void Create_GivenBatchingOptions_ShouldCopyThemToProducerOptions()
    {
        // Arrange
        var pulsarClient = Substitute.For<IPulsarClient>();
        ProducerOptions<string>? actual = null;
        pulsarClient.CreateProducer(Arg.Do<ProducerOptions<string>>(options => actual = options)).Returns(Substitute.For<IProducer<string>>());
        var uut = new ProducerBuilder<string>(pulsarClient, Schema.String);

        // Act
        _ = uut
            .Topic("persistent://public/default/my-topic")
            .EnableBatching(true)
            .BatchingMaxMessages(25)
            .BatchingMaxBytes(2048)
            .BatchingMaxPublishDelay(TimeSpan.FromSeconds(5))
            .BatcherType(BatcherType.KeyBased)
            .Create();

        // Assert
        actual.ShouldNotBeNull();
        actual.EnableBatching.ShouldBeTrue();
        actual.BatchingMaxMessages.ShouldBe(25u);
        actual.BatchingMaxBytes.ShouldBe(2048u);
        actual.BatchingMaxPublishDelay.ShouldBe(TimeSpan.FromSeconds(5));
        actual.BatcherType.ShouldBe(BatcherType.KeyBased);
    }

    // Additional coverage
    [Fact]
    public void Create_GivenDefaults_ShouldNotEnableBatching()
    {
        // Arrange
        var pulsarClient = Substitute.For<IPulsarClient>();
        ProducerOptions<string>? actual = null;
        pulsarClient.CreateProducer(Arg.Do<ProducerOptions<string>>(options => actual = options)).Returns(Substitute.For<IProducer<string>>());
        var uut = new ProducerBuilder<string>(pulsarClient, Schema.String);

        // Act
        _ = uut.Topic("persistent://public/default/my-topic").Create();

        // Assert
        actual.ShouldNotBeNull();
        actual.EnableBatching.ShouldBeFalse();
        actual.BatchingMaxMessages.ShouldBe(1000u);
        actual.BatchingMaxBytes.ShouldBe(128u * 1024u);
        actual.BatchingMaxPublishDelay.ShouldBe(TimeSpan.FromMilliseconds(1));
        actual.BatcherType.ShouldBe(BatcherType.Default);
    }

    // Upstream: ProducerBuilderImplTest#testProducerBuilderImplWhenSendTimeoutIsNegative, #testProducerBuilderImplWhenSendTimeoutPropertyIsNegative
    // Java throws when the timeout is set, and DotPulsar when the producer is created.
    [Theory]
    [InlineData(-1)]
    [InlineData(-1000)]
    public void Create_GivenNegativeSendTimeout_ShouldThrowConfigurationException(int milliseconds)
    {
        // Arrange
        var uut = new ProducerBuilder<string>(Substitute.For<IPulsarClient>(), Schema.String);

        // Act
        var act = () => uut
            .Topic("persistent://public/default/my-topic")
            .SendTimeout(TimeSpan.FromMilliseconds(milliseconds))
            .Create();

        // Assert
        var exception = act.ShouldThrow<ConfigurationException>();
        exception.Message.ShouldBe("ProducerOptions.SendTimeout may not be negative");
    }

    // Additional coverage
    [Fact]
    public void Create_GivenSendTimeout_ShouldCopyItToProducerOptions()
    {
        // Arrange
        var pulsarClient = Substitute.For<IPulsarClient>();
        ProducerOptions<string>? actual = null;
        pulsarClient.CreateProducer(Arg.Do<ProducerOptions<string>>(options => actual = options)).Returns(Substitute.For<IProducer<string>>());
        var uut = new ProducerBuilder<string>(pulsarClient, Schema.String);

        // Act
        _ = uut.Topic("persistent://public/default/my-topic").SendTimeout(TimeSpan.FromSeconds(30)).Create();

        // Assert
        actual.ShouldNotBeNull();
        actual.SendTimeout.ShouldBe(TimeSpan.FromSeconds(30));
    }

    // Additional coverage
    [Fact]
    public void Create_GivenDefaultOptions_ShouldHaveZeroSendTimeout()
    {
        // Arrange
        var pulsarClient = Substitute.For<IPulsarClient>();
        ProducerOptions<string>? actual = null;
        pulsarClient.CreateProducer(Arg.Do<ProducerOptions<string>>(options => actual = options)).Returns(Substitute.For<IProducer<string>>());
        var uut = new ProducerBuilder<string>(pulsarClient, Schema.String);

        // Act
        _ = uut.Topic("persistent://public/default/my-topic").Create();

        // Assert
        actual.ShouldNotBeNull();
        actual.SendTimeout.ShouldBe(TimeSpan.Zero);
    }

    // Upstream: MessageChunkingTest (cpp: #testInvalidConfig), ProducerBuilderImpl#createAsync
    // Java throws when the producer is created, and so does DotPulsar.
    [Fact]
    public void Create_GivenChunkingAndBatchingEnabled_ShouldThrowConfigurationException()
    {
        // Arrange
        var uut = new ProducerBuilder<string>(Substitute.For<IPulsarClient>(), Schema.String);

        // Act
        var act = () => uut
            .Topic("persistent://public/default/my-topic")
            .EnableBatching(true)
            .EnableChunking(true)
            .Create();

        // Assert
        var exception = act.ShouldThrow<ConfigurationException>();
        exception.Message.ShouldBe("Batching and chunking of messages can't be enabled together");
    }

    // Additional coverage
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public void Create_GivenChunkingAndBatchingNotBothEnabled_ShouldNotThrow(bool enableChunking, bool enableBatching)
    {
        // Arrange
        var pulsarClient = Substitute.For<IPulsarClient>();
        pulsarClient.CreateProducer(Arg.Any<ProducerOptions<string>>()).Returns(Substitute.For<IProducer<string>>());
        var uut = new ProducerBuilder<string>(pulsarClient, Schema.String);

        // Act
        var act = () => uut
            .Topic("persistent://public/default/my-topic")
            .EnableChunking(enableChunking)
            .EnableBatching(enableBatching)
            .Create();

        // Assert
        _ = act.ShouldNotThrow();
    }

    // Additional coverage
    [Fact]
    public void Create_GivenChunkingOptions_ShouldCopyThemToProducerOptions()
    {
        // Arrange
        var pulsarClient = Substitute.For<IPulsarClient>();
        ProducerOptions<string>? actual = null;
        pulsarClient.CreateProducer(Arg.Do<ProducerOptions<string>>(options => actual = options)).Returns(Substitute.For<IProducer<string>>());
        var uut = new ProducerBuilder<string>(pulsarClient, Schema.String);

        // Act
        _ = uut
            .Topic("persistent://public/default/my-topic")
            .EnableChunking(true)
            .ChunkMaxMessageSize(1000)
            .Create();

        // Assert
        actual.ShouldNotBeNull();
        actual.EnableChunking.ShouldBeTrue();
        actual.ChunkMaxMessageSize.ShouldBe(1000);
    }

    // Additional coverage
    [Fact]
    public void Create_GivenDefaults_ShouldNotEnableChunking()
    {
        // Arrange
        var pulsarClient = Substitute.For<IPulsarClient>();
        ProducerOptions<string>? actual = null;
        pulsarClient.CreateProducer(Arg.Do<ProducerOptions<string>>(options => actual = options)).Returns(Substitute.For<IProducer<string>>());
        var uut = new ProducerBuilder<string>(pulsarClient, Schema.String);

        // Act
        _ = uut.Topic("persistent://public/default/my-topic").Create();

        // Assert
        actual.ShouldNotBeNull();
        actual.EnableChunking.ShouldBeFalse();
        actual.ChunkMaxMessageSize.ShouldBe(0);
    }

    // Upstream: ProducerConfigurationData#cryptoFailureAction (the default is FAIL), #isEncryptionEnabled
    [Fact]
    public void Create_GivenNoCryptoSettings_ShouldUseTheDefaults()
    {
        // Arrange
        var (uut, options) = CreateBuilder();

        // Act
        uut.Create();

        // Assert
        options().CryptoKeyReader.ShouldBeNull();
        options().EncryptionKeys.ShouldBeEmpty();
        options().CryptoFailureAction.ShouldBe(ProducerCryptoFailureAction.Fail);
        ProducerOptions<string>.DefaultCryptoFailureAction.ShouldBe(ProducerCryptoFailureAction.Fail);
    }

    // Upstream: ProducerBuilderImpl#cryptoKeyReader, #addEncryptionKey, #cryptoFailureAction
    [Theory]
    [InlineData(ProducerCryptoFailureAction.Fail)]
    [InlineData(ProducerCryptoFailureAction.Send)]
    public void Create_GivenCryptoSettings_ShouldPassThemOnInTheOptions(ProducerCryptoFailureAction cryptoFailureAction)
    {
        // Arrange
        var (uut, options) = CreateBuilder();
        var cryptoKeyReader = Substitute.For<ICryptoKeyReader>();

        // Act
        uut.CryptoKeyReader(cryptoKeyReader).EncryptionKey("my-key").CryptoFailureAction(cryptoFailureAction).Create();

        // Assert
        options().CryptoKeyReader.ShouldBeSameAs(cryptoKeyReader);
        options().EncryptionKeys.ShouldBe(["my-key"]);
        options().CryptoFailureAction.ShouldBe(cryptoFailureAction);
    }

    // Additional coverage
    [Fact]
    public void Builder_GivenCryptoSettings_ShouldReturnTheBuilder()
    {
        // Arrange
        var (uut, _) = CreateBuilder();

        // Act & Assert
        uut.CryptoKeyReader(Substitute.For<ICryptoKeyReader>()).ShouldBeSameAs(uut);
        uut.EncryptionKey("my-key").ShouldBeSameAs(uut);
        uut.CryptoFailureAction(ProducerCryptoFailureAction.Send).ShouldBeSameAs(uut);
    }

    // Upstream: ProducerBuilderImpl#addEncryptionKey (the keys are a TreeSet), MessageCryptoBc#encrypt (the keys are written in this order)
    [Fact]
    public void Create_GivenSeveralEncryptionKeys_ShouldPassThemSortedByOrdinalOrderWithoutDuplicates()
    {
        // Arrange
        var (uut, options) = CreateBuilder();

        // Act
        uut.CryptoKeyReader(Substitute.For<ICryptoKeyReader>()).EncryptionKey("b").EncryptionKey("B").EncryptionKey("a").EncryptionKey("b").Create();

        // Assert
        options().EncryptionKeys.ShouldBe(["B", "a", "b"]);
    }

    // Upstream: ProducerBuilderImpl#addEncryptionKey (checkArgument(isNotBlank))
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t\n")]
    public void EncryptionKey_GivenBlankKeyName_ShouldThrowArgumentException(string? keyName)
    {
        // Arrange
        var (uut, _) = CreateBuilder();

        // Act
        var act = () => uut.EncryptionKey(keyName!);

        // Assert
        var exception = act.ShouldThrow<ArgumentException>();
        exception.ParamName.ShouldBe("keyName");
        exception.Message.ShouldStartWith("Encryption key cannot be blank");
    }

    // Additional coverage
    [Fact]
    public void Create_GivenEncryptionKeyWithoutCryptoKeyReader_ShouldThrowConfigurationException()
    {
        // Arrange
        var (uut, _) = CreateBuilder();

        // Act
        var act = () => uut.EncryptionKey("my-key").Create();

        // Assert
        var exception = act.ShouldThrow<ConfigurationException>();
        exception.Message.ShouldBe("ProducerOptions.CryptoKeyReader must be set when ProducerOptions.EncryptionKeys is not empty");
    }

    // Additional coverage
    [Fact]
    public void Create_GivenCryptoKeyReaderWithoutEncryptionKey_ShouldNotThrow()
    {
        // Arrange
        var (uut, options) = CreateBuilder();

        // Act
        var act = () => uut.CryptoKeyReader(Substitute.For<ICryptoKeyReader>()).Create();

        // Assert
        _ = act.ShouldNotThrow();
        options().EncryptionKeys.ShouldBeEmpty();
    }

    // Additional coverage
    [Fact]
    public void Create_GivenBuilderUsedForTwoProducers_ShouldNotShareTheEncryptionKeys()
    {
        // Arrange
        var (uut, options) = CreateBuilder();
        uut.CryptoKeyReader(Substitute.For<ICryptoKeyReader>()).EncryptionKey("one");
        uut.Create();
        var first = options();

        // Act
        uut.EncryptionKey("two").Create();

        // Assert
        first.EncryptionKeys.ShouldBe(["one"]);
        options().EncryptionKeys.ShouldBe(["one", "two"]);
    }

    // Upstream: ProducerBuilderImpl#defaultCryptoKeyReader(String)
    [Fact]
    public void Create_GivenDefaultCryptoKeyReaderWithPublicKey_ShouldPassAReaderThatReadsItForEveryKeyName()
    {
        // Arrange
        var (uut, options) = CreateBuilder();
        var publicKey = CryptoTestKeys.Public("ClientRsa");

        // Act
        uut.DefaultCryptoKeyReader(CreateDataUri(publicKey)).EncryptionKey("one").Create();

        // Assert
        var cryptoKeyReader = options().CryptoKeyReader.ShouldBeOfType<DefaultCryptoKeyReader>();
        cryptoKeyReader.GetPublicKey("one", new Dictionary<string, string>())!.Key.ShouldBe(publicKey);
        cryptoKeyReader.GetPublicKey("two", new Dictionary<string, string>())!.Key.ShouldBe(publicKey);
        cryptoKeyReader.GetPrivateKey("one", new Dictionary<string, string>()).ShouldBeNull();
    }

    // Upstream: ProducerBuilderImpl#defaultCryptoKeyReader(Map)
    [Fact]
    public void Create_GivenDefaultCryptoKeyReaderWithPublicKeysByName_ShouldPassAReaderThatReadsThemByKeyName()
    {
        // Arrange
        var (uut, options) = CreateBuilder();
        var publicKey = CryptoTestKeys.Public("ClientRsa");

        // Act
        uut.DefaultCryptoKeyReader(new Dictionary<string, string> { ["one"] = CreateDataUri(publicKey) }).EncryptionKey("one").Create();

        // Assert
        var cryptoKeyReader = options().CryptoKeyReader.ShouldBeOfType<DefaultCryptoKeyReader>();
        cryptoKeyReader.GetPublicKey("one", new Dictionary<string, string>())!.Key.ShouldBe(publicKey);
        cryptoKeyReader.GetPublicKey("two", new Dictionary<string, string>()).ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void ProducerOptions_GivenNoChanges_ShouldHaveTheDefaultCryptoSettings()
    {
        // Act
        var uut = new ProducerOptions<string>("persistent://public/default/my-topic", Schema.String);

        // Assert
        uut.CryptoKeyReader.ShouldBeNull();
        uut.EncryptionKeys.ShouldBeEmpty();
        uut.CryptoFailureAction.ShouldBe(ProducerOptions<string>.DefaultCryptoFailureAction);
    }

    // Additional coverage
    [Fact]
    public void ProducerOptions_GivenEncryptionKeysSetTwice_ShouldKeepOnlyTheLastOnesSortedByOrdinalOrder()
    {
        // Arrange
        var uut = new ProducerOptions<string>("persistent://public/default/my-topic", Schema.String);

        // Act
        uut.EncryptionKeys = ["b", "a"];
        uut.EncryptionKeys = ["c", "B", "c"];

        // Assert
        uut.EncryptionKeys.ShouldBe(["B", "c"]);
    }

    private static string CreateDataUri(byte[] key)
        => "data:application/x-pem-file;base64," + Convert.ToBase64String(key);

    private static (IProducerBuilder<string> Builder, Func<ProducerOptions<string>> Options) CreateBuilder()
    {
        ProducerOptions<string>? captured = null;
        var client = Substitute.For<IPulsarClient>();
        client.CreateProducer(Arg.Do<ProducerOptions<string>>(options => captured = options)).Returns(Substitute.For<IProducer<string>>());

        var builder = new ProducerBuilder<string>(client, Schema.String).Topic("persistent://public/default/my-topic");

        return (builder, () => captured ?? throw new InvalidOperationException("The producer was not created"));
    }

    private static IPulsarClient CreateClient()
        => PulsarClient
            .Builder()
            .ServiceUrl(new Uri("pulsar://localhost:6650"))
            .Build();
}
