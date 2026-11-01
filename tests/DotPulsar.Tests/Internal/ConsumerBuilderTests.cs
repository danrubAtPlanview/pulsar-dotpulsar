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
public sealed class ConsumerBuilderTests
{
    // Additional coverage
    [Fact]
    public void Create_GivenNoBatchIndexAckEnabled_ShouldDisableIt()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.Create();

        //Assert
        options().BatchIndexAckEnabled.ShouldBeFalse();
    }

    // Additional coverage
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Create_GivenBatchIndexAckEnabled_ShouldPassItOnInTheOptions(bool batchIndexAckEnabled)
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.BatchIndexAckEnabled(batchIndexAckEnabled).Create();

        //Assert
        options().BatchIndexAckEnabled.ShouldBe(batchIndexAckEnabled);
    }

    // Additional coverage
    [Fact]
    public void BatchIndexAckEnabled_ShouldReturnTheBuilder()
    {
        //Arrange
        var (uut, _) = CreateBuilder();

        //Act
        var actual = uut.BatchIndexAckEnabled(true);

        //Assert
        actual.ShouldBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public void ConsumerOptions_GivenNoChanges_ShouldHaveTheDefaultBatchIndexAckEnabled()
    {
        //Act
        var uut = new ConsumerOptions<string>("subscription", "persistent://public/default/test", Schema.String);

        //Assert
        ConsumerOptions<string>.DefaultBatchIndexAckEnabled.ShouldBeFalse();
        uut.BatchIndexAckEnabled.ShouldBe(ConsumerOptions<string>.DefaultBatchIndexAckEnabled);
    }

    // Additional coverage
    [Fact]
    public void Create_GivenNoAckTimeoutSettings_ShouldUseTheDefaults()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.Create();

        //Assert
        options().AckTimeout.ShouldBe(TimeSpan.Zero);
        options().AckTimeoutTickTime.ShouldBe(TimeSpan.FromSeconds(1));
        ConsumerOptions<string>.DefaultAckTimeout.ShouldBe(TimeSpan.Zero);
        ConsumerOptions<string>.DefaultAckTimeoutTickTime.ShouldBe(TimeSpan.FromSeconds(1));
    }

    // Additional coverage
    [Fact]
    public void Create_GivenAckTimeoutAndTickTime_ShouldPassThemOnInTheOptions()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.AckTimeout(TimeSpan.FromSeconds(30)).AckTimeoutTickTime(TimeSpan.FromMilliseconds(250)).Create();

        //Assert
        options().AckTimeout.ShouldBe(TimeSpan.FromSeconds(30));
        options().AckTimeoutTickTime.ShouldBe(TimeSpan.FromMilliseconds(250));
    }

    // Additional coverage
    [Fact]
    public void Create_GivenNoDeadLetterPolicy_ShouldNotSetOneInTheOptions()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.Create();

        //Assert
        options().DeadLetterPolicy.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void Create_GivenDeadLetterPolicy_ShouldPassItOnInTheOptions()
    {
        //Arrange
        var (uut, options) = CreateBuilder();
        var deadLetterPolicy = new DeadLetterPolicy(3);

        //Act
        uut.DeadLetterPolicy(deadLetterPolicy).Create();

        //Assert
        options().DeadLetterPolicy.ShouldBeSameAs(deadLetterPolicy);
    }

    // Upstream: ConsumerBuilderImpl#deadLetterPolicy, DeadLetterPolicyTest
    [Fact]
    public void Create_GivenDeadLetterPolicyWithZeroMaxRedeliverCount_ShouldThrowConfigurationException()
    {
        //Arrange
        var (uut, _) = CreateBuilder();
        uut.DeadLetterPolicy(new DeadLetterPolicy(0));

        //Act
        var exception = Record.Exception(uut.Create);

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("MaxRedeliverCount must be > 0.");
    }

    // Additional coverage
    [Fact]
    public void Create_GivenNoNegativeAcknowledgeRedeliveryDelay_ShouldUseTheDefault()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.Create();

        //Assert
        options().NegativeAcknowledgeRedeliveryDelay.ShouldBe(TimeSpan.FromMinutes(1));
        ConsumerOptions<string>.DefaultNegativeAcknowledgeRedeliveryDelay.ShouldBe(TimeSpan.FromMinutes(1));
    }

    // Additional coverage
    [Fact]
    public void Create_GivenNegativeAcknowledgeRedeliveryDelay_ShouldPassItOnInTheOptions()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromSeconds(5)).Create();

        //Assert
        options().NegativeAcknowledgeRedeliveryDelay.ShouldBe(TimeSpan.FromSeconds(5));
    }

    // Additional coverage
    [Fact]
    public void Create_GivenAckTimeoutRedeliveryBackoff_ShouldPassItOnInTheOptions()
    {
        //Arrange
        var (uut, options) = CreateBuilder();
        var backoff = new MultiplierRedeliveryBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10));

        //Act
        uut.AckTimeout(TimeSpan.FromSeconds(30)).AckTimeoutRedeliveryBackoff(backoff).Create();

        //Assert
        options().AckTimeoutRedeliveryBackoff.ShouldBeSameAs(backoff);
    }

    // Additional coverage
    [Fact]
    public void Create_GivenNoAckTimeoutRedeliveryBackoff_ShouldNotSetIt()
    {
        //Arrange
        var (uut, options) = CreateBuilder();

        //Act
        uut.AckTimeout(TimeSpan.FromSeconds(30)).Create();

        //Assert
        options().AckTimeoutRedeliveryBackoff.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void AckTimeoutRedeliveryBackoff_ShouldReturnTheBuilder()
    {
        //Arrange
        var (uut, _) = CreateBuilder();

        //Act
        var actual = uut.AckTimeoutRedeliveryBackoff(new MultiplierRedeliveryBackoff());

        //Assert
        actual.ShouldBeSameAs(uut);
    }

    // Additional coverage
    [Fact]
    public void Builder_GivenAckTimeoutDeadLetterPolicyAndNegativeAcknowledgeRedeliveryDelay_ShouldReturnTheBuilder()
    {
        //Arrange
        var (uut, _) = CreateBuilder();

        //Act & Assert
        uut.AckTimeout(TimeSpan.Zero).ShouldBeSameAs(uut);
        uut.AckTimeoutTickTime(TimeSpan.FromSeconds(1)).ShouldBeSameAs(uut);
        uut.DeadLetterPolicy(new DeadLetterPolicy(1)).ShouldBeSameAs(uut);
        uut.NegativeAcknowledgeRedeliveryDelay(TimeSpan.FromSeconds(1)).ShouldBeSameAs(uut);
    }

    // Upstream: ConsumerConfigurationData#maxPendingChunkedMessage, #autoAckOldestChunkedMessageOnQueueFull, #expireTimeOfIncompleteChunkedMessageMillis
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
        ConsumerOptions<string>.DefaultMaxPendingChunkedMessage.ShouldBe(10U);
        ConsumerOptions<string>.DefaultAutoAckOldestChunkedMessageOnQueueFull.ShouldBeFalse();
        ConsumerOptions<string>.DefaultExpireTimeOfIncompleteChunkedMessage.ShouldBe(TimeSpan.FromMinutes(1));
    }

    // Upstream: MessageChunkingTest#testReaderChunkingConfiguration
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
        ConsumerOptions<string>.DefaultCryptoFailureAction.ShouldBe(ConsumerCryptoFailureAction.Fail);
    }

    // Upstream: ConsumerBuilderImpl#cryptoKeyReader, ConsumerBuilderImpl#cryptoFailureAction
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
    public void ConsumerOptions_GivenNoChanges_ShouldHaveTheDefaultCryptoSettings()
    {
        //Act
        var uut = new ConsumerOptions<string>("subscription", "persistent://public/default/test", Schema.String);

        //Assert
        uut.CryptoKeyReader.ShouldBeNull();
        uut.CryptoFailureAction.ShouldBe(ConsumerOptions<string>.DefaultCryptoFailureAction);
    }

    // Upstream: ConsumerBuilderImpl#defaultCryptoKeyReader(String)
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

    // Upstream: ConsumerBuilderImpl#defaultCryptoKeyReader(Map)
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

    private static (IConsumerBuilder<string> Builder, Func<ConsumerOptions<string>> Options) CreateBuilder()
    {
        ConsumerOptions<string>? captured = null;
        var client = Substitute.For<IPulsarClient>();
        client.CreateConsumer(Arg.Do<ConsumerOptions<string>>(options => captured = options)).Returns(Substitute.For<IConsumer<string>>());

        var builder = new ConsumerBuilder<string>(client, Schema.String)
            .SubscriptionName("subscription")
            .Topic("persistent://public/default/test");

        return (builder, () => captured ?? throw new InvalidOperationException("The consumer was not created"));
    }
}
