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

namespace DotPulsar.Tests;

using DotPulsar.Abstractions;
using DotPulsar.Exceptions;

[Trait("Category", "Unit")]
public sealed class PulsarClientProducerOptionsTests
{
    private static readonly Uri _serviceUrl = new("pulsar://localhost:6650");

    // Upstream: ProducerBuilderImpl#createAsync
    [Fact]
    public async Task CreateProducer_GivenChunkingAndBatchingEnabled_ShouldThrowConfigurationException()
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = CreateOptions();
        options.EnableChunking = true;
        options.EnableBatching = true;

        //Act
        var exception = Record.Exception(() => client.CreateProducer(options));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("Batching and chunking of messages can't be enabled together");
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task CreateProducer_GivenChunkingAndBatchingNotBothEnabled_ShouldNotThrow(bool enableChunking, bool enableBatching)
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = CreateOptions();
        options.EnableChunking = enableChunking;
        options.EnableBatching = enableBatching;

        //Act
        var exception = Record.Exception(() => client.CreateProducer(options));

        //Assert
        exception.ShouldBeNull();
    }

    [Theory]
    [InlineData(2)]
    [InlineData(-1)]
    [InlineData(255)]
    public async Task CreateProducer_GivenCryptoFailureActionThatIsNotDefined_ShouldThrowConfigurationException(int cryptoFailureAction)
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = CreateOptions();
        options.CryptoFailureAction = (ProducerCryptoFailureAction) cryptoFailureAction;

        //Act
        var exception = Record.Exception(() => client.CreateProducer(options));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("ProducerOptions.CryptoFailureAction is not a valid ProducerCryptoFailureAction");
    }

    // Upstream: ProducerConfigurationData#isEncryptionEnabled (Java sends unencrypted if there is no key reader, DotPulsar does not)
    [Fact]
    public async Task CreateProducer_GivenEncryptionKeysWithoutCryptoKeyReader_ShouldThrowConfigurationException()
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = CreateOptions();
        options.EncryptionKeys = ["my-key"];

        //Act
        var exception = Record.Exception(() => client.CreateProducer(options));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("ProducerOptions.CryptoKeyReader must be set when ProducerOptions.EncryptionKeys is not empty");
    }

    // Upstream: ProducerBuilderImpl#addEncryptionKey (checkArgument(isNotBlank))
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public async Task CreateProducer_GivenBlankEncryptionKeyName_ShouldThrowConfigurationException(string? keyName)
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = CreateOptions();
        options.CryptoKeyReader = Substitute.For<ICryptoKeyReader>();
        options.EncryptionKeys = ["my-key", keyName!];

        //Act
        var exception = Record.Exception(() => client.CreateProducer(options));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("ProducerOptions.EncryptionKeys may not contain a null or blank key name");
    }

    [Fact]
    public async Task CreateProducer_GivenCryptoKeyReaderWithoutEncryptionKeys_ShouldNotThrow()
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = CreateOptions();
        options.CryptoKeyReader = Substitute.For<ICryptoKeyReader>();

        //Act
        var exception = Record.Exception(() => client.CreateProducer(options));

        //Assert
        exception.ShouldBeNull();
    }

    [Theory]
    [InlineData(ProducerCryptoFailureAction.Fail)]
    [InlineData(ProducerCryptoFailureAction.Send)]
    public async Task CreateProducer_GivenDefinedCryptoFailureActionAndNoKeys_ShouldNotThrow(ProducerCryptoFailureAction cryptoFailureAction)
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = CreateOptions();
        options.CryptoFailureAction = cryptoFailureAction;

        //Act
        var exception = Record.Exception(() => client.CreateProducer(options));

        //Assert
        exception.ShouldBeNull();
    }

    private static ProducerOptions<string> CreateOptions()
        => new("persistent://public/default/my-topic", Schema.String) { MaxPendingMessages = 500 };
}
