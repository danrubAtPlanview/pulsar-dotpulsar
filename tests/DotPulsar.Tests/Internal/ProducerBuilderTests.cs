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

    private static IPulsarClient CreateClient()
        => PulsarClient
            .Builder()
            .ServiceUrl(new Uri("pulsar://localhost:6650"))
            .Build();
}
