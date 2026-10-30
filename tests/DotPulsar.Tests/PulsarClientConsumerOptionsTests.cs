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

using DotPulsar.Exceptions;

[Trait("Category", "Unit")]
public sealed class PulsarClientConsumerOptionsTests
{
    private static readonly Uri _serviceUrl = new("pulsar://localhost:6650");

    // Upstream: UnAcknowledgedMessagesTimeoutTest#testAckTimeoutMinValue
    [Theory]
    [InlineData(999)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(-1000)]
    public async Task CreateConsumer_GivenAckTimeoutBelowOneSecond_ShouldThrowConfigurationException(int milliseconds)
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = CreateOptions();
        options.AckTimeout = TimeSpan.FromMilliseconds(milliseconds);

        //Act
        var exception = Record.Exception(() => client.CreateConsumer(options));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("ConsumerOptions.AckTimeout must be TimeSpan.Zero (disabled) or at least 1 second");
    }

    [Theory]
    [InlineData(99)]
    [InlineData(0)]
    [InlineData(-100)]
    public async Task CreateConsumer_GivenAckTimeoutTickTimeBelow100Milliseconds_ShouldThrowConfigurationException(int milliseconds)
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = CreateOptions();
        options.AckTimeoutTickTime = TimeSpan.FromMilliseconds(milliseconds);

        //Act
        var exception = Record.Exception(() => client.CreateConsumer(options));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("ConsumerOptions.AckTimeoutTickTime must be at least 100 milliseconds");
    }

    // Upstream: ConsumerBuilderImpl#deadLetterPolicy, DeadLetterPolicyTest
    [Fact]
    public async Task CreateConsumer_GivenDeadLetterPolicyWithZeroMaxRedeliverCount_ShouldThrowConfigurationException()
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = CreateOptions();
        options.DeadLetterPolicy = new DeadLetterPolicy(0);

        //Act
        var exception = Record.Exception(() => client.CreateConsumer(options));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("MaxRedeliverCount must be > 0.");
    }

    [Fact]
    public async Task CreateConsumer_GivenNegativeNegativeAcknowledgeRedeliveryDelay_ShouldThrowConfigurationException()
    {
        //Arrange
        await using var client = PulsarClient.Builder().ServiceUrl(_serviceUrl).Build();
        var options = CreateOptions();
        options.NegativeAcknowledgeRedeliveryDelay = TimeSpan.FromMilliseconds(-1);

        //Act
        var exception = Record.Exception(() => client.CreateConsumer(options));

        //Assert
        exception.ShouldBeOfType<ConfigurationException>().Message.ShouldBe("ConsumerOptions.NegativeAcknowledgeRedeliveryDelay may not be negative");
    }

    [Fact]
    public void ConsumerOptions_GivenNoChanges_ShouldHaveTheDefaultsOfTheRedeliverySettings()
    {
        //Act
        var uut = CreateOptions();

        //Assert
        uut.AckTimeout.ShouldBe(TimeSpan.Zero);
        uut.AckTimeoutTickTime.ShouldBe(TimeSpan.FromSeconds(1));
        uut.DeadLetterPolicy.ShouldBeNull();
        uut.NegativeAcknowledgeRedeliveryDelay.ShouldBe(TimeSpan.FromMinutes(1));
    }

    private static ConsumerOptions<string> CreateOptions()
        => new("subscription", "persistent://public/default/test", Schema.String);
}
