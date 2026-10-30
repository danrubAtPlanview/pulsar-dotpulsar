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

[Trait("Category", "Unit")]
public sealed class BatchMessageContainerFactoryTests
{
    [Fact]
    public void Create_GivenDefaultBatcher_ShouldReturnBatchMessageContainer()
    {
        //Act
        var actual = BatchMessageContainerFactory.Create<TestBatchItem>(BatcherType.Default, 1000, 128 * 1024, TimeSpan.FromMilliseconds(1));

        //Assert
        actual.ShouldBeOfType<BatchMessageContainer<TestBatchItem>>();
    }

    [Fact]
    public void Create_GivenKeyBasedBatcher_ShouldReturnKeyBasedBatchMessageContainer()
    {
        //Act
        var actual = BatchMessageContainerFactory.Create<TestBatchItem>(BatcherType.KeyBased, 1000, 128 * 1024, TimeSpan.FromMilliseconds(1));

        //Assert
        actual.ShouldBeOfType<KeyBasedBatchMessageContainer<TestBatchItem>>();
    }

    [Fact]
    public void Create_GivenUnknownBatcher_ShouldThrowArgumentOutOfRangeException()
    {
        //Act
        var exception = Record.Exception(() => BatchMessageContainerFactory.Create<TestBatchItem>((BatcherType) 99, 1000, 128 * 1024, TimeSpan.FromMilliseconds(1)));

        //Assert
        exception.ShouldBeOfType<ArgumentOutOfRangeException>();
    }

    [Theory]
    [InlineData(BatcherType.Default)]
    [InlineData(BatcherType.KeyBased)]
    public void Create_GivenLimitsAndClock_ShouldApplyThemToTheContainer(BatcherType batcherType)
    {
        //Arrange
        var clock = new FakeClock();
        var uut = BatchMessageContainerFactory.Create<TestBatchItem>(batcherType, 2, 0, TimeSpan.FromMilliseconds(5), clock: clock.Now);

        //Act
        uut.Add(TestBatchItem.Create(1), 1000);
        var full = uut.Add(TestBatchItem.Create(2), 1000);
        clock.Advance(TimeSpan.FromMilliseconds(5));

        //Assert
        full.ShouldBeTrue();
        uut.GetRemainingPublishDelay().ShouldBe(TimeSpan.Zero);
    }

    [Fact]
    public void BatcherType_GivenDefinition_ShouldBeBackedByByteWithDefaultFirst()
    {
        //Act
        var underlyingType = Enum.GetUnderlyingType(typeof(BatcherType));

        //Assert
        underlyingType.ShouldBe(typeof(byte));
        ((byte) BatcherType.Default).ShouldBe((byte) 0);
        ((byte) BatcherType.KeyBased).ShouldBe((byte) 1);
        default(BatcherType).ShouldBe(BatcherType.Default);
    }
}
