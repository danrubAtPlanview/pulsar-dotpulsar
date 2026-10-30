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
using Google.Protobuf;

[Trait("Category", "Unit")]
public sealed class DecryptionKeyCacheTests
{
    private static AesGcmCipher CreateCipher()
        => new(new byte[32]);

    private static ByteString CreateKey(int value, int length = 256)
    {
        var bytes = new byte[length];
        BitConverter.GetBytes(value).CopyTo(bytes, 0);
        return ByteString.CopyFrom(bytes);
    }

    [Fact]
    public void TryGet_GivenPutKey_ShouldReturnTheCipherByWrappedKeyContent()
    {
        //Arrange
        using var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), 1000);
        var cipher = CreateCipher();
        uut.Put(CreateKey(1), cipher);

        //Act
        var actual = uut.TryGet(CreateKey(1), out var actualCipher);

        //Assert
        actual.ShouldBeTrue();
        actualCipher.ShouldBeSameAs(cipher);
    }

    [Fact]
    public void TryGet_GivenUnknownKey_ShouldReturnFalse()
    {
        //Arrange
        using var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), 1000);
        uut.Put(CreateKey(1), CreateCipher());

        //Act
        var actual = uut.TryGet(CreateKey(2), out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryGetLast_GivenPutLast_ShouldReturnTheCipherByProducerName()
    {
        //Arrange
        using var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), 1000);
        var cipher = CreateCipher();
        uut.PutLast("producer", cipher);

        //Act
        var actual = uut.TryGetLast("producer", out var actualCipher);
        var other = uut.TryGetLast("other", out _);

        //Assert
        actual.ShouldBeTrue();
        actualCipher.ShouldBeSameAs(cipher);
        other.ShouldBeFalse();
    }

    [Fact]
    public void RemoveLast_GivenSameCipher_ShouldRemoveTheEntry()
    {
        //Arrange
        using var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), 1000);
        var cipher = CreateCipher();
        uut.PutLast("producer", cipher);

        //Act
        uut.RemoveLast("producer", cipher);

        //Assert
        uut.TryGetLast("producer", out _).ShouldBeFalse();
    }

    [Fact]
    public void RemoveLast_GivenOtherCipher_ShouldKeepTheEntry()
    {
        //Arrange
        using var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), 1000);
        var cipher = CreateCipher();
        uut.PutLast("producer", cipher);

        //Act
        uut.RemoveLast("producer", CreateCipher());

        //Assert
        uut.TryGetLast("producer", out var actual).ShouldBeTrue();
        actual.ShouldBeSameAs(cipher);
    }

    [Fact]
    public void TryGet_GivenEntryThatHasNotBeenAccessedForTheExpirationTime_ShouldReturnFalse()
    {
        //Arrange
        var now = TimeSpan.Zero;
        using var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), 1000, () => now);
        uut.Put(CreateKey(1), CreateCipher());
        now = TimeSpan.FromHours(4) + TimeSpan.FromTicks(1);

        //Act
        var actual = uut.TryGet(CreateKey(1), out _);

        //Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryGet_GivenEntryThatIsAccessedWithinTheExpirationTime_ShouldKeepTheEntryAlive()
    {
        //Arrange
        var now = TimeSpan.Zero;
        using var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), 1000, () => now);
        uut.Put(CreateKey(1), CreateCipher());

        //Act
        now = TimeSpan.FromHours(3);
        var first = uut.TryGet(CreateKey(1), out _);
        now = TimeSpan.FromHours(6);
        var second = uut.TryGet(CreateKey(1), out _);
        now = TimeSpan.FromHours(10.5);
        var third = uut.TryGet(CreateKey(1), out _);

        //Assert
        first.ShouldBeTrue();
        second.ShouldBeTrue();
        third.ShouldBeFalse();
    }

    [Fact]
    public void Put_GivenMaximumWeightIsExceeded_ShouldEvictTheOldestAccessedEntries()
    {
        //Arrange
        var now = TimeSpan.Zero;
        const int entryWeight = 256 + 32;
        using var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), entryWeight * 3, () => now);

        uut.Put(CreateKey(1), CreateCipher());
        now += TimeSpan.FromSeconds(1);
        uut.Put(CreateKey(2), CreateCipher());
        now += TimeSpan.FromSeconds(1);
        uut.Put(CreateKey(3), CreateCipher());
        now += TimeSpan.FromSeconds(1);
        uut.TryGet(CreateKey(1), out _).ShouldBeTrue(); // 2 is now the oldest accessed
        now += TimeSpan.FromSeconds(1);

        //Act
        uut.Put(CreateKey(4), CreateCipher());

        //Assert
        uut.TryGet(CreateKey(1), out _).ShouldBeTrue();
        uut.TryGet(CreateKey(2), out _).ShouldBeFalse();
        uut.TryGet(CreateKey(3), out _).ShouldBeTrue();
        uut.TryGet(CreateKey(4), out _).ShouldBeTrue();
    }

    [Fact]
    public void Put_GivenSameKeyTwice_ShouldReplaceTheEntryAndNotCountItsWeightTwice()
    {
        //Arrange
        const int entryWeight = 256 + 32;
        using var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), entryWeight * 2);
        var replacement = CreateCipher();
        uut.Put(CreateKey(1), CreateCipher());
        uut.Put(CreateKey(1), replacement);
        uut.Put(CreateKey(1), replacement);

        //Act
        uut.Put(CreateKey(2), CreateCipher());

        //Assert
        uut.TryGet(CreateKey(1), out var actual).ShouldBeTrue();
        actual.ShouldBeSameAs(replacement);
        uut.TryGet(CreateKey(2), out _).ShouldBeTrue();
    }

    [Fact]
    public void Put_GivenExpiredEntriesAndTheSweepTimeHasPassed_ShouldFreeTheWeightOfTheExpiredEntries()
    {
        //Arrange
        var now = TimeSpan.Zero;
        const int entryWeight = 256 + 32;
        using var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), entryWeight * 2, () => now);
        uut.Put(CreateKey(1), CreateCipher());
        uut.Put(CreateKey(2), CreateCipher());
        now = TimeSpan.FromHours(5);

        //Act
        uut.Put(CreateKey(3), CreateCipher());

        //Assert
        uut.TryGet(CreateKey(1), out _).ShouldBeFalse();
        uut.TryGet(CreateKey(2), out _).ShouldBeFalse();
        uut.TryGet(CreateKey(3), out _).ShouldBeTrue();
    }

    [Fact]
    public void Dispose_GivenEntries_ShouldDisposeTheCiphers()
    {
        //Arrange
        var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), 1000);
        var cipher = CreateCipher();
        uut.Put(CreateKey(1), cipher);
        uut.PutLast("producer", cipher);

        //Act
        uut.Dispose();

        //Assert
        cipher.Encrypt(new byte[12], [], [], new byte[16]).ShouldBeFalse();
        uut.TryGet(CreateKey(1), out _).ShouldBeFalse();
        uut.TryGetLast("producer", out _).ShouldBeFalse();
    }

    [Fact]
    public void Put_GivenConcurrentCalls_ShouldNotThrow()
    {
        //Arrange
        const int entryWeight = 256 + 32;
        using var uut = new DecryptionKeyCache(TimeSpan.FromHours(4), entryWeight * 50);

        //Act
        var exception = Record.Exception(() => Parallel.For(0, 2000, i =>
        {
            uut.Put(CreateKey(i % 200), CreateCipher());
            uut.TryGet(CreateKey((i * 7) % 200), out _);
            uut.PutLast($"producer-{i % 20}", CreateCipher());
            uut.TryGetLast($"producer-{(i * 3) % 20}", out _);
        }));

        //Assert
        exception.ShouldBeNull();
    }
}
