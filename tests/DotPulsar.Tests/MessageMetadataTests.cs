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

[Trait("Category", "Unit")]
public sealed class MessageMetadataTests
{
    // Additional coverage
    [Fact]
    public void Transaction_GivenNoTransaction_ShouldBeNullAndHaveNoTxnIds()
    {
        //Act
        var uut = new MessageMetadata();

        //Assert
        uut.Transaction.ShouldBeNull();
        uut.ProtocolMetadata().HasTxnidMostBits.ShouldBeFalse();
        uut.ProtocolMetadata().HasTxnidLeastBits.ShouldBeFalse();
    }

    // Additional coverage
    [Theory]
    [InlineData(0UL, 0UL)]
    [InlineData(0UL, 5UL)]
    [InlineData(3UL, 7UL)]
    [InlineData(ulong.MaxValue, ulong.MaxValue)]
    public void Transaction_GivenTransaction_ShouldSetTxnIds(ulong mostSigBits, ulong leastSigBits)
    {
        //Arrange
        var transaction = Substitute.For<ITransaction>();
        transaction.Id.Returns(new TransactionId(mostSigBits, leastSigBits));
        var uut = new MessageMetadata();

        //Act
        uut.Transaction = transaction;

        //Assert
        uut.Transaction.ShouldBeSameAs(transaction);
        uut.ProtocolMetadata().HasTxnidMostBits.ShouldBeTrue();
        uut.ProtocolMetadata().HasTxnidLeastBits.ShouldBeTrue();
        uut.ProtocolMetadata().TxnidMostBits.ShouldBe(mostSigBits);
        uut.ProtocolMetadata().TxnidLeastBits.ShouldBe(leastSigBits);
    }

    // Additional coverage
    [Fact]
    public void Transaction_GivenTransactionThenNull_ShouldClearTxnIds()
    {
        //Arrange
        var transaction = Substitute.For<ITransaction>();
        transaction.Id.Returns(new TransactionId(0, 0));
        var uut = new MessageMetadata { Transaction = transaction };

        //Act
        uut.Transaction = null;

        //Assert
        uut.Transaction.ShouldBeNull();
        uut.ProtocolMetadata().HasTxnidMostBits.ShouldBeFalse();
        uut.ProtocolMetadata().HasTxnidLeastBits.ShouldBeFalse();
    }
}
