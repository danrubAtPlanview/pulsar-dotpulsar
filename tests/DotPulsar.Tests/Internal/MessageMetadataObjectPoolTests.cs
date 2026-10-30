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
using DotPulsar.Internal;

[Trait("Category", "Unit")]
public sealed class MessageMetadataObjectPoolTests
{
    [Fact]
    public void Return_GivenMetadataWithTransaction_ShouldClearTransactionAndTxnIds()
    {
        //Arrange
        var transaction = Substitute.For<ITransaction>();
        transaction.Id.Returns(new TransactionId(0, 0));
        var metadata = MessageMetadataObjectPool.Get();
        metadata.Transaction = transaction;

        //Act
        MessageMetadataObjectPool.Return(metadata);

        //Assert
        metadata.Transaction.ShouldBeNull();
        metadata.ProtocolMetadata().HasTxnidMostBits.ShouldBeFalse();
        metadata.ProtocolMetadata().HasTxnidLeastBits.ShouldBeFalse();
    }

    [Fact]
    public void Get_GivenMetadataReturnedWithTransaction_ShouldNotHaveTransaction()
    {
        //Arrange
        var transaction = Substitute.For<ITransaction>();
        transaction.Id.Returns(new TransactionId(1, 2));
        var metadata = MessageMetadataObjectPool.Get();
        metadata.Transaction = transaction;
        MessageMetadataObjectPool.Return(metadata);

        //Act
        var actual = MessageMetadataObjectPool.Get();

        //Assert
        actual.Transaction.ShouldBeNull();
        actual.ProtocolMetadata().HasTxnidMostBits.ShouldBeFalse();
        actual.ProtocolMetadata().HasTxnidLeastBits.ShouldBeFalse();
    }
}
