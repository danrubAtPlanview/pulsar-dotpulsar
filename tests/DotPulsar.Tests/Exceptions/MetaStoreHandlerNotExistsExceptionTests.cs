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

namespace DotPulsar.Tests.Exceptions;

using DotPulsar.Exceptions;

[Trait("Category", "Unit")]
public sealed class MetaStoreHandlerNotExistsExceptionTests
{
    [Fact]
    public void Constructor_GivenTcId_ShouldInterpolateTcIdInMessage()
    {
        //Arrange & Act
        var uut = new MetaStoreHandlerNotExistsException(3UL);

        //Assert
        uut.Message.ShouldBe("Transaction meta store handler for transaction meta store 3 not exists.");
        uut.ShouldBeAssignableTo<DotPulsarException>();
    }

    [Fact]
    public void Constructor_GivenMessage_ShouldUseMessage()
    {
        //Arrange & Act
        var uut = new MetaStoreHandlerNotExistsException("message");

        //Assert
        uut.Message.ShouldBe("message");
    }

    [Fact]
    public void Constructor_GivenMessageAndInnerException_ShouldUseBoth()
    {
        //Arrange
        var innerException = new Exception();

        //Act
        var uut = new MetaStoreHandlerNotExistsException("message", innerException);

        //Assert
        uut.Message.ShouldBe("message");
        uut.InnerException.ShouldBeSameAs(innerException);
    }
}
