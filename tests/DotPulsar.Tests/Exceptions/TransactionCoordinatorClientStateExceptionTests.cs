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
public sealed class TransactionCoordinatorClientStateExceptionTests
{
    [Fact]
    public void Constructor_GivenNoArguments_ShouldUseUpstreamMessage()
    {
        //Arrange & Act
        var uut = new TransactionCoordinatorClientStateException();

        //Assert
        uut.Message.ShouldBe("Unexpected state for transaction metadata client.");
        uut.ShouldBeAssignableTo<DotPulsarException>();
    }

    [Fact]
    public void Constructor_GivenMessage_ShouldUseMessage()
    {
        //Arrange & Act
        var uut = new TransactionCoordinatorClientStateException("message");

        //Assert
        uut.Message.ShouldBe("message");
    }

    [Fact]
    public void Constructor_GivenMessageAndInnerException_ShouldUseBoth()
    {
        //Arrange
        var innerException = new Exception();

        //Act
        var uut = new TransactionCoordinatorClientStateException("message", innerException);

        //Assert
        uut.Message.ShouldBe("message");
        uut.InnerException.ShouldBeSameAs(innerException);
    }
}
