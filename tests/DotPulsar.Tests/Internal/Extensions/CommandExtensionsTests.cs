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

namespace DotPulsar.Tests.Internal.Extensions;

using DotPulsar.Exceptions;
using DotPulsar.Internal.Extensions;
using DotPulsar.Internal.PulsarApi;

[Trait("Category", "Unit")]
public sealed class CommandExtensionsTests
{
    // Additional coverage
    [Theory]
    [InlineData(ServerError.TransactionConflict, typeof(TransactionConflictException))]
    [InlineData(ServerError.NotAllowedError, typeof(NotAllowedException))]
    [InlineData(ServerError.InvalidTxnStatus, typeof(InvalidTransactionStatusException))]
    [InlineData(ServerError.TransactionCoordinatorNotFound, typeof(TransactionCoordinatorNotFoundException))]
    [InlineData(ServerError.TransactionNotFound, typeof(TransactionNotFoundException))]
    [InlineData(ServerError.ConsumerNotFound, typeof(ConsumerNotFoundException))]
    [InlineData(ServerError.ServiceNotReady, typeof(ServiceNotReadyException))]
    [InlineData(ServerError.UnknownError, typeof(UnknownException))]
    public void ThrowIfAckError_GivenEachMappedServerError_ShouldThrowMappedException(ServerError error, Type expected)
    {
        //Arrange
        var command = CreateAckResponse(error);

        //Act
        var actual = Record.Exception(() => command.ThrowIfAckError());

        //Assert
        actual.ShouldNotBeNull();
        actual.GetType().ShouldBe(expected);
    }

    // Additional coverage
    [Fact]
    public void ThrowIfAckError_GivenNoError_ShouldNotThrow()
    {
        //Arrange
        var command = CreateAckResponse(null);

        //Act
        var actual = Record.Exception(() => command.ThrowIfAckError());

        //Assert
        actual.ShouldBeNull();
    }

    // Additional coverage
    [Fact]
    public void ThrowIfAckError_GivenError_ShouldKeepTheBrokerMessage()
    {
        //Arrange
        var command = CreateAckResponse(ServerError.TransactionConflict);

        //Act
        var actual = Record.Exception(() => command.ThrowIfAckError());

        //Assert
        actual.ShouldNotBeNull();
        actual.Message.ShouldBe("broker message");
    }

    // Additional coverage
    [Fact]
    public void ThrowIfAckError_GivenCommandThatIsNotAnAckResponse_ShouldThrowUnexpectedResponse()
    {
        //Arrange
        var command = new BaseCommand { Type = BaseCommand.Types.Type.Success, Success = new CommandSuccess() };

        //Act
        var actual = Record.Exception(() => command.ThrowIfAckError());

        //Assert
        actual.ShouldNotBeNull();
    }

    // Additional coverage
    [Fact]
    public void Throw_GivenAckResponseWithTransactionNotFound_ShouldThrowTransactionNotFoundException()
    {
        //Arrange
        var command = new CommandAckResponse { Error = ServerError.TransactionNotFound, Message = "not found" };

        //Act
        var actual = Record.Exception(() => command.Throw());

        //Assert
        actual.ShouldBeOfType<TransactionNotFoundException>();
    }

    // Additional coverage
    [Fact]
    public void ToException_GivenServerError_ShouldReturnTheExceptionWithoutThrowing()
    {
        //Act
        var actual = DotPulsar.Internal.Extensions.CommandExtensions.ToException(ServerError.TransactionNotFound, "message");

        //Assert
        actual.ShouldBeOfType<TransactionNotFoundException>();
        actual.Message.ShouldBe("message");
    }

    private static BaseCommand CreateAckResponse(ServerError? error)
    {
        var response = new CommandAckResponse { ConsumerId = 1, RequestId = 1 };

        if (error.HasValue)
        {
            response.Error = error.Value;
            response.Message = "broker message";
        }

        return new BaseCommand { Type = BaseCommand.Types.Type.AckResponse, AckResponse = response };
    }
}
