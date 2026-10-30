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

using DotPulsar.Exceptions;
using DotPulsar.Internal;

[Trait("Category", "Unit")]
public sealed class RetriableErrorsTests
{
    // Additional coverage
    [Theory]
    [InlineData(typeof(AuthorizationException))]
    [InlineData(typeof(InvalidSchemeException))]
    [InlineData(typeof(ConfigurationException))]
    [InlineData(typeof(TopicNotFoundException))]
    [InlineData(typeof(IncompatibleSchemaException))]
    [InlineData(typeof(SubscriptionNotFoundException))]
    [InlineData(typeof(InvalidTopicNameException))]
    [InlineData(typeof(UnsupportedVersionException))]
    [InlineData(typeof(NotAllowedException))]
    [InlineData(typeof(ChecksumException))]
    [InlineData(typeof(CryptoException))]
    [InlineData(typeof(ConsumerAssignException))]
    [InlineData(typeof(TransactionConflictException))]
    [InlineData(typeof(ProducerBusyException))]
    [InlineData(typeof(ConsumerBusyException))]
    [InlineData(typeof(TransactionHasOperationFailedException))]
    public void IsRetriable_GivenNonRetriableException_ShouldReturnFalse(Type exceptionType)
    {
        //Arrange
        var exception = CreateException(exceptionType);

        //Act
        var actual = RetriableErrors.IsRetriable(exception);

        //Assert
        actual.ShouldBeFalse();
    }

    // Additional coverage
    [Theory]
    [InlineData(typeof(AuthenticationException))]
    [InlineData(typeof(ServiceNotReadyException))]
    [InlineData(typeof(TooManyRequestsException))]
    [InlineData(typeof(UnknownException))]
    [InlineData(typeof(InvalidTransactionStatusException))]
    [InlineData(typeof(TransactionCoordinatorNotFoundException))]
    [InlineData(typeof(TransactionCoordinatorClientException))]
    [InlineData(typeof(TransactionCoordinatorClientStateException))]
    [InlineData(typeof(MetaStoreHandlerNotExistsException))]
    [InlineData(typeof(MetaStoreHandlerNotReadyException))]
    [InlineData(typeof(Exception))]
    [InlineData(typeof(InvalidOperationException))]
    [InlineData(typeof(TimeoutException))]
    [InlineData(typeof(IOException))]
    public void IsRetriable_GivenRetriableException_ShouldReturnTrue(Type exceptionType)
    {
        //Arrange
        var exception = CreateException(exceptionType);

        //Act
        var actual = RetriableErrors.IsRetriable(exception);

        //Assert
        actual.ShouldBeTrue();
    }

    private static Exception CreateException(Type exceptionType)
        => (Exception) Activator.CreateInstance(exceptionType, "message")!;
}
