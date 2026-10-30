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

namespace DotPulsar.Internal;

using DotPulsar.Exceptions;

public static class RetriableErrors
{
    // Upstream: PulsarClientException#isRetriableError
    // Java lists the non-retriable types, so everything else is retriable, including authentication errors.
    // Java types without a DotPulsar equivalent: UnsupportedAuthenticationException, InvalidMessageException and MessageAcknowledgeException.
    // TODO: add CryptoException when it is introduced by the end-to-end encryption work package.
    public static bool IsRetriable(Exception exception)
        => exception is not (
            AuthorizationException or
            InvalidSchemeException or
            ConfigurationException or
            TopicNotFoundException or
            IncompatibleSchemaException or
            SubscriptionNotFoundException or
            InvalidTopicNameException or
            UnsupportedVersionException or
            NotAllowedException or
            ChecksumException or
            ConsumerAssignException or
            TransactionConflictException or
            ProducerBusyException or
            ConsumerBusyException or
            TransactionHasOperationFailedException);
}
