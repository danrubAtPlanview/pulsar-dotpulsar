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

using System.Reflection;

// The public interfaces of DotPulsar must not get new members: that breaks every implementation of them outside of DotPulsar (a class, a decorator, a test double).
// What a new feature needs is an extension method with the same name, which calls a capability interface (IExtendedConsumer, IExtendedProducer, IExtendedPulsarClient, IExtendedMessage) or the builder that DotPulsar
// creates. See decisions/0009 of the transactions spec and the CHANGELOG.
[Trait("Category", "Unit")]
public sealed class PublicApiTests
{
    // The members of the interfaces of DotPulsar.Abstractions that existed on master when the work started (e8f2ba2, after release 5.3.4), written as Name/NumberOfParameters or Name{get}{set}, sorted. The key is the name of the type.
    // An interface is allowed to be new, but a released one is not allowed to change. A member that is removed or changed is a breaking change too.
    private static readonly Dictionary<string, string> _releasedInterfaces = new()
    {
        ["IAuthentication"] = "AuthenticationMethodName{get};GetAuthenticationData/1",
        ["IConsumer"] = "Acknowledge/2;Acknowledge/2;AcknowledgeCumulative/2;RedeliverUnacknowledgedMessages/1;RedeliverUnacknowledgedMessages/2;ServiceUrl{get};SubscriptionName{get};SubscriptionType{get};Topic{get};Unsubscribe/1",
        ["IConsumerBuilder`1"] = "AllowOutOfOrderDeliver/1;ConsumerName/1;Create/0;InitialPosition/1;MessagePrefetchCount/1;PriorityLevel/1;ReadCompacted/1;RegexSubscriptionMode/1;ReplicateSubscriptionState/1;StateChangedHandler/1;SubscriptionName/1;SubscriptionProperty/2;SubscriptionType/1;Topic/1;Topics/1;TopicsPattern/1",
        ["IConsumer`1"] = "",
        ["IGetLastMessageIds"] = "GetLastMessageIds/1",
        ["IHandleException"] = "OnException/1",
        ["IHandleStateChanged`1"] = "CancellationToken{get};OnStateChanged/2",
        ["IMessage"] = "Data{get};EventTimeAsDateTimeOffset{get};EventTimeAsDateTime{get};EventTime{get};HasBase64EncodedKey{get};HasEventTime{get};HasKey{get};HasOrderingKey{get};KeyBytes{get};Key{get};MessageId{get};OrderingKey{get};ProducerName{get};Properties{get};PublishTimeAsDateTimeOffset{get};PublishTimeAsDateTime{get};PublishTime{get};RedeliveryCount{get};SchemaVersion{get};SequenceId{get}",
        ["IMessageBuilder`1"] = "DeliverAt/1;DeliverAt/1;DeliverAt/1;EventTime/1;EventTime/1;EventTime/1;Key/1;KeyBytes/1;OrderingKey/1;Property/2;SchemaVersion/1;Send/2;SequenceId/1",
        ["IMessageRouter"] = "ChoosePartition/2",
        ["IMessage`1"] = "Value/0",
        ["IProducer"] = "ServiceUrl{get};Topic{get}",
        ["IProducerBuilder`1"] = "AttachTraceInfoToMessages/1;CompressionType/1;Create/0;InitialSequenceId/1;MaxPendingMessages/1;MessageRouter/1;ProducerAccessMode/1;ProducerName/1;ProducerProperty/2;StateChangedHandler/1;Topic/1",
        ["IProducer`1"] = "SendChannel{get}",
        ["IPulsarClient"] = "CreateConsumer/1;CreateProducer/1;CreateReader/1;ServiceUrl{get}",
        ["IPulsarClientBuilder"] = "AuthenticateUsingClientCertificate/1;Authentication/1;Build/0;CheckCertificateRevocation/1;CloseInactiveConnectionsInterval/1;ConnectionSecurity/1;ExceptionHandler/1;KeepAliveInterval/1;ListenerName/1;RemoteCertificateValidation/1;RetryInterval/1;ServiceUrl/1;TrustedCertificateAuthority/1;VerifyCertificateAuthority/1;VerifyCertificateName/1",
        ["IReader"] = "ServiceUrl{get};Topic{get}",
        ["IReaderBuilder`1"] = "Create/0;MessagePrefetchCount/1;ReadCompacted/1;ReaderName/1;StartMessageId/1;StateChangedHandler/1;SubscriptionName/1;SubscriptionRolePrefix/1;Topic/1",
        ["IReader`1"] = "",
        ["IReceive`1"] = "Receive/1",
        ["ISchema`1"] = "Decode/2;Encode/1;SchemaInfo{get}",
        ["ISeek"] = "Seek/2;Seek/2",
        ["ISendChannel`1"] = "Complete/0;Completion/1;Send/4",
        ["ISend`1"] = "Send/3",
        ["IStateHolder`1"] = "State{get}",
        ["IState`1"] = "IsFinalState/0;IsFinalState/1;OnStateChangeFrom/2;OnStateChangeTo/2",
        ["IValidateRemoteCertificate"] = "Validate/4",
    };

    public static TheoryData<string> ReleasedInterfaces()
    {
        var data = new TheoryData<string>();
        foreach (var name in _releasedInterfaces.Keys)
            data.Add(name);
        return data;
    }

    [Theory]
    [MemberData(nameof(ReleasedInterfaces))]
    public void Members_GivenReleasedPublicInterface_ShouldBeTheMembersOfTheRelease(string name)
    {
        //Arrange
        var type = typeof(PulsarClient).Assembly.GetType($"DotPulsar.Abstractions.{name}");

        //Act
        var actual = type is null ? "the type does not exist" : GetMembers(type);

        //Assert
        actual.ShouldBe(_releasedInterfaces[name], $"{name} is a released public interface. Do not add members to it: that breaks every implementation of it outside of DotPulsar. Add an extension method that calls a capability interface instead.");
    }

    [Fact]
    public void Interfaces_GivenNoneOfTheReleasedOnes_ShouldBeTheOnesThatTheBranchAdded()
    {
        //Arrange
        var added = new[] { "ICryptoKeyReader", "IRedeliveryBackoff", "ITransaction", "ITransactionBuilder" };

        //Act
        var actual = typeof(PulsarClient).Assembly.GetExportedTypes()
            .Where(type => type.IsInterface && type.Namespace == "DotPulsar.Abstractions" && !_releasedInterfaces.ContainsKey(type.Name))
            .Select(type => type.Name)
            .OrderBy(name => name, StringComparer.Ordinal);

        //Assert
        actual.ShouldBe(added);
    }

    private static string GetMembers(Type type)
    {
        var members = type.GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Select(member => member switch
            {
                MethodInfo method when !method.IsSpecialName => $"{method.Name}/{method.GetParameters().Length}",
                PropertyInfo property => $"{property.Name}{{{(property.CanRead ? "get" : "")}{(property.CanWrite ? "set" : "")}}}",
                EventInfo e => $"event {e.Name}",
                _ => null
            })
            .Where(member => member is not null)
            .OrderBy(member => member, StringComparer.Ordinal);

        return string.Join(";", members);
    }
}
