# WP12: the public interfaces are not changed (as built)

Written on 2026-11-01. Commit `427df7dd` `Keep the public interfaces as they were` on `feature/transactions` (the commit after `fc04185d`;
it is local until the requester pushes it). Decision: [0009](../decisions/0009-no-breaking-public-api.md), which supersedes
[0003](../decisions/0003-public-api-as-interface-members.md).

**Why this note exists.** The branch was first built with new members on nine public interfaces, which is a breaking change of the public API,
and the requester ruled that out. Another session continued the work from the earlier shape. This note lists **every** change that undid the
breaking shape, so that the other session can bring its work to the same shape (section 6 is the recipe) and so that nothing that is built later
adds a member to a public interface again (section 2).

## 1. What changed, in one paragraph

The nine public interfaces (`IPulsarClient`, `IPulsarClientBuilder`, `IConsumer`, `IConsumerBuilder<T>`, `IProducer`, `IProducerBuilder<T>`,
`IReaderBuilder<T>`, `IMessage`, `IMessageBuilder<T>`) are **byte-for-byte what `master` has** (the only difference in `Abstractions/` is the
documentation of `ISeek`, which gained a `<remarks>` block about seeking to a batch). The 40 members that the work had added to them are extension
methods in `DotPulsar.Extensions` with the same names, parameters and documentation. Each extension method calls a capability interface that
DotPulsar's own implementations have (`IExtendedConsumer`, `IExtendedProducer`, `IExtendedPulsarClient`, `IExtendedMessage`), or the builder class
that DotPulsar creates, found by `ExtendedApi.Require<T>`. The implementations keep their public methods; they only declare the capability
interface in addition. Nothing else about the features changed: the same code runs, through one more call.

## 2. The rule for new work

**Never add a member to a public interface that `master` has** (`DotPulsar.Abstractions`: `IConsumer`, `IProducer`, `IReader`, `IPulsarClient`,
`IMessage`, `IMessage<T>`, `IMessageBuilder<T>`, `IConsumerBuilder<T>`, `IProducerBuilder<T>`, `IReaderBuilder<T>`, `IPulsarClientBuilder`,
`ISeek`, `IGetLastMessageIds`, `IStateHolder<T>`, `ISendChannel<T>`, `ISend<T>`, `IReceive<T>`, `ISchema<T>`, ...; `PublicApiTests` has the list).
A new interface is allowed; a new class, enum value or method of a class is allowed. For a call that would have been an interface member:

1. Add the method (or property) to the implementation class as a public member, as before.
2. If the type of the receiver is a non-generic interface (`IConsumer`, `IProducer`, `IPulsarClient`, `IMessage`): add the member to the capability
   interface (`IExtendedConsumer`, ...) in `src/DotPulsar/Internal/Abstractions/`, and make sure every class of DotPulsar that implements the public
   interface implements the capability interface (`Consumer<T>` **and** `SubConsumer<T>`, for example).
3. If the receiver is a builder: nothing to add; the method is a public method of the builder class.
4. Add the extension method with the same name, parameters and documentation to the extension class of the interface in `src/DotPulsar/Extensions/`.
   Its body is `=> ExtendedApi.Require<TheCapabilityOrBuilder>(x, nameof(x), nameof(Method)).Method(args);`.
5. Test it: a mock needs `Substitute.For<IConsumer, IExtendedConsumer>()` and the assertion on `((IExtendedConsumer) mock)`.
6. Never edit `PublicApiTests` to make it pass. It is the guard. (It changes only when `master` itself changes a released interface.)

## 3. The 40 members that moved

**`IPulsarClient`**, extension methods in `PulsarClientExtensions`, reaching `IExtendedPulsarClient` (implemented by `PulsarClient`)

| Member that is not on the interface any more | Extension method (same name, same parameters, same documentation) |
|---|---|
| `IPulsarClient.CreateTransaction(TransactionOptions options, CancellationToken cancellationToken = default)` | `CreateTransaction(this IPulsarClient x, TransactionOptions options, CancellationToken cancellationToken = default)` |

**`IPulsarClientBuilder`**, extension methods in `PulsarClientBuilderExtensions`, reaching the `PulsarClientBuilder` that `PulsarClient.Builder()` creates

| Member that is not on the interface any more | Extension method (same name, same parameters, same documentation) |
|---|---|
| `IPulsarClientBuilder.OperationTimeout(TimeSpan operationTimeout)` | `OperationTimeout(this IPulsarClientBuilder x, TimeSpan operationTimeout)` |
| `IPulsarClientBuilder.EnableTransaction(bool enableTransaction)` | `EnableTransaction(this IPulsarClientBuilder x, bool enableTransaction)` |

**`IConsumer`**, extension methods in `ConsumerExtensions`, reaching `IExtendedConsumer` (implemented by `Consumer<T>` and `SubConsumer<T>`)

| Member that is not on the interface any more | Extension method (same name, same parameters, same documentation) |
|---|---|
| `IConsumer.Acknowledge(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken = default)` | `Acknowledge(this IConsumer x, MessageId messageId, ITransaction transaction, CancellationToken cancellationToken = default)` |
| `IConsumer.Acknowledge(IEnumerable<MessageId> messageIds, ITransaction transaction, CancellationToken cancellationToken = default)` | `Acknowledge(this IConsumer x, IEnumerable<MessageId> messageIds, ITransaction transaction, CancellationToken cancellationToken = default)` |
| `IConsumer.AcknowledgeCumulative(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken = default)` | `AcknowledgeCumulative(this IConsumer x, MessageId messageId, ITransaction transaction, CancellationToken cancellationToken = default)` |
| `IConsumer.NegativeAcknowledge(MessageId messageId, CancellationToken cancellationToken = default)` | `NegativeAcknowledge(this IConsumer x, MessageId messageId, CancellationToken cancellationToken = default)` |
| `IConsumer.NegativeAcknowledge(MessageId messageId, uint redeliveryCount, CancellationToken cancellationToken = default)` | `NegativeAcknowledge(this IConsumer x, MessageId messageId, uint redeliveryCount, CancellationToken cancellationToken = default)` |

**`IConsumerBuilder<TMessage>`**, extension methods in `ConsumerBuilderExtensions`, reaching the `ConsumerBuilder<TMessage>` that `NewConsumer` creates

| Member that is not on the interface any more | Extension method (same name, same parameters, same documentation) |
|---|---|
| `IConsumerBuilder<TMessage>.AckReceiptEnabled(bool ackReceiptEnabled)` | `AckReceiptEnabled<TMessage>(this IConsumerBuilder<TMessage> x, bool ackReceiptEnabled)` |
| `IConsumerBuilder<TMessage>.BatchIndexAckEnabled(bool batchIndexAckEnabled)` | `BatchIndexAckEnabled<TMessage>(this IConsumerBuilder<TMessage> x, bool batchIndexAckEnabled)` |
| `IConsumerBuilder<TMessage>.AckTimeout(TimeSpan ackTimeout)` | `AckTimeout<TMessage>(this IConsumerBuilder<TMessage> x, TimeSpan ackTimeout)` |
| `IConsumerBuilder<TMessage>.AckTimeoutTickTime(TimeSpan tickTime)` | `AckTimeoutTickTime<TMessage>(this IConsumerBuilder<TMessage> x, TimeSpan tickTime)` |
| `IConsumerBuilder<TMessage>.AckTimeoutRedeliveryBackoff(IRedeliveryBackoff redeliveryBackoff)` | `AckTimeoutRedeliveryBackoff<TMessage>(this IConsumerBuilder<TMessage> x, IRedeliveryBackoff redeliveryBackoff)` |
| `IConsumerBuilder<TMessage>.DeadLetterPolicy(DeadLetterPolicy deadLetterPolicy)` | `DeadLetterPolicy<TMessage>(this IConsumerBuilder<TMessage> x, DeadLetterPolicy deadLetterPolicy)` |
| `IConsumerBuilder<TMessage>.NegativeAcknowledgeRedeliveryDelay(TimeSpan delay)` | `NegativeAcknowledgeRedeliveryDelay<TMessage>(this IConsumerBuilder<TMessage> x, TimeSpan delay)` |
| `IConsumerBuilder<TMessage>.NegativeAcknowledgeRedeliveryBackoff(IRedeliveryBackoff redeliveryBackoff)` | `NegativeAcknowledgeRedeliveryBackoff<TMessage>(this IConsumerBuilder<TMessage> x, IRedeliveryBackoff redeliveryBackoff)` |
| `IConsumerBuilder<TMessage>.MaxPendingChunkedMessage(uint maxPendingChunkedMessage)` | `MaxPendingChunkedMessage<TMessage>(this IConsumerBuilder<TMessage> x, uint maxPendingChunkedMessage)` |
| `IConsumerBuilder<TMessage>.AutoAckOldestChunkedMessageOnQueueFull(bool autoAckOldestChunkedMessageOnQueueFull)` | `AutoAckOldestChunkedMessageOnQueueFull<TMessage>(this IConsumerBuilder<TMessage> x, bool autoAckOldestChunkedMessageOnQueueFull)` |
| `IConsumerBuilder<TMessage>.ExpireTimeOfIncompleteChunkedMessage(TimeSpan expireTimeOfIncompleteChunkedMessage)` | `ExpireTimeOfIncompleteChunkedMessage<TMessage>(this IConsumerBuilder<TMessage> x, TimeSpan expireTimeOfIncompleteChunkedMessage)` |
| `IConsumerBuilder<TMessage>.CryptoKeyReader(ICryptoKeyReader cryptoKeyReader)` | `CryptoKeyReader<TMessage>(this IConsumerBuilder<TMessage> x, ICryptoKeyReader cryptoKeyReader)` |
| `IConsumerBuilder<TMessage>.CryptoFailureAction(ConsumerCryptoFailureAction cryptoFailureAction)` | `CryptoFailureAction<TMessage>(this IConsumerBuilder<TMessage> x, ConsumerCryptoFailureAction cryptoFailureAction)` |

**`IProducer`**, extension methods in `ProducerExtensions`, reaching `IExtendedProducer` (implemented by `Producer<T>`)

| Member that is not on the interface any more | Extension method (same name, same parameters, same documentation) |
|---|---|
| `IProducer.Flush(CancellationToken cancellationToken = default)` | `Flush(this IProducer x, CancellationToken cancellationToken = default)` |

**`IProducerBuilder<TMessage>`**, extension methods in `ProducerBuilderExtensions`, reaching the `ProducerBuilder<TMessage>` that `NewProducer` creates

| Member that is not on the interface any more | Extension method (same name, same parameters, same documentation) |
|---|---|
| `IProducerBuilder<TMessage>.EnableBatching(bool enableBatching)` | `EnableBatching<TMessage>(this IProducerBuilder<TMessage> x, bool enableBatching)` |
| `IProducerBuilder<TMessage>.BatchingMaxMessages(uint batchingMaxMessages)` | `BatchingMaxMessages<TMessage>(this IProducerBuilder<TMessage> x, uint batchingMaxMessages)` |
| `IProducerBuilder<TMessage>.BatchingMaxBytes(uint batchingMaxBytes)` | `BatchingMaxBytes<TMessage>(this IProducerBuilder<TMessage> x, uint batchingMaxBytes)` |
| `IProducerBuilder<TMessage>.BatchingMaxPublishDelay(TimeSpan batchingMaxPublishDelay)` | `BatchingMaxPublishDelay<TMessage>(this IProducerBuilder<TMessage> x, TimeSpan batchingMaxPublishDelay)` |
| `IProducerBuilder<TMessage>.BatcherType(BatcherType batcherType)` | `BatcherType<TMessage>(this IProducerBuilder<TMessage> x, BatcherType batcherType)` |
| `IProducerBuilder<TMessage>.SendTimeout(TimeSpan sendTimeout)` | `SendTimeout<TMessage>(this IProducerBuilder<TMessage> x, TimeSpan sendTimeout)` |
| `IProducerBuilder<TMessage>.EnableChunking(bool enableChunking)` | `EnableChunking<TMessage>(this IProducerBuilder<TMessage> x, bool enableChunking)` |
| `IProducerBuilder<TMessage>.ChunkMaxMessageSize(int chunkMaxMessageSize)` | `ChunkMaxMessageSize<TMessage>(this IProducerBuilder<TMessage> x, int chunkMaxMessageSize)` |
| `IProducerBuilder<TMessage>.CryptoKeyReader(ICryptoKeyReader cryptoKeyReader)` | `CryptoKeyReader<TMessage>(this IProducerBuilder<TMessage> x, ICryptoKeyReader cryptoKeyReader)` |
| `IProducerBuilder<TMessage>.EncryptionKey(string keyName)` | `EncryptionKey<TMessage>(this IProducerBuilder<TMessage> x, string keyName)` |
| `IProducerBuilder<TMessage>.CryptoFailureAction(ProducerCryptoFailureAction cryptoFailureAction)` | `CryptoFailureAction<TMessage>(this IProducerBuilder<TMessage> x, ProducerCryptoFailureAction cryptoFailureAction)` |

**`IReaderBuilder<TMessage>`**, extension methods in `ReaderBuilderExtensions`, reaching the `ReaderBuilder<TMessage>` that `NewReader` creates

| Member that is not on the interface any more | Extension method (same name, same parameters, same documentation) |
|---|---|
| `IReaderBuilder<TMessage>.MaxPendingChunkedMessage(uint maxPendingChunkedMessage)` | `MaxPendingChunkedMessage<TMessage>(this IReaderBuilder<TMessage> x, uint maxPendingChunkedMessage)` |
| `IReaderBuilder<TMessage>.AutoAckOldestChunkedMessageOnQueueFull(bool autoAckOldestChunkedMessageOnQueueFull)` | `AutoAckOldestChunkedMessageOnQueueFull<TMessage>(this IReaderBuilder<TMessage> x, bool autoAckOldestChunkedMessageOnQueueFull)` |
| `IReaderBuilder<TMessage>.ExpireTimeOfIncompleteChunkedMessage(TimeSpan expireTimeOfIncompleteChunkedMessage)` | `ExpireTimeOfIncompleteChunkedMessage<TMessage>(this IReaderBuilder<TMessage> x, TimeSpan expireTimeOfIncompleteChunkedMessage)` |
| `IReaderBuilder<TMessage>.CryptoKeyReader(ICryptoKeyReader cryptoKeyReader)` | `CryptoKeyReader<TMessage>(this IReaderBuilder<TMessage> x, ICryptoKeyReader cryptoKeyReader)` |
| `IReaderBuilder<TMessage>.CryptoFailureAction(ConsumerCryptoFailureAction cryptoFailureAction)` | `CryptoFailureAction<TMessage>(this IReaderBuilder<TMessage> x, ConsumerCryptoFailureAction cryptoFailureAction)` |

**`IMessageBuilder<TMessage>`**, extension methods in `MessageBuilderExtensions`, reaching the `MessageBuilder<TMessage>` that `NewMessage` creates

| Member that is not on the interface any more | Extension method (same name, same parameters, same documentation) |
|---|---|
| `IMessageBuilder<TMessage>.Transaction(ITransaction transaction)` | `Transaction<TMessage>(this IMessageBuilder<TMessage> x, ITransaction transaction)` |

**`IMessage`**, extension method in `MessageExtensions`, reading `IExtendedMessage` (implemented by `Message<T>`; `null` for an `IMessage` that DotPulsar did not create)

| Member that is not on the interface any more | Extension method |
|---|---|
| `IMessage.EncryptionContext { get; }` | `EncryptionContext? GetEncryptionContext(this IMessage message)` (a method: an extension property needs C# 14 at the call site) |

Total: 40 members.


## 4. New types

All in `DotPulsar.Internal` (public, as the repo's convention is, because the test project has no `InternalsVisibleTo`; the namespace says
they are not API), except where stated.

| Type | What it is |
|---|---|
| `ExtendedApi` (`src/DotPulsar/Internal/ExtendedApi.cs`, **internal**) | `Require<T>(object? instance, string parameterName, string member)`: `ArgumentNullException(parameterName)` for `null`; the instance as `T`; otherwise `NotSupportedException("'member' is not supported by 'type'. It is only supported by the implementations that DotPulsar creates.")` |
| `IExtendedConsumer` (`Internal/Abstractions`) | `Acknowledge(MessageId, ITransaction, ct)`, `Acknowledge(IEnumerable<MessageId>, ITransaction, ct)`, `AcknowledgeCumulative(MessageId, ITransaction, ct)`, `NegativeAcknowledge(MessageId, ct)`, `NegativeAcknowledge(MessageId, uint redeliveryCount, ct)` |
| `IExtendedProducer` | `Flush(ct)` |
| `IExtendedPulsarClient` | `CreateTransaction(TransactionOptions, ct)` |
| `IExtendedMessage` | `EncryptionContext? EncryptionContext { get; }` |

Declared on: `Consumer<T>`, `SubConsumer<T>` (`IExtendedConsumer`); `Producer<T>` (`IExtendedProducer`); `PulsarClient` (`IExtendedPulsarClient`);
`Message<T>` (`IExtendedMessage`). Their methods already existed; only the declaration changed.

Other change in `src`: `Internal/Extensions/TaskExtensions` (the `Task.WaitAsync` polyfill for targets before .NET 6) is **internal**. It was a public
class that only the `netstandard` builds had, so the package validation reported a type that `netstandard2.1` has and `net8.0` lacks.

## 5. Files of the commit (`git show --stat 427df7dd`)

Source (`src/`):

- `src/DotPulsar/Abstractions/IConsumer.cs` (changed)
- `src/DotPulsar/Abstractions/IConsumerBuilder.cs` (changed)
- `src/DotPulsar/Abstractions/IMessage.cs` (changed)
- `src/DotPulsar/Abstractions/IMessageBuilder.cs` (changed)
- `src/DotPulsar/Abstractions/IProducer.cs` (changed)
- `src/DotPulsar/Abstractions/IProducerBuilder.cs` (changed)
- `src/DotPulsar/Abstractions/IPulsarClient.cs` (changed)
- `src/DotPulsar/Abstractions/IPulsarClientBuilder.cs` (changed)
- `src/DotPulsar/Abstractions/IReaderBuilder.cs` (changed)
- `src/DotPulsar/Extensions/ConsumerBuilderExtensions.cs` (changed)
- `src/DotPulsar/Extensions/ConsumerExtensions.cs` (changed)
- `src/DotPulsar/Extensions/MessageBuilderExtensions.cs` (changed)
- `src/DotPulsar/Extensions/MessageExtensions.cs` (changed)
- `src/DotPulsar/Extensions/ProducerBuilderExtensions.cs` (changed)
- `src/DotPulsar/Extensions/ProducerExtensions.cs` (changed)
- `src/DotPulsar/Extensions/PulsarClientBuilderExtensions.cs` (changed)
- `src/DotPulsar/Extensions/PulsarClientExtensions.cs` (changed)
- `src/DotPulsar/Extensions/ReaderBuilderExtensions.cs` (changed)
- `src/DotPulsar/Internal/Abstractions/IExtendedConsumer.cs` (new)
- `src/DotPulsar/Internal/Abstractions/IExtendedMessage.cs` (new)
- `src/DotPulsar/Internal/Abstractions/IExtendedProducer.cs` (new)
- `src/DotPulsar/Internal/Abstractions/IExtendedPulsarClient.cs` (new)
- `src/DotPulsar/Internal/Consumer.cs` (changed)
- `src/DotPulsar/Internal/ExtendedApi.cs` (new)
- `src/DotPulsar/Internal/Extensions/TaskExtensions.cs` (changed)
- `src/DotPulsar/Internal/Message.cs` (changed)
- `src/DotPulsar/Internal/Producer.cs` (changed)
- `src/DotPulsar/Internal/SubConsumer.cs` (changed)
- `src/DotPulsar/Internal/TransactionBuilder.cs` (changed)
- `src/DotPulsar/PulsarClient.cs` (changed)

Tests (`tests/`):

- `tests/DotPulsar.Tests/AuthenticatedTransactionFixture.cs` (changed)
- `tests/DotPulsar.Tests/Encryption/ConsumerEncryptionTests.cs` (changed)
- `tests/DotPulsar.Tests/Encryption/EncryptionInteropTests.cs` (changed)
- `tests/DotPulsar.Tests/Encryption/EncryptionRequiredTests.cs` (changed)
- `tests/DotPulsar.Tests/Extensions/ConsumerExtensionsNegativeAcknowledgeTests.cs` (changed)
- `tests/DotPulsar.Tests/Extensions/ConsumerExtensionsTests.cs` (changed)
- `tests/DotPulsar.Tests/Extensions/ExtendedApiTests.cs` (new)
- `tests/DotPulsar.Tests/Internal/BatchHandlerTests.cs` (changed)
- `tests/DotPulsar.Tests/Internal/ConsumerChannelFactoryTests.cs` (changed)
- `tests/DotPulsar.Tests/Internal/Crypto/ConsumerChannelEncryptionTests.cs` (changed)
- `tests/DotPulsar.Tests/Internal/MessageFactoryTests.cs` (changed)
- `tests/DotPulsar.Tests/Internal/Transactions/TransactionBuilderTests.cs` (changed)
- `tests/DotPulsar.Tests/PublicApiTests.cs` (new)
- `tests/DotPulsar.Tests/Transactions/TransactionTests.cs` (changed)

Documentation (repository): `CHANGELOG.md`, `docs/api/index.md`.

## 6. How to bring another branch to this shape

Assumption: the other branch has the interface members, and its implementation classes have the same methods (they do, they are the same code).

1. **Cherry-pick first.** If the other branch contains the work up to `fc04185d`, `git cherry-pick 427df7dd` does most of it. The conflicts that to
   expect, and what to do:
   - the nine interface files in `src/DotPulsar/Abstractions/`: take the `master` version (`git checkout master -- <file>`), also for any member
     that the other branch added after `fc04185d`. Each such member needs steps 2 and 3 of the rule (section 2);
   - the nine extension classes: keep both sides (the other branch's extension methods and the new ones that are generated here);
   - the test files: see section 7.
2. **No cherry-pick possible: do it by hand, in this order.**
   1. `git checkout master -- src/DotPulsar/Abstractions/{IConsumer,IConsumerBuilder,IMessage,IMessageBuilder,IProducer,IProducerBuilder,IPulsarClient,IPulsarClientBuilder,IReaderBuilder}.cs`
      (before that, save the `<summary>` text of each member that the file loses: it is the documentation of the extension method).
   2. Add the five new files of section 4 (copy them from `427df7dd`).
   3. Add the extension methods of section 3 to the nine extension classes (copy them from `427df7dd`; add `using DotPulsar.Internal;` and
      `using DotPulsar.Internal.Abstractions;` where the file lacks them).
   4. Add the capability interface to the declarations of `Consumer<T>`, `SubConsumer<T>`, `Producer<T>`, `PulsarClient`, `Message<T>`, and to any other
      class of the other branch that implements `IConsumer<T>`, `IProducer<T>`, `IPulsarClient` or `IMessage<T>`.
   5. `Internal/Extensions/TaskExtensions.cs`: `internal static class`.
3. **Call sites** (the compiler lists them):
   - `message.EncryptionContext` on an `IMessage` becomes `message.GetEncryptionContext()`; where the code relied on the nullable flow state after
     `ShouldNotBeNull()`, the later uses need `!` (`message.GetEncryptionContext()!.Keys`);
   - a file that calls a moved member through the interface type needs `using DotPulsar.Extensions;` (production code in `DotPulsar.Internal` too:
     `TransactionBuilder` needed it);
   - code that holds the concrete class (`Consumer<T>`, `ConsumerBuilder<T>`, ...) keeps calling the instance members, no change.
4. **Mocks** (`Substitute.For<...>`): see section 7.
5. **Documentation**: `CHANGELOG.md` (an `Added` line that says the calls are extension methods and that nothing that implements the interfaces
   breaks; no 'breaking change' lines under `Changed`), `docs/api/index.md` (one paragraph under 'Transactions'), the spec files listed in
   decision 0009.
6. **Guards**: add `tests/DotPulsar.Tests/PublicApiTests.cs` and `tests/DotPulsar.Tests/Extensions/ExtendedApiTests.cs` from `427df7dd` (37 test
   cases). If the other branch added interfaces after `fc04185d`, add their names to the list of new interfaces in
   `Interfaces_GivenNoneOfTheReleasedOnes_ShouldBeTheOnesThatTheBranchAdded` (the only legitimate edit of that file).
7. **Verify** with section 9 (package validation against a `master` build): the result must have no `CP0002`, `CP0006` or other error outside the
   `DotPulsar.Internal` namespace, and the guard tests must pass.

## 7. Changes in the tests

| Change | Files | Why |
|---|---|---|
| `message.EncryptionContext` became `message.GetEncryptionContext()` (76 places) | `Encryption/ConsumerEncryptionTests`, `Encryption/EncryptionInteropTests`, `Encryption/EncryptionRequiredTests`, `Internal/BatchHandlerTests`, `Internal/ConsumerChannelFactoryTests`, `Internal/Crypto/ConsumerChannelEncryptionTests`, `Internal/MessageFactoryTests`, `Transactions/TransactionTests` | the property is an extension method now |
| `!` after `GetEncryptionContext()` (22 places) | `ConsumerEncryptionTests`, `EncryptionInteropTests`, `ConsumerChannelEncryptionTests`, `TransactionTests` | the compiler tracked the property as not null after `ShouldNotBeNull()`, and does not track a method result; without it the build has 44 new CS8602 warnings (the build must keep the 4 warnings that `master` has) |
| `using DotPulsar.Extensions;` added | `AuthenticatedTransactionFixture`, `BatchHandlerTests`, `ConsumerChannelFactoryTests`, `ConsumerChannelEncryptionTests`, `MessageFactoryTests` | they call moved members through the interface type |
| mocks: `Substitute.For<IConsumer, IExtendedConsumer>()` and assertions on `((IExtendedConsumer) consumer)` | `Extensions/ConsumerExtensionsTests`, `Extensions/ConsumerExtensionsNegativeAcknowledgeTests` | `consumer.Received().NegativeAcknowledge(...)` on a plain `IConsumer` mock is the extension method now, and throws `NotSupportedException` |
| mocks: `Substitute.For<IPulsarClient, IExtendedPulsarClient>()`, `extendedClient.CreateTransaction(...)`, `using DotPulsar.Internal.Abstractions;` | `Internal/Transactions/TransactionBuilderTests` | same, for `CreateTransaction` |
| a mock that throws leaves a half configured argument matcher in NSubstitute's thread-local state | (symptom only) | 15 tests failed in the first run: 13 were these mocks, and 2 (`ProducerChunkingTests.Send_GivenChunkedMessageAndPartitionedTopic_...` with a `TaskCanceledException`, and `MessageMetadataObjectPoolTests.Return_GivenMetadataWithTransaction_...` with an `UnexpectedArgumentMatcherException`) failed in the same run and passed once the mocks were fixed; the leaked matcher of a test that ran before them on the same thread is the likely cause |
| new `PublicApiTests` (27 interface rows + 1) | `tests/DotPulsar.Tests/PublicApiTests.cs` | the guard of section 2; mutation checked: adding `ValueTask Mutation() => default;` to `IProducer` fails the row `IProducer` |
| new `ExtendedApiTests` (9 tests) | `tests/DotPulsar.Tests/Extensions/ExtendedApiTests.cs` | `NotSupportedException` for a consumer, producer, client and the five builders that DotPulsar did not create; `ArgumentNullException`; `GetEncryptionContext` for a foreign message and for an extended one; the DotPulsar classes have the capability interfaces; the real builder accepts the extension methods |

## 8. What is still different from `master` (not changed by this work package)

1. **`DotPulsar.Internal.*` types.** 27 members of public types in the `Internal` namespace changed, and 15 members were added to internal
   interfaces (`IConnection`, `IProducerChannel`, `IConsumerChannel<T>`). The types are `public` only because the repo has no `InternalsVisibleTo`;
   the namespace says that they are not API. They are not preserved. Open question for the requester: followup
   [0020](../followups/0020-public-api-compatibility.md), item 2.
2. **Behaviour changes** (the CHANGELOG lists them under `Changed` and `Fixed`): the protocol version, `RedeliverUnacknowledgedMessages`, `Seek` to a
   message of a batch, undecryptable messages, chunked messages delivered as one message, the state of partitioned consumers and producers, the
   bounded wait when disposing, producers without a name. They are not API breaks, but a user can observe them. Open question for the requester:
   followup 0020, item 1.

## 9. Verification

Run on 2026-11-01 on the commit `427df7dd`.

**A. The interfaces equal `master`.** `git diff --name-status master 427df7dd -- src/DotPulsar/Abstractions` lists only new files (`A`) and
`ISeek.cs` (`M`, a `<remarks>` block of documentation). None of the nine interfaces is in it.

**B. Package validation against a build of `master`** (this is the check to repeat, section 6 step 7). It packs `master` and the branch with the
SDK, and lets the SDK compare every target framework of the branch with the package of `master`, and the target frameworks with each other:

```
git worktree add --detach <dir>/master master          # e8f2ba2
git worktree add --detach <dir>/branch feature/transactions
# in each worktree:
dotnet restore src/DotPulsar/DotPulsar.csproj --source https://api.nuget.org/v3/index.json
dotnet build   src/DotPulsar/DotPulsar.csproj -c Release --no-restore
dotnet pack    src/DotPulsar/DotPulsar.csproj -c Release --no-build --no-restore -o <dir>/pk-<master|branch>
# in the branch worktree (package version 1.0.0 in both, because the worktrees have no tag):
dotnet pack src/DotPulsar/DotPulsar.csproj -c Release --no-build --no-restore -o <dir>/pk-validate \
    -p:EnablePackageValidation=true -p:PackageValidationBaselinePath=<dir>/pk-master/DotPulsar.1.0.0.nupkg
# add -p:EnableStrictModeForBaselineValidation=true to list the additions as well
```

| | Before WP12 (`fc04185d`) | After WP12 (`427df7dd`) |
|---|---|---|
| Findings outside `DotPulsar.Internal` | **40**, all `CP0006` (member added to a public interface) | **0** |
| Findings inside `DotPulsar.Internal` | 43 (27 x `CP0002`, 15 x `CP0006`, 1 x `CP0001`: `TaskExtensions`, a public type that the `netstandard` builds had and `net8.0` lacked) | 42 (27 x `CP0002`, 15 x `CP0006`): see section 8 |
| Strict mode (additions), outside `DotPulsar.Internal` | - | 161: 137 new members (`CP0002`), 24 new types (`CP0001`); no changed or removed member, no member added to an existing interface |

(The tool reports a finding once per target framework; the numbers above are unique findings after the framework name was removed from the message.)

**C. The samples and benchmarks of `master` still compile**, unchanged, against the branch: `samples/Consuming`, `Producing`, `Reading`,
`Extensions`, `benchmarks/Compression`, and also `samples/Transactions` and `benchmarks/Transactions` (build, 0 errors).

**D. Tests** (the build has only the 4 warnings that `master` has):

| Run | Result |
|---|---|
| Unit, Release, 3 of 3 runs | 2377 passed, 0 failed (2340 before + the 37 new cases) |
| Unit, Debug | 2366 passed, 11 skipped (the allocation tests, which are measured in an optimised build only), 0 failed |
| Integration (`Category=Integration`, Release and Debug), and the stress of the unit suite (8 rounds of 4 processes under CPU load) | **not finished when this note was written** (they were running on `427df7dd`). The numbers are added here by the next checkpoint of this folder. The last full integration run, on `fc04185d` before this change, was 569 tests, 555 passed, 14 skipped, 0 failed in Debug and in Release; this change adds no integration test and moves calls to extension methods only. |
