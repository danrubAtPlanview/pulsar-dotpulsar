# Followup 0020: public API compatibility (open items after WP12)

Status: **item 1 is decided on 2026-11-01: opt-out ([decision 0010](../decisions/0010-behaviour-changes-are-opt-out.md), implemented in `d462dae8`, [wp13](../implementation-notes/wp13-behaviour-change-switches.md)); item 2 is open for the requester**; nothing else. Written on 2026-11-01 after the public interfaces were put back to what
`master` has ([wp12](../implementation-notes/wp12-non-breaking-public-api.md), decision [0009](../decisions/0009-no-breaking-public-api.md)).

The requirement: the work must not be a breaking change *in any way*. WP12 removed the breaking changes of the **public API** (no member of a
released interface, no removed or changed public member). What is left is below.

## 1. Behaviour changes that a user can observe (decided: opt-out, [decision 0010](../decisions/0010-behaviour-changes-are-opt-out.md))

These are not API breaks (the same code compiles and runs), but they change what an existing program sees. Each is in the CHANGELOG. The
spec made Java the reference (decision 0001), and most of them are what Java does.

| # | Change | Who sees it | Could be gated by |
|---|---|---|---|
| 1 | The protocol version that the client sends is 19, was 14 (decision 0004) | the broker may enable features for the client | not gateable (the transaction and the ack receipt need it); the code does not exceed 19 |
| 2 | A send that the broker rejects with `NotAllowedError` fails only that message (`NotAllowedException`), was: the producer faulted | a producer that was faulted by it | an option on `ProducerOptions` (default: the old behaviour) |
| 3 | A message larger than the maximum message size fails only its own send (`TooLargeMessageException`), was: the producer faulted | the same | the same option |
| 4 | `RedeliverUnacknowledgedMessages` with an empty list does nothing, was: redelivered everything | a caller that passed an empty list to redeliver all | keep the old behaviour for the empty list |
| 5 | `RedeliverUnacknowledgedMessages` with ids on an Exclusive or Failover subscription redelivers all unacknowledged messages (the broker only redelivers individual messages for Shared) | the same | not gateable (broker behaviour) |
| 6 | `RedeliverUnacknowledgedMessages` sends the ids without the batch index, once per entry, so a whole batch is redelivered; it drops the messages that were received and not delivered, and gives the permits back | a caller that relied on the old duplicates | fixes of wrong behaviour; not gated |
| 7 | `Seek` with the id of a message in a batch replays the batch from that message (needs the broker setting `acknowledgmentAtBatchIndexLevelEnabled`) | a caller that seeks into a batch | an option on the consumer |
| 8 | A consumer or reader no longer delivers a message that it cannot decrypt (default `CryptoFailureAction.Fail`), was: it delivered the ciphertext | a consumer without a key reader on an encrypted topic | default `Consume` restores the old behaviour, but differs from Java (default `FAIL`) |
| 9 | A consumer or reader delivers the chunks of a chunked message as one message, was: every chunk as a message | a consumer of a topic that a chunking producer writes | an option that is off by default; before this work nobody could have used the chunks, they are fragments of a message |
| 10 | State fixes: a partitioned consumer is `Active` only when every partition is; the state is published when the channel is in place; a partitioned producer reports `PartiallyConnected`, not `WaitingForExclusive` | a program that logs or reacts to states | fixes of wrong behaviour; not gated |
| 11 | Disposing a producer, consumer or reader waits at most `OperationTimeout` (30 s by default) for the close response, was: without a limit | a dispose on a dead connection | not gated (it only shortens a hang) |
| 12 | A producer without a name sends the name the broker generated, a fixed producer id and a growing epoch when it reconnects, so the broker deduplicates | a program that sent duplicates after a reconnect | not gated (the old behaviour lost data to duplicates) |

**Decided (2026-11-01, "Behavior changes - should become opt-out"):** the new behaviour stays the default and each change that has a legitimate previous behaviour gets an option that restores it: rows 2 and 3 (`ProducerOptions.FailOnlyTheRejectedMessage`), 4 (`ConsumerOptions.RedeliverNothingForNoMessageIds`), 7 (`SeekToBatchIndex` on `ConsumerOptions` and `ReaderOptions`), 8 (the existing `CryptoFailureAction`: `Consume`), 9 (`AssembleChunkedMessages` on `ConsumerOptions` and `ReaderOptions`). Rows 1, 5, 6, 10, 11 and 12 have no switch: they are decided by the wire or the broker, or they fix a defect. The planned names, the rule for later changes and the note that opt-in is a one-constant flip are in decision 0010.

Everything new is off by default: batching, chunking in the producer, the ack timeout, ack receipts, the send timeout, transactions, the dead letter
policy, the negative acknowledge backoff.

## 2. The `DotPulsar.Internal` namespace (open: confirm that it is not API)

27 members of public types in `DotPulsar.Internal` changed (constructors of `Consumer<T>`, `Producer<T>`, `Reader<T>`, `SubConsumer<T>`,
`SubProducer`, `SubReader<T>`, the channels and their factories, `BatchHandler<T>`, `MessagePackage`, `SendOp`; the signatures of
`IMessageFactory<T>.Create`, `BatchHandler<T>.Add` and `.Acknowledge`, `IConsumerChannel<T>.Send`, `AsyncQueueWithCursor<T>.RemoveCurrentItem`; the
generated `CommandNewTxn`), and internal interfaces (`IConnection`, `IProducerChannel`, `IConsumerChannel<T>`) got members. The package validation
reports them as `CP0002` and `CP0006`. They are public in the code only because the test project has no `InternalsVisibleTo`, and the namespace says
`Internal`.

Recommendation: they are not API. If the requester disagrees, the old constructors could be kept as overloads (13 of the 27), but the others change
return types or parameters and cannot be kept; that would be a different design (a facade), not a flag.

## 3. Limits of the extension methods (accepted, documented in decision 0009)

- they need `using DotPulsar.Extensions;`;
- a class that implements a public interface outside DotPulsar does not support the new calls (`NotSupportedException`, `null` for
  `GetEncryptionContext()`), a decorator included, until it implements the capability interface (which is in the `Internal` namespace);
- a mock of `IConsumer`, `IPulsarClient` or `IMessage` takes part only as `Substitute.For<IConsumer, IExtendedConsumer>()`; builders cannot be mocked
  for the new methods;
- `IMessage.EncryptionContext` is the method `GetEncryptionContext()`.

## 4. Keeping it that way

`PublicApiTests` (in the test project, `Category=Unit`) fails when a released interface changes. The package validation of
[wp12 section 9](../implementation-notes/wp12-non-breaking-public-api.md#9-verification) is the check to repeat before a pull request is opened and
whenever `master` moves: pack `master` and the branch, and compare them with `PackageValidationBaselinePath`.
