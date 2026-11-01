# WP13: switches for the behaviour changes (as built)

Written on 2026-11-01. Commit `d462dae8` `Make the behaviour changes opt-out` on `feature/transactions` (the commit after `427df7dd`, WP12).
Decision: [0010](../decisions/0010-behaviour-changes-are-opt-out.md). Open items: [followup 0020](../followups/0020-public-api-compatibility.md).

**Why this note exists.** The requester decided that the changes of behaviour that an existing program can observe are **opt-out**: the new
behaviour stays the default, and an option restores the previous one. Another session continued the work from the shape before this decision.
This note lists every option, every place that it touches, the tests, and a recipe, so that the other session can bring its work to the same
shape and apply the same rule to any behaviour change that it made.

**The interpretation, stated so that it can be corrected:** "opt-out" is taken to mean that the **new** behaviour is the default and the **previous**
behaviour is the option. If "opt-in" was meant (the previous behaviour stays the default), flip the `Default...` constant of each option below
and the text that says 'The default is true' (XML documentation of the options and the builder methods, the CHANGELOG line, the table in
`docs/api/index.md`), and the tests of the defaults. Nothing else changes.

## 1. The switches

All options default to `true`, which is the new behaviour. `false` is the previous behaviour. Every option is a property of an options class with a
`Default...` constant (like `ProducerOptions.DefaultEnableChunking`), a builder method that is an extension method (decision 0009) calling the public
method of the builder class, and a CHANGELOG line under `Changed` or `Fixed` that names it.

| Option | Where | Builder method | `false` gives (the previous behaviour) | Where it acts |
|---|---|---|---|---|
| `FailOnlyTheRejectedMessage` | `ProducerOptions<T>` | `IProducerBuilder<T>.FailOnlyTheRejectedMessage(bool)` | a send that the broker rejects with `NotAllowedError`, and a message that is larger than the maximum message size, **fault the producer** (the exception reaches the executor: `NotAllowedException` and `TooLargeMessageException` are `DotPulsarException`s, so `DefaultExceptionHandler` returns `Rethrow`) | `SubProducer`: constructor parameter `failOnlyTheRejectedMessage`; `AwaitSend` has `catch (TooLargeMessageException) when (_failOnlyTheRejectedMessage)`; the `NotAllowedError` branch of the response processor tests it; `Producer.CreateSubProducer` passes `_options.FailOnlyTheRejectedMessage` |
| `RedeliverNothingForNoMessageIds` | `ConsumerOptions<T>` | `IConsumerBuilder<T>.RedeliverNothingForNoMessageIds(bool)` | `RedeliverUnacknowledgedMessages` with an **empty list** redelivers everything, for a consumer of one topic (a consumer of several topics did nothing before and still does) | `SubConsumer`: constructor parameter `redeliverNothingForNoMessageIds`; in `RedeliverUnacknowledgedMessages(IEnumerable<MessageId>)` an empty list calls `RedeliverUnacknowledgedMessages(ct)` when it is false |
| `SeekToBatchIndex` | `ConsumerOptions<T>`, `ReaderOptions<T>` | `IConsumerBuilder<T>.SeekToBatchIndex(bool)`, `IReaderBuilder<T>.SeekToBatchIndex(bool)` | a seek to the id of a message in a batch ignores the position in the batch, so the broker **replays the whole batch** | `MessageIdDataExtensions.ToSeekMessageIdData(this MessageId, bool seekToBatchIndex)` (new overload; the old one calls it with `true`) does not write the ack set when false; `SubConsumer` and `SubReader`: constructor parameter `seekToBatchIndex` |
| `AssembleChunkedMessages` | `ConsumerOptions<T>`, `ReaderOptions<T>` | `IConsumerBuilder<T>.AssembleChunkedMessages(bool)`, `IReaderBuilder<T>.AssembleChunkedMessages(bool)` | **every chunk is delivered as a message** (the assembler is not used; `MaxPendingChunkedMessage`, `AutoAckOldestChunkedMessageOnQueueFull` and `ExpireTimeOfIncompleteChunkedMessage` have no effect) | `ChunkedMessageAssembler`: new constructor with `bool enabled` (the old constructor calls it with `true`) and the property `Enabled`; `ConsumerChannel` tests `_chunkAssembler.Enabled && metadata.IsChunkedMessage()` in the two places that look at chunks (the undecryptable branch and the chunk branch); `Consumer` and `Reader` pass `AssembleChunkedMessages` to the assembler |

That is **six new properties** (`ProducerOptions` 1, `ConsumerOptions` 3, `ReaderOptions` 2) and six builder methods.

No constructor or method of the `Internal` namespace lost a parameter: `SubProducer`, `SubConsumer` and `SubReader` got optional parameters at the end,
`ChunkedMessageAssembler` and `ToSeekMessageIdData` got an overload, so the callers of the earlier signatures compile.

## 2. What has no switch, and why

| Change | Why no switch |
|---|---|
| The protocol version is 19, was 14 | the wire; the transactions need 19 and the ack receipt 17; the code does not use what a broker of a lower version lacks |
| `RedeliverUnacknowledgedMessages` with ids on an Exclusive or Failover subscription redelivers all unacknowledged messages | the broker only redelivers individual messages for Shared and KeyShared |
| `RedeliverUnacknowledgedMessages` sends the ids without the batch index, once per entry; drops the received and not delivered messages; gives the permits back | it fixes duplicates and lost permits; the previous behaviour was wrong |
| A message that cannot be decrypted is not delivered | already an option: `ConsumerCryptoFailureAction.Consume` delivers the ciphertext, as before (the CHANGELOG line says so) |
| The state of a partitioned consumer is `Active` only when every partition is; the state is published when the channel is in place; a partitioned producer reports `PartiallyConnected` | it fixes states that were wrong |
| Disposing waits at most `OperationTimeout` (30 s) for the close response | it only ends a hang; the time is the `OperationTimeout` of the client |
| A producer without a name sends the name that the broker generated, a fixed producer id and a growing epoch | the previous behaviour stored a message twice after a reconnect |

The requester can overrule any of them (decision 0010, rules 3 and 4).

## 3. Tests (22 new cases: 20 unit, 2 integration)

| File | Cases |
|---|---|
| `Internal/SubProducerTests` | `NotAllowedSendError` and a message that is too large (2 rows) with `failOnlyTheRejectedMessage: false`: the producer faults (`ExecutorFaulted`), and the message is not failed on its own |
| `Internal/SubConsumerTests` | an empty list with `redeliverNothingForNoMessageIds: false` redelivers everything (4 subscription types); a non-empty list is unchanged (1) |
| `Internal/SeekBatchIndexTests` | `ToSeekMessageIdData(false)` has no ack set; `SubConsumer` and `SubReader` seek without the ack set |
| `Internal/Chunking/ConsumerChannelChunkingTests` | with the assembler disabled every chunk is delivered as a message, with its own id and no first chunk id |
| `Internal/ConsumerBuilderTests`, `ReaderBuilderTests`, `ProducerBuilderTests` | defaults are `true` (the options and the `Default...` constants), `false` is passed on in the options, the builder methods return the builder (8) |
| `MessageChunkingTests` (integration) | a chunked message of 12 MB of another client is delivered as 3 messages with `AssembleChunkedMessages(false)` |
| `Prerequisites/ConsumerSeekBatchIndexAckTests` (integration) | with `SeekToBatchIndex(false)` a seek to message 3 of a batch of 5 replays all 5 messages, on a broker that has `acknowledgmentAtBatchIndexLevelEnabled` |

Test helpers that got an optional parameter: `CreateSubProducer(... failOnlyTheRejectedMessage)`, `CreateSubConsumer(... redeliverNothingForNoMessageIds)`, `CreateSubConsumer` and
`CreateSubReader` of `SeekBatchIndexTests` (`seekToBatchIndex`), `ChunkTestData.CreateAssembler(... enabled)`, `ConsumerChannelChunkingTests.CreateUut(... assembleChunkedMessages)`,
and `ConsumerSeekBatchIndexAckTests.CreateConsumer(... seekToBatchIndex)`. Not covered against a real broker: `FailOnlyTheRejectedMessage` and `RedeliverNothingForNoMessageIds` (the rejected
send needs a broker that refuses a send, and the empty redelivery is a command; both are covered by the unit tests of the sub producer and the sub consumer).

## 4. Files of the commit (`git show --stat d462dae8`)

Source (`src/`):

- `src/DotPulsar/ConsumerOptions.cs` (changed)
- `src/DotPulsar/Extensions/ConsumerBuilderExtensions.cs` (changed)
- `src/DotPulsar/Extensions/ProducerBuilderExtensions.cs` (changed)
- `src/DotPulsar/Extensions/ReaderBuilderExtensions.cs` (changed)
- `src/DotPulsar/Internal/Chunking/ChunkedMessageAssembler.cs` (changed)
- `src/DotPulsar/Internal/Consumer.cs` (changed)
- `src/DotPulsar/Internal/ConsumerBuilder.cs` (changed)
- `src/DotPulsar/Internal/ConsumerChannel.cs` (changed)
- `src/DotPulsar/Internal/Extensions/MessageIdDataExtensions.cs` (changed)
- `src/DotPulsar/Internal/Producer.cs` (changed)
- `src/DotPulsar/Internal/ProducerBuilder.cs` (changed)
- `src/DotPulsar/Internal/Reader.cs` (changed)
- `src/DotPulsar/Internal/ReaderBuilder.cs` (changed)
- `src/DotPulsar/Internal/SubConsumer.cs` (changed)
- `src/DotPulsar/Internal/SubProducer.cs` (changed)
- `src/DotPulsar/Internal/SubReader.cs` (changed)
- `src/DotPulsar/ProducerOptions.cs` (changed)
- `src/DotPulsar/ReaderOptions.cs` (changed)

Tests (`tests/`):

- `tests/DotPulsar.Tests/Internal/Chunking/ChunkTestData.cs` (changed)
- `tests/DotPulsar.Tests/Internal/Chunking/ConsumerChannelChunkingTests.cs` (changed)
- `tests/DotPulsar.Tests/Internal/ConsumerBuilderTests.cs` (changed)
- `tests/DotPulsar.Tests/Internal/ProducerBuilderTests.cs` (changed)
- `tests/DotPulsar.Tests/Internal/ReaderBuilderTests.cs` (changed)
- `tests/DotPulsar.Tests/Internal/SeekBatchIndexTests.cs` (changed)
- `tests/DotPulsar.Tests/Internal/SubConsumerTests.cs` (changed)
- `tests/DotPulsar.Tests/Internal/SubProducerTests.cs` (changed)
- `tests/DotPulsar.Tests/MessageChunkingTests.cs` (changed)
- `tests/DotPulsar.Tests/Prerequisites/ConsumerSeekBatchIndexAckTests.cs` (changed)

Documentation (repository): `CHANGELOG.md`, `docs/api/index.md`.

## 5. Recipe for another branch

1. `git cherry-pick d462dae8` if the branch has WP12 (`427df7dd`). Conflicts to expect: the three options classes, the three builders and their extension classes
   (keep both sides: each option is an independent block of 3 + 4 + 1 places), `SubProducer`, `SubConsumer`, `SubReader` (constructor parameters: keep the
   parameters of the other branch and add these at the end), `ConsumerChannel`, `MessageIdDataExtensions`.
2. By hand, for **each option**, the places of section 1; the pattern to copy is `AutoAckOldestChunkedMessageOnQueueFull` (consumer and reader) and `EnableChunking` (producer):
   options class: `Default...` constant with XML documentation, the initialisation in the constructor, the property with XML documentation;
   builder class: field, initialisation from the `Default...` constant, public method, the line in `Create()`; extension class: the extension method with the same
   documentation, `=> ExtendedApi.Require<TheBuilder<TMessage>>(builder, nameof(builder), nameof(Method)).Method(value)`.
3. Every behaviour change that the other branch made after `fc04185d` needs the same: apply decision 0010, rule 1 (a switch in the same commit) or rule 3 / 4 (documented).
4. Do not add a member to a public interface for any of it (decision 0009); `PublicApiTests` fails if you do.

## 6. Verification

Run on 2026-11-01 on the commit `d462dae8` (the tip: WP12 and WP13). The build has only the 4 warnings that `master` has.

| Run | Result |
|---|---|
| Unit, Release, 3 of 3 runs | 2397 passed, 0 failed, none aborted (2377 + the 20 new cases) |
| Unit, Debug | 2386 passed, 11 skipped (the allocation tests, which are measured in an optimised build only), 0 failed |
| Package validation against `master` (both packed as 1.0.0, `PackageValidationBaselinePath`) | 42 findings, all in the `DotPulsar.Internal` namespace (27 CP0002, 15 CP0006), none outside it; strict mode: 185 additions outside `Internal` (161 members, 24 types), nothing changed or removed |
| Integration, Release (`Category=Integration`) | 571 tests (569 + the 2 new): **557 passed, 14 skipped, 0 failed**, none aborted, 44 min 15 s. The 14 skips are the known ones (11 by design in the collection without batch index acknowledgment, 2 rows of `ConsumerTests` for apache/pulsar#27128, 1 row for apache/pulsar#27082) |
| Integration, Debug | **not finished, and not green**: see below |
| Stress of the unit suite (8 rounds of 4 processes under CPU load and 12 busy loops), on a copy of the Release build of the tip | **partial: 19 of 19 completed runs passed with all 2397 tests, none aborted** (rounds 1 to 4, and 3 runs of round 5); **13 of the 32 planned runs were not completed**: the verification script went on to the stress by itself when the Debug run was stopped, and the stress was stopped by hand in round 5 because the machine was low on memory. The last complete stress, 32 of 32 runs with all 2340 tests, was on `fc04185d`, before WP12 and WP13 |

**The Debug integration run.** It started at 18:30 and was cut off at about 19:02 (32 minutes in), when the background shells of the session were stopped
by the harness because the machine was critically low on memory; the test processes were left running without a reader and produced no output for 19 minutes, so
they were stopped by hand. No summary was written. It recorded **3 failures**, all of them an error that the broker returned, none an assertion of the test:

| Test | Error from the broker |
|---|---|
| `PendingAckPersistentTests.AcknowledgeCumulative_GivenCommittedAndAbortedTransactions_ShouldReportPendingAckMetrics` (16:48:34 UTC) | `SchemaException: Error while recovering ledger - ledger=614 - operation=Failed to open ledger` while the producer got the schema version |
| `AckTimeoutTests.Receive_GivenAckTimeoutRedeliveryBackoff_ShouldRedeliverWhenTheAckTimeoutAndTheDelayHavePassed` (Exclusive; 16:51:32 UTC) | `Error while using MetaStore - ledger=-1 - operation=Failed to create ledger` at the subscribe |
| `ConsumerTests.Acknowledge_GivenAckReceiptEnabled_ShouldCompleteAndAdvanceSubscription` (17:02:25 UTC) | `SchemaException: Error while recovering ledger - ledger=613 - operation=Failed to open ledger` while the producer got the schema version |

All three tests passed in the Release run of the same tip, and in the Debug run of `fc04185d` (569 tests, 555 passed, 0 failed). All three failed at the broker
(its ledger storage could not open or create a ledger), which makes a problem of the machine likely (the machine was low on memory, and other sessions were
running brokers and builds at the same time), **but that is not established**: the three tests were not run again, and the Debug run was not completed.
The error of each is a response of the broker to a schema version request or to a subscribe, not an assertion of the test.

**To finish the verification** (when the machine has memory): run the three tests again in Debug on a fresh broker
(`dotnet test tests/DotPulsar.Tests -c Debug --filter "FullyQualifiedName~PendingAckPersistentTests|FullyQualifiedName~AckTimeoutTests|FullyQualifiedName~ConsumerTests"`),
then the whole Debug integration run, then the rest of the stress of the unit suite on the tip (`stress2.sh`, 8 rounds of 4; 19 of the 32 runs are done). Record the numbers here.
