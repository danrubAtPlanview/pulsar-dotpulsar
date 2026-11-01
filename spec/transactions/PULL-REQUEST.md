<!-- Draft of the pull request description of the branch feature/transactions. The branch is on the fork danrubAtPlanview/pulsar-dotpulsar (pushed on 2026-11-01, tip f96b0a03); no pull request is opened. The issue is apache/pulsar-dotpulsar#246 ("Add support for the Pulsar Transactions API", open, found on 2026-11-01); check that it is the one to close before it is used. -->

Fixes #246

# Description

Adds transaction support to DotPulsar, together with the client features that the upstream transaction tests depend on. Everything is off by default, so existing users see no change, except for the behaviour changes that are listed below.

**Transactions:** `IPulsarClient.NewTransaction()` / `CreateTransaction()`, `IPulsarClientBuilder.EnableTransaction` and `OperationTimeout`, sending messages in a transaction (`IMessageBuilder.Transaction`, `MessageMetadata.Transaction`), acknowledging messages in a transaction (single and cumulative, with and without batch index acknowledgment), commit and abort, the transaction coordinator client with discovery and reconnect, and the new exceptions.

**Client features that they need:** protocol version 19, acknowledgment receipts, batch index acknowledgment (and seek to a message in a batch), producer batching with `Flush`, send timeout, ack timeout and negative acknowledgment (both with an optional redelivery backoff: `IRedeliveryBackoff`, `MultiplierRedeliveryBackoff`), dead letter topic, a redelivery primitive, chunking (producer and consumer), producers that keep their id and epoch across reconnects (deduplication after a reconnect), end-to-end encryption (producer, consumer and reader; RSA and ECDSA keys).

README, `docs/api/index.md` and the CHANGELOG (`[Unreleased]`) are updated, and `samples/Transactions` is new. The feature matrix of the Pulsar clients on the Pulsar site still has to be updated by the maintainers.

**Production bugs on master that the work found and fixed** (each has its own commit, and can be taken out and sent on its own):
- a consumer of a partitioned topic, of several topics or of a pattern was `Active` when its first partition was (`Report Active when every partition is subscribed`)
- a consumer or reader published `Active` / `Connected` before it had its channel, so a `Receive` right after could wait the 3 s retry (`Publish the state when the channel is in place`)
- the producer never put the schema version in its messages, so `IMessage.SchemaVersion` was null
- `RedeliverUnacknowledgedMessages` delivered some messages twice, and (partitioned or multi-topic consumers) one that was already received besides the redelivered ones
- disposing a producer, consumer or reader waited for the close response without a limit, 91 s with a dead connection that was not detected yet (`Bound the wait for the close response on dispose`)
- a batch could be returned to the pool while the channel still wrote it (`Do not return a batch while it is written`)
- a producer without a name was not deduplicated after a reconnect: it had another id on the new connection and did not send the name that the broker had generated, so the messages that were sent again were stored twice
- the state of a partitioned producer was a transient `WaitingForExclusive` (`Start the partition states as Disconnected`)
- `MessageProcessorTests` hang in Release (`Do not spin in the MessageProcessor tests`, a test-only commit that can be sent upstream on its own)

**Additions to public interfaces that implementers of them must follow:** `IConsumer` gets the `Acknowledge` (two) and `AcknowledgeCumulative` overloads that take an `ITransaction`, and `NegativeAcknowledge(MessageId, CancellationToken)` and `NegativeAcknowledge(MessageId, uint redeliveryCount, CancellationToken)` (the backoff needs the redelivery count of the message; the `NegativeAcknowledge(IMessage)` extension passes it).

**Behaviour changes that users can see:** `ConsumerOptions.CryptoFailureAction` defaults to `Fail`, so a consumer without a key reader no longer receives an encrypted message as ciphertext (documented in the CHANGELOG); `EnableBatching` is `false` by default (the wire and `BatchIndex == -1` are unchanged); a message that is larger than the maximum message size now fails only its own send with `TooLargeMessageException`; encryption keys without a key reader are a `ConfigurationException` (Java sends the messages unencrypted); a producer without a name sends `UserProvidedProducerName = false` and an epoch on every connect, and the name that the broker generated from the second connect on, and its id comes from a process-wide allocator; a seek to a received message that is in a batch replays the batch from that message on a broker with `acknowledgmentAtBatchIndexLevelEnabled` (the whole batch on a broker without it, and for a message id that has no batch size, as before).

**Deviations from the specification:** the list is in `spec/transactions/implementation-notes/deviations-summary.md`. Two of them matter most:
- *Batching throughput* (accepted by the requester). The specification asserts 3x the throughput of unbatched sends on the fake connection. Measured: 1.55x, and 3.3x less allocation per message (1.22 KB against 4.09 KB). The part of the send path that batching does not remove (the send operation, its task and its receipt, the queue) is about 1.3 us of the 2.0 us of an unbatched send, so 3x needs that part to become about 3.5x cheaper; it is the same code as on master. See `07-performance.md` 3.1.
- *Deduplication after a reconnect* works for producers with and without a name. A reconnect while the broker still holds the old connection is registered after the liveness check of the broker: 8 s after the cut on 4.0.9 (3 s retry and 5 s), and after the keep-alive (30 to 60 s) on a broker without that check. Java behaves the same. The epoch is not sent for producers with a name (Java sends it), and the path of the broker that replaces a producer of the same connection by its successor is covered by unit tests only.

# Regression

No regression is known. The pooled commands of the ack path were measured against master: a plain acknowledgment is 104 B and 84 ns (master 104 B, 104-115 ns), a plain send 2.88 KB (master 2.87 KB). The Release and Debug builds have the four warnings that master has (NU1608 and three xUnit1031 in the untouched `AwaiterTests`).

| In-process benchmark, Release, .NET 10 | Result |
|---|---|
| `TxnAck_SteadyState` over `PlainAck_Receipt` | +8 B |
| `TxnSend_SteadyState` over `PlainSend` | +0 B |
| `EncryptedSend_100Bytes` over `PlainSend_100Bytes` | +229 B |
| `Send_Batching_1000` / `Send_NoBatching_1000` (per message) | 1.29 us / 2.00 us, 1.22 KB / 4.09 KB |
| `CreateCommit_Empty` | 1.09 KB |

# Testing

- The unit suite: 2295 passed and 11 skipped in Debug, 2306 passed in Release (nothing excluded), 3 of 3 runs. Under CPU load (4 parallel suites and 12 busy loops) the stress passes of the earlier work packages each found one failing run in about 60, each a different test that depends on a short real-time window, and they were fixed one by one. The pass after the last work package found a hang in a test helper that this branch added (`LossyRelay`: 21 of 56 runs aborted by the hang detector); it is fixed in the branch, and 60 of 60 runs then passed with all 2290 tests; after the last feature (the negative-ack redelivery backoff) 48 of 48 runs passed with all 2306 tests, none aborted (`followups/0009`). The list of such tests is not proven to be finished.
- The integration tests against `apachepulsar/pulsar:4.0.9` (Docker, up to 2 brokers at a time, see `BrokerResources`): 569 tests, 555 passed, 14 skipped, 0 failed, none aborted, both in Debug and in Release, on the final tree. The Transactions tests (`FullyQualifiedName~DotPulsar.Tests.Transactions`) ran 30 iterations in a row without a failure, and 5 more before the last feature.
- 335 upstream tests are mapped (`upstream-map.yaml`): 259 ported (92 of them partially) and 76 excluded with a reason. Every test that this work added says which upstream test it ports (`// Upstream:`) or `// Additional coverage`.
- `samples/Transactions` was run against a broker with `transactionCoordinatorEnabled=true`: 5 messages in, 10 out, 5 committed transactions, none aborted.
- External items for the maintainers: the EC encryption tests have not been run on macOS (there was no Mac; they are skipped there).
- The 14 skipped integration tests (the two rows of `ConsumerTests` are an external item for the maintainers: the client side is covered by unit tests, and the rows run when the test image has the broker fix): 11 rows of the collection without batch index acknowledgment (two tests are disabled upstream with `enabled = false`, and a third turns batch index acknowledgment on at runtime upstream, so the same scenario runs in the other collection), 2 rows of `ConsumerTests` that need apache/pulsar#27128 (not in a published image yet; the newest is 5.0.1 from 2026-10-26), and 1 row that needs apache/pulsar#27082.

**Known gaps:** everything that the work does not do, does differently or could not verify is listed in `spec/transactions/followups/0019-known-gaps-at-the-end-of-wp10.md` (not part of the branch).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
