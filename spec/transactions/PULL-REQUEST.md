<!-- Draft of the pull request description of the branch feature/transactions. Nothing has been pushed or opened. Replace the issue number before it is used. -->

Fixes #{issue number}

# Description

Adds transaction support to DotPulsar, together with the client features that the upstream transaction tests depend on. Everything is off by default, so existing users see no change, except for the behaviour changes that are listed below.

**Transactions:** `IPulsarClient.NewTransaction()` / `CreateTransaction()`, `IPulsarClientBuilder.EnableTransaction` and `OperationTimeout`, sending messages in a transaction (`IMessageBuilder.Transaction`, `MessageMetadata.Transaction`), acknowledging messages in a transaction (single and cumulative, with and without batch index acknowledgment), commit and abort, the transaction coordinator client with discovery and reconnect, and the new exceptions.

**Client features that they need:** protocol version 19, acknowledgment receipts, batch index acknowledgment, producer batching with `Flush`, send timeout, ack timeout, negative acknowledgment, dead letter topic, a redelivery primitive, chunking (producer and consumer), end-to-end encryption (producer, consumer and reader; RSA and ECDSA keys).

README, `docs/api/index.md` and the CHANGELOG (`[Unreleased]`) are updated, and `samples/Transactions` is new. The feature matrix of the Pulsar clients on the Pulsar site still has to be updated by the maintainers.

**Production bugs on master that the work found and fixed** (each has its own commit, and can be taken out and sent on its own):
- a consumer of a partitioned topic, of several topics or of a pattern was `Active` when its first partition was (`Report Active when every partition is subscribed`)
- a consumer or reader published `Active` / `Connected` before it had its channel, so a `Receive` right after could wait the 3 s retry (`Publish the state when the channel is in place`)
- the producer never put the schema version in its messages, so `IMessage.SchemaVersion` was null
- `RedeliverUnacknowledgedMessages` delivered some messages twice, and (partitioned or multi-topic consumers) one that was already received besides the redelivered ones
- disposing a producer, consumer or reader waited for the close response without a limit, 91 s with a dead connection that was not detected yet (`Bound the wait for the close response on dispose`)
- a batch could be returned to the pool while the channel still wrote it (`Do not return a batch while it is written`)
- the state of a partitioned producer was a transient `WaitingForExclusive` (`Start the partition states as Disconnected`)
- `MessageProcessorTests` hang in Release (`Do not spin in the MessageProcessor tests`, a test-only commit that can be sent upstream on its own)

**Behaviour changes that users can see:** `ConsumerOptions.CryptoFailureAction` defaults to `Fail`, so a consumer without a key reader no longer receives an encrypted message as ciphertext (documented in the CHANGELOG); `EnableBatching` is `false` by default (the wire and `BatchIndex == -1` are unchanged); a message that is larger than the maximum message size now fails only its own send with `TooLargeMessageException`; encryption keys without a key reader are a `ConfigurationException` (Java sends the messages unencrypted).

**Deviations from the specification:** the list is in `spec/transactions/implementation-notes/deviations-summary.md`. Two of them matter most:
- *Batching throughput* (accepted by the requester). The specification asserts 3x the throughput of unbatched sends on the fake connection. Measured: 1.55x, and 3.3x less allocation per message (1.22 KB against 4.09 KB). The part of the send path that batching does not remove (the send operation, its task and its receipt, the queue) is about 1.3 us of the 2.0 us of an unbatched send, so 3x needs that part to become about 3.5x cheaper; it is the same code as on master. See `07-performance.md` 3.1.
- *Deduplication after a reconnect* works for producers with a name; the generated name of a producer without a name is not sent again (pre-existing, not changed).

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

- The unit suite: 2155 passed and 11 skipped in Debug, 2166 passed in Release (nothing excluded), 3 of 3 runs. Under CPU load (4 parallel suites and 12 busy loops) 59 of 60 runs passed in each of the last two passes; the failures of the stress passes were each a different test that depends on a short real-time window, and were fixed one by one (`followups/0009`).
- The integration tests against `apachepulsar/pulsar:4.0.9` (Docker, up to 2 brokers at a time, see `BrokerResources`): 521 tests, 507 passed, 14 skipped, 0 failed, in Debug and in Release. The Transactions tests (`FullyQualifiedName~DotPulsar.Tests.Transactions`) ran 20 iterations in a row without a failure, and 10 more afterwards.
- 331 upstream tests are mapped (`upstream-map.yaml`): 253 ported and 78 excluded with a reason. Every test that this work added says which upstream test it ports (`// Upstream:`) or `// Additional coverage`.
- `samples/Transactions` was run against a broker with `transactionCoordinatorEnabled=true`: 5 messages in, 10 out, 5 committed transactions, none aborted.
- External items for the maintainers: the EC encryption tests have not been run on macOS (there was no Mac; they are skipped there).
- The 14 skipped integration tests (the two rows of `ConsumerTests` are an external item for the maintainers: the client side is covered by unit tests, and the rows run when the test image has the broker fix): 11 rows of the collection without batch index acknowledgment (two tests are disabled upstream with `enabled = false`, and a third turns batch index acknowledgment on at runtime upstream, so the same scenario runs in the other collection), 2 rows of `ConsumerTests` that need apache/pulsar#27128 (not in a published image yet; the newest is 5.0.1 from 2026-10-26), and 1 row that needs apache/pulsar#27082.

**Known gaps:** everything that the work does not do, does differently or could not verify is listed in `spec/transactions/followups/0019-known-gaps-at-the-end-of-wp10.md` (not part of the branch).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
