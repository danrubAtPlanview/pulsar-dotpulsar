# Deviations from the specification, as built (summary)

The specification files (`03`, `05-components/`, `06-prerequisites/`, `08-tests/`) are the design drafts that the implementation started from. Where they differ from the code, **the implementation note of the work package is authoritative** (each note has its own deviations list; this file collects the ones that a reviewer is most likely to trip over). `upstream-map.yaml` is final: every test row is `done: true` or `Excluded` with a reason, and every `csharp:` reference was checked against the code.

## Public API and behaviour

| Area | Spec says | As built | Note |
|---|---|---|---|
| Send timeout exception | `SendTimeoutException` | `OperationTimeoutException` (sequence id in the message); `SendTimeout` default zero (off) | wp6 |
| Batching default | Java default `true` | `EnableBatching` is `false` by default (wire and `BatchIndex == -1` unchanged for existing users) | wp6, ADR in `docs/decisions` |
| Batching with chunking | - | `ConfigurationException("Batching and chunking of messages can't be enabled together")` (the Java text); chunking is switched off for `non-persistent://` topics | wp8-producer |
| Consumer decryption default | ignore encryption metadata | default `CryptoFailureAction` is `Fail` (a behaviour change: the consumer no longer delivers an undecryptable payload; `Consume` restores the old behaviour); documented in the CHANGELOG | wp9-consumer, followup 0010 |
| Undecryptable chunk with `Consume` | spec 5.3 delivers it | treated like `Fail` (each encrypted chunk has its own IV and is larger than its share of `total_chunk_msg_size`, so it cannot be assembled) | wp9-consumer |
| Producer encryption keys without a key reader | Java parity (send unencrypted), decision D2 | `ConfigurationException` (silent clear text when the user asked for encryption is a hazard) | wp9-producer, followup 0013 |
| Producer crypto scope | one `MessageCrypto` per `Producer` | one per sub-producer (same as the consumer); the key reader is called once per partition | wp9-producer |
| Reader and undecryptable message | - | with `Fail` the message is skipped (a reader cannot redeliver); documented on `ReaderOptions` | wp9-consumer |
| Negative-ack tracker | Java re-delivers an already pending entry twice | a nack for an entry that is pending keeps its first deadline; the delay is clamped to at least 100 ms | wp7 core |
| DLQ message key | Java copies the key | an empty key is not copied (a received message without key has `Key == ""`) | wp7 core |
| DLQ of an encrypted message | - | the dead letter producer has no encryption, so the dead letter topic receives the decrypted payload (same as Java); accepted, documented on `DeadLetterPolicy` | followup 0010 item 1, 0013 item 5 |
| Ack timeout redelivery backoff | `ackTimeoutRedeliveryBackoff` rows | built in WP11-B (`IRedeliveryBackoff`, `MultiplierRedeliveryBackoff`, `ConsumerOptions.AckTimeoutRedeliveryBackoff`); one tracker per sub consumer instead of the multi-topic tracker of Java (2 `UnAckedTopicMessageRedeliveryTrackerTest` rows Excluded); the redelivery count is unsigned | `wp11-deferred-features.md` |
| Seek to a message in a batch | - (OD 3: deferred) | built in WP11-C: the seek sends the `ack_set` of Java, so a broker with `acknowledgmentAtBatchIndexLevelEnabled` replays the batch from the message; a message id without a batch size replays the whole batch; no client-side filter of the messages before the index (Java has one) | `wp11-deferred-features.md` |
| Negative-ack redelivery backoff | `negativeAckRedeliveryBackoff` (OD-3) | not built; `IRedeliveryBackoff` exists now and can be reused; `NegativeAcksTest#testNegativeAcksWithBackoff` is Excluded | followup 0019 |
| Transactional ack of a batch index | untrack the entry | does not untrack (Java parity); spec OD-2 not applied | wp7a |
| Multi-topic cumulative ack with a transaction | Java drops the transaction | `Consumer` routes it with the transaction | wp5 |
| Empty `Acknowledge(IEnumerable<MessageId>, txn)` | Java sends an empty ack | no-op | wp5 |
| `RedeliverUnacknowledgedMessages` on a partitioned or multi-topic consumer | - | also drops the message that a sub consumer already returned to the `Consumer` (production fix, WP10) | wp10-parity-completion |
| Producer schema version | set in every message | now set (production fix, WP10; before, `IMessage.SchemaVersion` was null for DotPulsar-produced messages) | followup 0005 item 1 |
| State of a consumer or a reader when the channel is created | - | `Active` / `Connected` is published when the channel is in place, not when the broker has answered (production fix, WP10; before, a `Receive` right after the state change could wait the 3 s retry interval) | followup 0016 |
| Dispose of a producer, consumer or reader | - | waits for the close response as long as the operation timeout (30 s by default), then goes on (production fix, WP10; before, up to 91 s with a dead connection that was not detected yet) | followup 0014 |
| State of a partitioned producer or reader while some partitions are not connected | - | `PartiallyConnected` instead of a transient `WaitingForExclusive` on a producer (production fix, WP10, cosmetic) | followup 0017 |
| Batching throughput | batching at least 3x the throughput of unbatched sends on the fake connection (`07-performance.md`) | 1.55x throughput and 3.3x less allocation per message; the per-message part of the send path is the same with and without batching. **Accepted by the requester (2026-11-01)** | `07-performance.md` 3.1, followup 0007 |
| Deduplication after a reconnect | each message exactly once for any producer (`06-prerequisites/producer-batching.md` criterion 7) | met (WP11-A): a producer has the same id on every connection, a growing epoch, and a producer without a name sends `UserProvidedProducerName = false` and its generated name again. A reconnect while the broker still holds the old connection is registered after the liveness check of the broker (8 s on 4.0.9: 3 s retry + 5 s), and after the keep-alive (30 to 60 s) on a broker without that check, like Java | followup 0012 item 1, `wp11-deferred-features.md` |
| State of a consumer of a partitioned topic, of several topics or of a pattern | - | `Active` only when every sub consumer is active (production fix, WP10; before, the first active partition made the consumer `Active`, because `Active` is the zero value of the enum). Until then `PartiallyConnected` or `Disconnected` | followup 0017 |

## Internal design

| Area | Spec says | As built | Note |
|---|---|---|---|
| `SendOp` | new fields for gate, transaction, chunk state | `RegistrationGate` is a `Task?`; no `CancellationToken` field (the caller's cancellation completes the receipt); the chunk state lives in `_gateOrGroup` as a group of one; `CreatedTimestamp` only when a timeout is set | wp4, wp6, wp8-producer |
| Registration of a transactional send | after `ChoosePartitions` | before `ChoosePartitions` (R-OP-SYNC holds before the producer is connected) | wp4 |
| Batching | in the producer at enqueue | in the dispatcher (`SubProducer`); containers are generic over `IBatchItem`; compression stays in `ProducerChannel.Send` | wp6a, wp6 |
| Chunk responses | not mentioned | every chunk shares `(producerId, sequenceId)`, so `RequestResponseHandler` keeps chunk sends in a FIFO per key; receipts and send errors complete the oldest pending chunk | wp8-producer, followup 0011 item 5 |
| Chunk size check | `NumChunksFromMsg <= 1` | `SendPackage.SkipSizeCheck` (Java never size-checks a chunking producer); `PlanChunking` is synchronous | wp8-producer |
| `MessageId` chunk info | two fields | one private chunk-info object; `ToEntryLevel` keeps the chunk ids and `ExpandChunks` is a separate step after the dead-letter step | wp8-consumer |
| Failure handling of encryption | in `MessageDispatcher` | in `SubProducer.AwaitSend` and `SendChunks`; initial wrap in the `Producer` constructor | wp9-producer |
| Receipts vs write (hazard) | - | fixed in WP10: a batch is not returned while the channel writes it (`PreparedBatch.BeginWrite/EndWrite`) | followup 0009 |

## Tests

- One small-limit broker (`maxMessageSize` 100) instead of the spec's four containers; the values 35 and 50 are covered with `ChunkMaxMessageSize(40)` (wp8-producer).
- The Java tests that read broker internals (Mockito spies, reflection on the broker service, `getAvailablePermits()` of the Java client) were ported through admin REST or unit tests, or are Excluded with a reason (`upstream-map.yaml`, `note:`).
- New fixtures (each with its own xUnit collection and broker): chunking (`maxMessageSize` 100), no batch index ack, pending-ack log index (`minLag` 1), pending-ack replay (`minLag` 4002), max active transactions (2), Toxiproxy, authenticated broker (token auth, roles).
- The brokers of a test run are limited to 2 at a time with a JVM of 1 GB (`BrokerResources`; `DOTPULSAR_TESTS_MAX_BROKERS`, `DOTPULSAR_TESTS_BROKER_MEMORY`), because nine collections need nine brokers and the image default of a 2 GB heap each does not fit in a Docker VM of 16 GB (wp10-parity-completion, final verification).
- The two transaction end-to-end test bases were folded into `TransactionEndToEndTestsBase<TFixture>` (wp10-test-ports note).
- Every test added by this work carries `// Upstream: Class#method` or `// Additional coverage` (parity rule 3).
- Skipped rows, all for documented broker bugs of apachepulsar/pulsar:4.0.9: `TopicTransactionBufferPublishOrderTests` row `[false]` (apache/pulsar#27082) and 2 rows of `ConsumerTests` (apache/pulsar#27128, followup 0002).

The gaps and open notes of the whole work, in one list: [`followups/0019-known-gaps-at-the-end-of-wp10.md`](../followups/0019-known-gaps-at-the-end-of-wp10.md).
