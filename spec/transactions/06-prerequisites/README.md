# 06 — Prerequisite features

These client features are missing from DotPulsar, and the upstream transaction tests depend on them
([ADR 0005](../decisions/0005-port-prerequisite-features.md)). Each file below is a self-contained work-package spec.
Every spec contains:

- an upstream mapping (Java primary, C++ cross-reference, at the pinned SHAs);
- the public API with XML docs;
- the internal design and exact DotPulsar edit points;
- performance notes;
- tests, including ports of upstream's own tests for that feature;
- acceptance criteria;
- open decisions.

## Mapping to the work-package order in [10-implementation-plan](../10-implementation-plan.md)

The drafts use their own working labels (`WP-PREREQ-n`). The table maps them to the WPs that are authoritative.

| Spec | Draft label | WP in plan | Unblocks |
|---|---|---|---|
| [ack-receipt.md](ack-receipt.md) | WP-PREREQ-3 | **WP1** | every transactional ack; `isAckReceiptEnabled` tests |
| [batch-index-ack.md](batch-index-ack.md) | WP-PREREQ-2 | **WP5a** | transactional acks of batch messages; `testIndividualAckAbortFilterAckSetInPendingAckState`, … |
| [producer-batching.md](producer-batching.md) | WP-PREREQ-1 | **WP6** | every batch=true variant; `BatchMessageContainerImplTest`; `IProducer.Flush` |
| [send-timeout.md](send-timeout.md) | WP-PREREQ-4 | **WP7** | `testSendTxnMessageTimeout`; `TransactionHasOperationFailedException` paths |
| [ack-timeout.md](ack-timeout.md) (includes the **redelivery primitive**) | — | **WP7a**: redelivery primitive, as its own PR **before** WP7; **WP7**: ack timeout itself | `txnCumulativeAckTest` (redelivery), ack-timeout tests |
| [dead-letter-policy.md](dead-letter-policy.md) | — | **WP7** | `testSendTxnAckMessageToDLQ`, `testSendTxnAckBatchMessageToDLQ` |
| [chunking.md](chunking.md) | — | **WP8** | `TransactionConsumeTest#testAckChunkMessage` |
| [encryption.md](encryption.md) | WP-PREREQ-ENCRYPTION | **WP9** | `TransactionTest#testEncryptionRequired` |

## Decisions that override text inside the individual specs

Where a spec in this folder conflicts with the list below, the list wins. The core specs it refers to are
[05-components](../05-components/) and [03-public-api](../03-public-api.md).

1. **Shared ack-with-response plumbing.** The core and prerequisite agents agreed this design during drafting. The
   connection-level parts are specified in [connection-integration](../05-components/connection-integration.md) and
   the consumer-level parts in [consumer-integration](../05-components/consumer-integration.md).
   - `IConnection.SendWithResponse(CommandAck, ct)`.
   - `RequestResponseHandler.OutgoingWithResponse`. Ack requests are keyed by **request id only**, so a broker
     `CloseConsumer` does not cancel them (`AckReceiptOnTopicUnloadTest`).
   - `IConsumerChannel.SendTransactional`. `batch-index-ack.md` §5.5 fills `ack_set` / `batch_size`.
   - Ack-response errors are evaluated **outside** the consumer `Executor`.
   - `CommandExtensions.ToException(ServerError, string)` is a non-throwing mapping. `Throw` delegates to it.
2. **Ack-receipt timeout.** An ack awaiting a receipt is bounded by `IPulsarClientBuilder.OperationTimeout`
   (default 30 s), as in Java (`ClientCnx#newAckForReceipt` → `sendRequestAndHandleTimeout`). On timeout it fails
   with `OperationTimeoutException`. This replaces the "no client-wide timeout" note in `ack-receipt.md`.
3. **Defaults.**

   | Option | DotPulsar default | Java default |
   |---|---|---|
   | `EnableBatching` | **false** | true |
   | `BatchIndexAckEnabled` | **false** | true at the pinned SHA, `ConsumerConfigurationData` L417 |
   | `AckReceiptEnabled` | false | |
   | `SendTimeout` | `TimeSpan.Zero` (disabled) | 30 s |
   | `AckTimeout` | `TimeSpan.Zero` (disabled) | |
   | `EnableChunking` | false | |

   Ported tests set these options explicitly wherever upstream relies on Java's defaults. Transactional acks always
   send `ack_set` for batch messages, whatever `BatchIndexAckEnabled` is set to.
4. **Option names are fixed.** Test specs must use exactly these names:

   | Builder | Options |
   |---|---|
   | `IProducerBuilder<T>` | `EnableBatching`, `BatchingMaxMessages`, `BatchingMaxPublishDelay`, `BatchingMaxBytes`, `BatcherType` (`enum BatcherType : byte { Default, KeyBased }`), `SendTimeout`, `EnableChunking`, `ChunkMaxMessageSize`, `CryptoKeyReader`, `EncryptionKey`, `CryptoFailureAction` |
   | `IConsumerBuilder<T>` | `AckReceiptEnabled`, `BatchIndexAckEnabled`, `AckTimeout`, `AckTimeoutTickTime`, `DeadLetterPolicy`, `NegativeAcknowledgeRedeliveryDelay` |
   | `IProducer` | `Flush(CancellationToken)` |

   New builder members are **appended** to the interfaces ([01 §7](../01-conventions.md#7-options-and-builders)).
5. **Transform order** for chunking and encryption follows upstream:
   - producer: compress → chunk → encrypt each chunk;
   - consumer: decrypt each chunk → reassemble → decompress.

   Chunk sizing subtracts the 16-byte GCM tag.
6. **Behaviour changes to existing public API** (CHANGELOG `### Changed`):
   - `RedeliverUnacknowledgedMessages` with an **empty** list becomes a no-op.
   - Per-id redelivery on Exclusive/Failover subscriptions redelivers all messages, as in Java.

   Both are specified in `ack-timeout.md` §6 and ship with WP7a.
7. **Pre-existing bugs fixed along the way.** Each one is listed in its spec, with a regression test:
   - a cumulative ack of a partially acked batch is dropped;
   - batch trackers leak;
   - batch messages carry an empty-topic `MessageId`;
   - the ordering key is taken from the batch rather than the single-message metadata;
   - `CommandSend.NumMessages` is never reset;
   - the pooled ack command keeps the broker's `MessageIdData` instance.

   These fixes ship in the WP that touches the code. CHANGELOG `### Fixed` gets one line per bug.

## As built

These files are the design drafts the work started from. The implementation notes in [../implementation-notes](../implementation-notes/README.md) say what was built and where it differs; where a note and a draft disagree, the note is authoritative. The consolidated list of deviations is [../implementation-notes/deviations-summary.md](../implementation-notes/deviations-summary.md); `upstream-map.yaml` is the final test-parity record.
