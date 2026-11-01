# 04 — Wire protocol

DotPulsar's `src/DotPulsar/Internal/PulsarApi.proto` already contains every v4 transaction message. No new messages
are needed. The only proto edit is a rename of `CommandNewTxn` field 2 (§3.1), which does not change the wire
format.

## 1. Client → broker commands added by this feature

| Command (`BaseCommand.Type`) | Sent by | Fields set (C# property names) | Response | Correlation |
|---|---|---|---|---|
| `TcClientConnectRequest` (62) | TC handler, on every (re)connect when `connection.ProtocolVersion >= 19` | `RequestId` (assigned by `RequestResponseHandler`), `TcId` | `TcClientConnectResponse` (63) or `Error` | `request_id` |
| `NewTxn` (50) | TC handler | `RequestId`, `TcId`, `TxnTtlMillis` (**milliseconds**, §3.1) | `NewTxnResponse` (51) | `request_id` |
| `AddPartitionToTxn` (52) | TC handler (via `Transaction.RegisterProducedTopic`) | `RequestId`, `TxnidMostBits`, `TxnidLeastBits`, `Partitions` = [partition topic] | `AddPartitionToTxnResponse` (53) | `request_id` |
| `AddSubscriptionToTxn` (54) | TC handler (via `Transaction.RegisterAckedTopic`) | `RequestId`, `TxnidMostBits`, `TxnidLeastBits`, `Subscription` = [{`Topic` = partition topic, `Subscription`}] | `AddSubscriptionToTxnResponse` (55) | `request_id` |
| `EndTxn` (56) | TC handler | `RequestId`, `TxnidMostBits`, `TxnidLeastBits`, `TxnAction` (`Commit` = 0 / `Abort` = 1) | `EndTxnResponse` (57) | `request_id` |
| `Send` (6) | producer, txn message | existing fields + `TxnidMostBits`, `TxnidLeastBits` (both, or neither: §3.2) | `SendReceipt` / `SendError` | `(producer_id, sequence_id)` (existing) |
| `MessageMetadata` (in `Send` payload) | producer, txn message | `TxnidMostBits` (23), `TxnidLeastBits` (22) | — | — |
| `Ack` (10) | consumer, txn ack | existing + `TxnidMostBits`, `TxnidLeastBits`, `RequestId`; per `MessageIdData`: `AckSet`, `BatchSize` for batch messages | `AckResponse` (38) | `request_id` |
| `Ack` (10) | consumer, plain ack with ack receipt enabled ([06/ack-receipt](06-prerequisites/ack-receipt.md)) | existing + `RequestId` | `AckResponse` (38) | `request_id` |
| `Producer` (5) | producer creation | existing + `TxnEnabled = true` when the client has transactions enabled (unset otherwise) | — | existing |

All TC commands are sent to the broker that owns
`persistent://pulsar/system/transaction_coordinator_assign-partition-{tcId}`. That connection comes from
`ConnectionPool.FindConnectionForTopic`. Every TC response carries a **required** `request_id`, so per-connection
correlation is enough. Java's routing of TC responses by `txnid_most_bits` is not ported (deviation X3,
[12](12-risks-open-questions.md)).

When the broker has transactions disabled, it answers a TC request with `CommandError` carrying the same
`request_id` (`ServerCnx#checkTransactionEnableAndSendError`). `response.Expect(type)` maps that error through
`CommandExtensions`.

Exact C# edits (registrations, `Outgoing`, `Send` overloads, `AsBaseCommand`) are in
[05/connection-integration](05-components/connection-integration.md).

## 2. Error mapping

[05/exceptions §4](05-components/exceptions.md#4-servererror--exception-mapping) has two tables:

- the client-side command mapping in `CommandExtensions`;
- the TC response mapping in `TransactionMetaStoreHandler`.

## 3. Field-level gotchas

### 3.1 `CommandNewTxn` TTL is in milliseconds

DotPulsar's proto declares `optional uint64 txn_ttl_seconds = 2`, but the value has always been milliseconds:

- Java sends `unit.toMillis(timeout)`.
- Upstream renamed the field `txn_ttl_millis` at L1162 of the pinned proto.

WP1 renames the DotPulsar field the same way. The field number is unchanged, so this is wire-compatible; see
[connection-integration §0.2](05-components/connection-integration.md). Callers write
`TxnTtlMillis = (ulong) timeout.TotalMilliseconds`.

### 3.2 Txn-id has-bits make a command transactional, even (0,0)

The broker checks `hasTxnidMostBits() && hasTxnidLeastBits()` on `CommandSend` (`ServerCnx#handleSend` L3122-3128)
and on `CommandAck` (`Consumer#messageAcked` L590-594, L622). It does not check the values.

Google.Protobuf proto2 sets a has-bit on **any** assignment, including 0. Any pooled `CommandSend`, `CommandAck` or
`MessageMetadata` that once carried a transaction must therefore have its fields cleared with `ClearTxnidMostBits()` and
`ClearTxnidLeastBits()`, or it will turn a later plain message into a txn `(0,0)` message. Hazard R-POOL in
[producer-integration](05-components/producer-integration.md).

### 3.3 `ack_set` encoding (batch-index ack)

`MessageIdData.ack_set` is a `repeated int64` bitset in `java.util.BitSet.toLongArray()` layout: bit *i* of word
*i / 64*, little-endian within each word. A **set** bit means "still unacked". For a transactional ack of index *k* in a
batch of size *n*:

| Ack type | Bits |
|---|---|
| Individual | set bits `[0, n)`, then clear bit *k* |
| Cumulative | set bits `[0, n)`, then clear bits `[0, k]` |

`batch_size` is set to *n*. Upstream: `ConsumerImpl#doTransactionAcknowledgeForResponse` L3527-3540. Full spec:
[06/batch-index-ack](06-prerequisites/batch-index-ack.md).

### 3.4 `CommandPartitionedTopicMetadata.metadata_auto_creation_allowed`

Java's TC discovery asks for metadata with auto-creation allowed (`AssignTopicTcDiscovery` L61). In proto2 the field
defaults to `true` when it is unset, so DotPulsar's existing `GetNumberOfPartitions` (field unset) behaves the same. No
change.

## 4. Protocol version bump 14 → 19 ([ADR 0004](decisions/0004-protocol-version-19.md))

Analysis was done against the pinned upstream master and the `v4.0.9` tag that the test image uses. The set of
version gates is identical in both.

### 4.1 How the broker uses the client version

- `ServerCnx#handleConnect` (L2064-2130) reads `connect.protocol_version`, then `completeConnect` stores the **raw**
  client value as the remote version.
- `Commands#newConnectedCommand` (L336-364) answers with `CommandConnected.protocol_version = min(server, client)`. For
  4.0.9 and master that is 19 after the bump. DotPulsar currently ignores this field. It starts recording it as
  `IConnection.ProtocolVersion` (§5).

### 4.2 Every broker behaviour gated on the client protocol version

| Gate | Where | v14 effect | v19 effect |
|---|---|---|---|
| ≥ v4 batch compatibility | `ServerCnx#isBatchMessageCompatibleVersion` (L5166-5169) | none (already ≥ 4) | none |
| ≥ v5 close producer/consumer by command | `ServerCnx#closeProducer` (L4829-4852), `#closeConsumer` | none | none |
| ≥ v9 ReachedEndOfTopic | `PulsarCommandSenderImpl#sendReachedEndOfTopic` (L209-216) | none | none |
| ≥ v11 checksum kept | `PulsarCommandSenderImpl#sendMessagesToConsumer` (L282-284) | none | none |
| ≥ v12 ActiveConsumerChange | `PulsarCommandSenderImpl#sendActiveConsumerChange` (L199-206) | none | none |
| v15 `peerSupportsGetOrCreateSchema` | `Commands` (L2654-2656) | client-side helper only. The broker does not check the version, which is why DotPulsar already uses GetOrCreateSchema at v14. | none |
| v16 `peerSupportsBrokerMetadata` | `Commands` (L2698-2700) | dead helper | none |
| v17 `peerSupportsAckReceipt` | `Commands` (L2658-2660) | client-side only. The broker sends `CommandAckResponse` whenever `ack.hasRequestId()`, regardless of version (`ServerCnx#handleAck` L3162-3224). | none |
| v18 broker entry metadata to client | `PulsarCommandSenderImpl#sendMessagesToConsumer` (L274-280) | stripped | **still stripped**. Sending it needs v18 **and** the `supports_broker_entry_metadata` feature flag **and** broker config `exposingBrokerEntryMetadataToClientEnabled`. DotPulsar does not send the flag. |
| v18 (client side) TC connect | `TransactionMetaStoreHandler#connectionOpened` (L207-246) | Java skips `TcClientConnectRequest` | Java sends it. **This is the only reason the bump matters for transactions.** |
| v19 | no main-code reference | — | — |
| ≥ v20 TopicMigrated | `PulsarCommandSenderImpl#sendTopicMigrated` (L218-227) | not sent | **not sent** (19 < 20) |
| ≥ v21 AUTO_CONSUME schema | client-only | — | — |
| Proxy: < v10 rejected | `ProxyConnection#handleConnect` | none | none |

**Conclusion.** Raising to 19 introduces **no** new unsolicited broker→client commands and no change in broker
behaviour on 4.0.9 or master. Risk R1 in [12](12-risks-open-questions.md) is therefore low. The regression suite in §6
is still mandatory.

### 4.3 Commands DotPulsar may receive after this feature

| Command | Sent when | Required action |
|---|---|---|
| `TopicMigrated` (68) | client version ≥ 20 | none. **Do not exceed 19** until it is handled. Add the code comment `// Do not exceed 19 until CommandTopicMigrated is handled` at `Constants.ProtocolVersion`. |
| `AckResponse` (38) | any `CommandAck` with `request_id` | **must register** in `RequestResponseHandler` before any ack carries a request id |
| `TcClientConnectResponse`, `NewTxnResponse`, `AddPartitionToTxnResponse`, `AddSubscriptionToTxnResponse`, `EndTxnResponse` | reply to a request | **must register** all five |
| `CommandMessage` with broker-entry-metadata magic `0x0e02` | v ≥ 18 + feature flag + broker config | none, as long as `FeatureFlags.SupportsBrokerEntryMetadata` stays unset. **Warning:** setting it would make `ValidateMagicNumberAndChecksum` reject every message (silent loss). |

## 5. Required edits tied to the bump (WP1)

1. `Constants.ProtocolVersion = 19`, plus the code comment from §4.3.
2. Record the negotiated version: add `int ProtocolVersion { get; }` to `IConnection`, add a settable property on
   `Connection`, and in `ConnectionPool.EstablishNewConnection` set
   `connection.ProtocolVersion = response.Connected.ProtocolVersion;`, next to the existing `MaxMessageSize`.
3. Leave the other `FeatureFlags` (`SupportsBrokerEntryMetadata`, `SupportsPartialProducer`, `SupportsTopicWatchers`)
   unset. `FeatureFlags` in `PulsarClientBuilder` stays unchanged.
4. **Fix a latent bug in the same PR.** `Connection.Send(CommandAuthResponse)` never sets `ProtocolVersion` /
   `ClientVersion`. During a multi-round **initial** authentication the broker would then record the client as v0
   (`ServerCnx#authChallengeSuccessCallback` → `completeConnect(0, "")`). Set
   `ProtocolVersion = Constants.ProtocolVersion` and the client version string, exactly as `CommandConnect` does. Java
   sets both in `Commands.newAuthResponse`.
5. Unknown incoming response types are **ignored**, not thrown, as decided in WP0. This is stricter than the draft
   recommendation, so that no future unsolicited command can kill a connection.

## 6. Regression checklist (WP1 acceptance)

**Integration** (existing `IntegrationFixture`, broker 4.0.9): the **entire existing integration suite** must pass
unchanged on v19. New tests:

| Test file | Test |
|---|---|
| `tests/DotPulsar.Tests/Internal/ConnectionPoolTests.cs` | `FindConnectionForTopic_GivenBrokerSupportsVersion19_ShouldNegotiateProtocolVersion19` |
| `tests/DotPulsar.Tests/PulsarClientTests.cs` | `TokenSupplier_WhenAuthChallengeIsReceived_ShouldStayConnectedWithProtocolVersion19` |
| existing consumer/producer test classes | `Receive_GivenFailoverSubscription_ShouldReceiveActiveConsumerChange` (≥ v12) |
| existing consumer/producer test classes | `Receive_GivenTopicTerminated_ShouldReachEndOfTopic` (≥ v9) |
| existing consumer/producer test classes | `Send_GivenTopicUnloaded_ShouldReconnectViaCloseProducer` (≥ v5) |
| existing consumer/producer test classes | `CreateProducer_GivenSchema_ShouldUseGetOrCreateSchema` (v15 path) |
| existing consumer/producer test classes | `Receive_GivenBatchedMessagesProducedByBroker_ShouldReceiveAll`. Runs once producer batching (WP6) exists; until then, produce batches via `pulsar-client produce` in the container. |

**Unit:**

| Test file | Test |
|---|---|
| `tests/DotPulsar.Tests/Internal/RequestResponseHandlerTests.cs` (new) | `Incoming_GivenAckResponse_ShouldCompleteRequestWithMatchingRequestId` |
| same | one test per TC response type: `Incoming_GivenNewTxnResponse_...`, `..._GivenAddPartitionToTxnResponse_...`, `..._GivenAddSubscriptionToTxnResponse_...`, `..._GivenEndTxnResponse_...`, `..._GivenTcClientConnectResponse_...` |
| same | `Incoming_GivenUnregisteredCommandType_ShouldIgnoreIt` (WP0) |
| `tests/DotPulsar.Tests/Internal/ConnectionTests.cs` or builder tests | `Build_GivenDefaults_ShouldAdvertiseProtocolVersion19AndOnlyAuthRefreshFeatureFlag` |
| `TransactionMetaStoreHandlerTests` | `Connect_GivenNegotiatedProtocolVersionBelow19_ShouldNotSendTcClientConnectRequest` |
