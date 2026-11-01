# Core open decisions (D1-D10)

Several component files refer to these decisions by number (D1-D10).

Status as integrated into this spec:

| Decision | Status |
|---|---|
| D1, D2 | Adopted: lazy start, see ADR 0008 |
| D5 | Adopted: proto rename, see connection-integration §0.2 |
| D6 | Adopted: flat exceptions |
| D9 | Resolved: `MessageMetadata.Transaction` is public |
| D10 | Adopted, with one addition: `OperationTimeout` also bounds ack receipts, see 06-prerequisites/README §2 |
| D3, D4, D7, D8 | Follow the recommendation below |

## Open decisions (recommendation)

- **D1 TC client construction** (lazy start itself is DECIDED): construct `TransactionCoordinatorClient` in the `PulsarClient` ctor without I/O
  so `PulsarClient.TransactionCoordinatorClient` is non-null for every txn-enabled client, and start discovery lazily on the TC client's first
  operation — **recommended**. Alternative: create the instance on first `CreateTransaction` (then the property is null until then and tests that
  use the TC client directly must create a transaction first). Ports of `TransactionClientConnectTest#testNotEnableTransactionInBroker`
  (L177-L183) assert `NewTransaction().Create()` throws `TransactionCoordinatorClientException`.
- **D2 failed start caching**: do not cache a failed TC start; retry on the next operation — recommended (Java has no equivalent since the
  whole client fails). "0 assign-topic partitions" => `TransactionCoordinatorClientException` with Java's message (decided in API UPDATE).
- **D3 SendError scope**: only `NotAllowedError` becomes per-message (Java parity, required by TransactionTest). Java's reconnect-and-resend for
  `PersistenceError`/default (ClientCnx L1255-L1257) and terminate for `TopicTerminatedError` are **out of scope** (existing DotPulsar behaviour
  — fault — kept); sequence mismatch -> reconnect rather than Java's silent ignore.
- **D4 txn ack transport retry**: keep `_executor.Execute` retry for transport errors (resend after reconnect) — recommended for DotPulsar
  consistency; Java fails fast with `ConnectException`. Whether the broker pending-ack store accepts a duplicate ack of the same position by the same txn was NOT verified; if it rejects it, a resend surfaces as TransactionConflictException to the caller (acceptable).
- **D5 proto field name**: rename `CommandNewTxn.txn_ttl_seconds` to `txn_ttl_millis` in `PulsarApi.proto` (same field number 2, wire-compatible,
  matches pinned upstream L1162) — recommended to prevent a seconds/millis bug; otherwise document at the single call site.
- **D6 exception hierarchy**: the exception set is decided (API UPDATE). Open point: whether `TransactionCoordinatorNotFoundException`,
  `InvalidTransactionStatusException` and `TransactionNotFoundException` should derive from the new `TransactionCoordinatorClientException`
  (Java hierarchy) — they are existing `sealed` `DotPulsarException`s, so re-parenting is a breaking-ish change. Recommendation: keep them as
  direct `DotPulsarException`s; `TransactionCoordinatorClientException` is the generic fallback only (section 1.2 note).
- **D7 instrumentation**: none in core (Java has none); optional activity tag later.
- **D8 cumulative-ack check for non-txn `AcknowledgeCumulative`**: Java applies the Shared/KeyShared check to both overloads; adding it to the
  existing non-txn overload is a behaviour change (today the broker silently ignores). Recommendation: txn overload only in this change.
- **D9** (resolved by API UPDATE): `MessageMetadata.Transaction` is a public `ITransaction?`; consequently send-op registration lives in
  `Producer.InternalSend` (9.3), not in `MessageBuilder`.
- **D10 `OperationTimeout` scope**: applies to TC client requests/connect deadline only (Java also uses it for lookups/producer creation);
  recommendation: do not retrofit existing DotPulsar lookup/producer/consumer paths with it in this change.

## Blockers found

None hard. Dependencies: transactional ack of batched messages needs the batch-index-ack prerequisite to expose the batch size per message id
(10.5); `ProcessNotAllowedSendError` must switch to highest-sequence-id once producer batching lands (9.6).
