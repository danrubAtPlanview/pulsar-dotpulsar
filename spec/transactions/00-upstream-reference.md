# 00 — Upstream reference

## 1. Choice of reference

| Candidate | Transactions? | Verdict |
|---|---|---|
| `apache/pulsar-client-cpp` | **No** (placeholder in `pulsar::st`, issue #366 open) | Cannot be ported. Used only as a cross-reference for prerequisite features. |
| `apache/pulsar` Java client | Yes, complete (PIP-31, since 2.7.0), most tested | **Primary reference** ([ADR 0001](decisions/0001-java-client-as-reference.md)) |
| `apache/pulsar-client-go` | Yes (`transaction_impl.go`, `transaction_coordinator_client.go`) | Not chosen. Smaller test suite; it is itself a port of the Java client. Implementers may read it as an informal second opinion, but never as a source of truth. |

Pinned commits are listed in [README §2](README.md#2-pinned-upstream-revisions). To fetch any upstream file at the pinned
commit:

```bash
gh api "repos/apache/pulsar/contents/<path>?ref=f3846f4811b6a7d98de40a3be2caff1fe288e026" -H "Accept: application/vnd.github.raw"
gh api "repos/apache/pulsar-client-cpp/contents/<path>?ref=f71d7e99019d58b1d11858f05ff0dc83b718627b" -H "Accept: application/vnd.github.raw"
```

## 2. v4 vs v5 transactions in upstream master

At the pinned commit, upstream master contains **two** transaction paths:

| Path | Discovery | Enabled by | Status for DotPulsar |
|---|---|---|---|
| **v4** (PIP-31) | `AssignTopicTcDiscovery`: partitioned metadata + lookup of `persistent://pulsar/system/transaction_coordinator_assign-partition-N` | `ClientBuilder.enableTransaction(true)` | **In scope** |
| v5 (PIP-473, scalable topics) | `WatchTcAssignmentsDiscovery`: `CommandWatchTcAssignments` stream | `ClientConfigurationData.scalableTransactions` (internal, v5 SDK only) | Out of scope. All `scalable` proto fields stay unset (false). |

Whenever a Java method has `scalable` branches, port **only** the `scalable == false` branch. Mention the omitted branch
in the `// Upstream:` comment, e.g. `// Upstream: TransactionMetaStoreHandler#start (v4 branch only)`.

## 3. Upstream file inventory — transactions (Java, pinned)

`java:` paths are relative to the repository root. Lines are file lengths at the pinned commit.

### 3.1 API (`pulsar-client-api/src/main/java/org/apache/pulsar/client/api/`)

| File | Lines | C# counterpart | Notes |
|---|---|---|---|
| `transaction/Transaction.java` | 108 | `Abstractions/ITransaction.cs`, `TransactionState.cs` | `State` enum L32-79, `commit` L86, `abort` L93, `getTxnID` L99, `getState` L106 |
| `transaction/TransactionBuilder.java` | 54 | `Abstractions/ITransactionBuilder.cs` | `withTransactionTimeout` L45, `build` L52 |
| `transaction/TxnID.java` | 86 | `TransactionId.cs` | `toString` L63 → `"(most,least)"` |
| `transaction/TransactionCoordinatorClient.java` | 183 | `Internal/Transactions/TransactionCoordinatorClient.cs` (+ `TransactionCoordinatorClientState.cs`) | `DEFAULT_TXN_TTL_MS = 60000` L36; `State` L41-47 |
| `transaction/TransactionCoordinatorClientException.java` | 146 | `Exceptions/*` (see [05-components/exceptions.md](05-components/exceptions.md)) | `unwrap` L126-145 |
| `transaction/TransactionBufferClient*.java` | 98/95 | — | Broker-to-broker, out of scope |
| `PulsarClient.java` | 411 | `Abstractions/IPulsarClient.cs` | `newTransaction()` L403 |
| `ClientBuilder.java` | 816 | `Abstractions/IPulsarClientBuilder.cs` | `enableTransaction` L692 |
| `Producer.java` | 277 | `Abstractions/IMessageBuilder.cs` | `newMessage(Transaction)` L193, `newMessage(Schema, Transaction)` L206 |
| `MessageAcknowledger.java` | 209 | `Abstractions/IConsumer.cs` | ack-with-txn overloads L113, L136, L177, L193 |
| `PulsarClientException.java` | 1190 | `Exceptions/*` | `TransactionConflictException` L932, `TransactionHasOperationFailedException` L955, `unwrap` L1036-1141, `isRetriableError` L1174-1178 |

### 3.2 Implementation (`pulsar-client/src/main/java/org/apache/pulsar/client/impl/`)

| File | Lines | C# counterpart |
|---|---|---|
| `transaction/TransactionImpl.java` | 280 | `Internal/Transactions/Transaction.cs` |
| `transaction/TransactionBuilderImpl.java` | 81 | `Internal/Transactions/TransactionBuilder.cs` |
| `transaction/TransactionCoordinatorClientImpl.java` | 307 | `Internal/Transactions/TransactionCoordinatorClient.cs` |
| `transaction/TcDiscovery.java` | 67 | `Internal/Abstractions/ITcDiscovery.cs`, following the precedent of `ICompressorFactory`, which lives in `Internal/Abstractions` while its implementations live in `Internal/Compression` |
| `transaction/AssignTopicTcDiscovery.java` | 126 | `Internal/Transactions/AssignTopicTcDiscovery.cs` |
| `TransactionMetaStoreHandler.java` | 923 | `Internal/Transactions/TransactionMetaStoreHandler.cs` |
| `transaction/WatchTcAssignmentsDiscovery.java` | 285 | — (v5, out of scope) |
| `transaction/TransactionBufferHandler.java` | 79 | — (broker, out of scope) |
| `ClientCnx.java` | 2229 | `Internal/Connection.cs`, `ChannelManager.cs`, `RequestResponseHandler.cs`, `Extensions/CommandExtensions.cs` |
| `PulsarClientImpl.java` | 2123 | `PulsarClient.cs`, `Internal/PulsarClientBuilder.cs` |
| `ProducerBase.java`, `ProducerImpl.java`, `PartitionedProducerImpl.java`, `TypedMessageBuilderImpl.java` | 207/3278/574/377 | `Internal/MessageBuilder.cs`, `Producer.cs`, `SubProducer.cs`, `ProducerChannel.cs`, `ProducerChannelFactory.cs` |
| `ConsumerBase.java`, `ConsumerImpl.java`, `MultiTopicsConsumerImpl.java` | 1478/3705/1739 | `Internal/Consumer.cs`, `SubConsumer.cs`, `ConsumerChannel.cs` |
| `conf/ClientConfigurationData.java` | 640 | `Internal/PulsarClientBuilder.cs` fields |

### 3.3 Protocol (`pulsar-common`)

| File | C# counterpart |
|---|---|
| `pulsar-common/src/main/proto/PulsarApi.proto` | `src/DotPulsar/Internal/PulsarApi.proto`. It already contains every v4 transaction message. Do **not** sync the v5 `scalable` fields unless needed for wire compatibility; unknown fields are ignored by proto2. |
| `pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java` (2701) | Command construction is inlined at the call sites, following DotPulsar's convention: `new CommandX { ... }` + `CommandExtensions.AsBaseCommand`. |

## 4. Upstream inventory — prerequisite features

| Feature | Java (primary) | C++ (cross-reference) | Spec |
|---|---|---|---|
| Producer batching | `BatchMessageContainerImpl` (505), `BatchMessageKeyBasedContainer` (168), `AbstractBatchMessageContainer` (165), `ProducerImpl` batching paths | `lib/BatchMessageContainer*.{h,cc}`, `lib/MessageAndCallbackBatch.*`, `lib/OpSendMsg.h`, `lib/ProducerImpl.cc` | [06-prerequisites/producer-batching.md](06-prerequisites/producer-batching.md) |
| Batch-index ack (`ack_set`) | `BatchMessageIdImpl` (128), `MessageIdAdvUtils` (86), `ConsumerImpl`, `PersistentAcknowledgmentsGroupingTracker` | `lib/BatchMessageAcker.h`, `lib/BitSet.h`, `lib/AckGroupingTrackerEnabled.cc` | [batch-index-ack.md](06-prerequisites/batch-index-ack.md) |
| Ack receipt | `ClientCnx#newAckForReceipt/handleAckResponse`, `PersistentAcknowledgmentsGroupingTracker` | `lib/AckGroupingTracker*.{h,cc}` | [ack-receipt.md](06-prerequisites/ack-receipt.md) |
| Send timeout | `ProducerImpl#run` (timer), `ProducerConfigurationData.sendTimeoutMs` | `lib/ProducerImpl.cc` (`handleSendTimeout`) | [send-timeout.md](06-prerequisites/send-timeout.md) |
| Ack timeout | `UnAckedMessageTracker` (307) | `lib/UnAckedMessageTracker*` | [ack-timeout.md](06-prerequisites/ack-timeout.md) |
| Dead-letter policy | `DeadLetterPolicy` (85), `ConsumerImpl` DLQ paths | `include/pulsar/DeadLetterPolicy*.h`, `lib/ConsumerImpl.cc` | [dead-letter-policy.md](06-prerequisites/dead-letter-policy.md) |
| Chunking | `ProducerImpl` chunking, `ChunkMessageIdImpl` (76), `ConsumerImpl#processMessageChunk` | `lib/ChunkMessageIdImpl.h`, `lib/ProducerImpl.cc`, `lib/ConsumerImpl.cc` | [chunking.md](06-prerequisites/chunking.md) |
| E2E encryption | `MessageCryptoBc` (pulsar-client-messagecrypto-bc), `CryptoKeyReader` | `lib/MessageCrypto.{h,cc}`, `include/pulsar/CryptoKeyReader.h` | [encryption.md](06-prerequisites/encryption.md) |

## 5. Upstream test inventory

The full list, classification and C# mapping are in [08-test-parity.md](08-test-parity.md). The upstream test sources live in:

- `pulsar-client/src/test/java/org/apache/pulsar/client/impl/` (client unit tests: `TransactionBuilderTest`,
  `TransactionMetaStoreHandlerTest`, `BatchMessageContainerImplTest`, `transaction/WatchTcAssignmentsDiscoveryTest`)
- `pulsar-broker/src/test/java/org/apache/pulsar/client/impl/` (`TransactionEndToEndTest`, `TransactionEndToEndWithoutBatchIndexAckTest`,
  `TransactionClientCloseTest`, `TransactionClientConnectTest`, `AutoCloseUselessClientConTXTest`)
- `pulsar-broker/src/test/java/org/apache/pulsar/broker/transaction/**` (`TransactionTest`, `TransactionProduceTest`,
  `TransactionConsumeTest`, `coordinator/*`, `pendingack/*`, `buffer/*`, `AuthenticatedTransactionProducerConsumerTest`, …)
