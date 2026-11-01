# Impact on tests, instrumentation and other files

All DotPulsar line numbers refer to baseline `e8f2ba22c7136a35d279451e7278589fb0084d2e`. Java citations use `java:<path>#Symbol (Lx-Ly)` at `f3846f4811b6a7d98de40a3be2caff1fe288e026`. Abbreviations: `J=pulsar-client/src/main/java/org/apache/pulsar/client/impl`, `B=pulsar-broker/src/main/java/org/apache/pulsar/broker/service`, `C=pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java`.

> Line numbers drift as earlier WPs land. Locate each edit by the anchor text (method name or neighbouring member), and use the number only as a hint.

## 12. Tracing / metrics (`DotPulsarActivitySource.cs`, `DotPulsarMeter.cs`)

Java client has **no** transaction instrumentation: `TransactionMetaStoreHandler.java`, `transaction/TransactionImpl.java`,
`transaction/TransactionCoordinatorClientImpl.java`, `transaction/TransactionBuilderImpl.java`, `transaction/AssignTopicTcDiscovery.java` contain no
`InstrumentProvider`/`Counter`/`LatencyHistogram` usage (grep at pinned SHA). Recommendation: **no new meters or activities** in the core change.
Existing producer send activity (Producer.cs L248, `StartProducerActivity`) and consumer process activity are unchanged. Optional follow-up (D7):
tag the producer activity with `messaging.pulsar.transaction_id` = `TransactionId.ToString()` when a txn is attached.

---

## 13. Other repo files

- `CHANGELOG.md`: new `## [Unreleased]` entry (Added: transactions, `EnableTransaction`, `OperationTimeout`, `CreateTransaction`/`NewTransaction()`, `TransactionOptions`, txn acks, `IMessageBuilder.Transaction`, `MessageMetadata.Transaction`, the new transaction exceptions,
  `TransactionNotFoundException`; Changed: protocol version 19, `NotAllowedError` send errors fail the message instead of faulting the producer).
- `src/DotPulsar/README.md` / `docs`: optional usage snippet.
- `src/DotPulsar/Internal/PulsarApi.proto`: only if D5 (rename `txn_ttl_seconds` -> `txn_ttl_millis`) is accepted.

---

## 14. Test doubles / test call sites affected by the interface and constructor changes

Search: `: IConnection`, `IConsumerChannel`, `IProducerChannel`, `Substitute.For<...>`, explicit implementers of every changed interface,
constructor call sites (grep over `tests/`, `src/`, `samples/`, `benchmarks/`, excluding `obj/`).

| Changed type | Implementers / call sites in tests | Impact |
|---|---|---|
| `IConnection` (+`ProtocolVersion`, `SendWithResponse`, 5 TC `Send`, 2 registration members) | `Substitute.For<IConnection>()` in `tests/DotPulsar.Tests/Internal/ConsumerChannelTests.cs` L32, L57, L83, L110, L134, L149 | NSubstitute proxies — compile unaffected; `connection.Send(Arg.Any<CommandAck>(), ...)` stays unambiguous because the new member is named `SendWithResponse` |
| `IConsumerChannel<T>` (+`SendTransactional`; ack-receipt.md changes `Send(CommandAck)` to `Task<BaseCommand?>`) | no test double; implementers `ConsumerChannel` L23, `NotReadyChannel` L23 | src only |
| `IProducerChannel` | `Substitute.For<IProducerChannel>()` `tests/DotPulsar.Tests/Internal/SubProducerTests.cs` L30, L50 | interface unchanged by this draft |
| `IConsumer<T>` / `IConsumer` (+3 members) | `[Frozen] IConsumer<byte[]> consumer` (AutoFixture+NSubstitute) `tests/DotPulsar.Tests/Internal/MessageProcessorTests.cs` L42 and the other theories in that file | proxy — compile unaffected |
| `IMessage<T>` | `Substitute.For<IMessage<byte[]>>()` MessageProcessorTests.cs L109 | unchanged |
| `IPulsarClient` (+`CreateTransaction`), `IPulsarClientBuilder` (+`EnableTransaction`, `OperationTimeout`), `IMessageBuilder<T>` (+`Transaction`), `MessageMetadata` (+`Transaction`) | no test doubles; implementers `PulsarClient`, `PulsarClientBuilder`, `MessageBuilder` only | src only |
| `IExecute` | explicit implementer `SubProducerTests.DirectExecutor` (SubProducerTests.cs L58-L111) | unchanged (no IExecute edit) |
| `IContainsChannel` | explicit implementer `ProcessReconnectTests.TrackingChannelContainer` (ProcessReconnectTests.cs L247) | unchanged |
| `SubProducer` ctor | `SubProducerTests.CreateSubProducer` L46-L56 (named args) | unchanged (no ctor edit in SubProducer) |
| `Producer<T>` ctor (+`transactionsEnabled`), `ProducerChannelFactory` ctor (+`transactionsEnabled`), `PulsarClient` internal ctor (+`enableTransaction`, `operationTimeout`) | no test call sites (only `PulsarClient.cs` L76, `Producer.cs` L163, `PulsarClientBuilder.cs` L178, `PulsarClientFactory.cs` L23) | src only |
| `ChannelManager`, `RequestResponseHandler`, `Connection` | not constructed in tests (IntegrationFixture L63/L108 `new Connection` is `Toxiproxy.Client.Connection`, unrelated) | none |
| `ConsumerChannel<T>` ctor | unchanged by this draft | none |

New unit tests to add (mirror src paths, `[Trait("Category", "Unit")]`), suggested names:
- `tests/DotPulsar.Tests/Internal/RequestResponseHandlerTests.cs`: `Incoming_GivenNewTxnResponse_ShouldCompleteOutgoingNewTxn`,
  `Incoming_GivenAckResponse_ShouldCompleteOutgoingWithResponse` (+ one per TC response type),
  `Incoming_GivenCloseConsumer_ShouldNotCancelPendingAckWithResponse`.
- `tests/DotPulsar.Tests/Internal/ChannelManagerTests.cs`: `AddTransactionMetaStoreHandler_GivenNoChannels_ShouldSetStateActive`,
  `RemoveTransactionMetaStoreHandler_GivenLastRegistration_ShouldSetStateInactive`, `Dispose_GivenRegisteredHandler_ShouldCallConnectionClosed`.
- `tests/DotPulsar.Tests/Internal/ProducerChannelTests.cs`: `Send_GivenTransactionalThenNonTransactionalMetadata_ShouldClearTxnIdsOnPooledCommand`.
- `tests/DotPulsar.Tests/Internal/SubProducerTests.cs`: `ResponseProcessor_GivenNotAllowedSendError_ShouldFailOnlyThatMessage` (via `Send` with a
  substitute `IProducerChannel` completing the TCS with a `SendError`), `..._GivenPersistenceSendError_ShouldFaultProducer` (pins current behaviour).
- `tests/DotPulsar.Tests/Internal/ConsumerChannelTests.cs`: `SendTransactional_GivenBatchMessageId_ShouldNotBeSwallowedByBatchHandler`.
- `tests/DotPulsar.Tests/Internal/Extensions/CommandExtensionsTests.cs` (new): `Throw_GivenTransactionNotFound_ShouldThrowTransactionNotFoundException`.

---

