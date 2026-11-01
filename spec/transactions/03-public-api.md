# 03 — Public API

This is the public surface of the transaction feature. Prerequisite features add more producer and consumer options,
specified in each [06-prerequisites](06-prerequisites/) file.

The shape follows DotPulsar's existing pattern ([ADR 0003](decisions/0003-public-api-as-interface-members.md)):

- a `Create*(Options)` member on `IPulsarClient`;
- an `Options` class;
- a fluent builder returned by a `New*()` extension method in `PulsarClientExtensions`.

Producers, consumers and readers already work this way.

## 1. Java → C# map

| Java | C# |
|---|---|
| `ClientBuilder.enableTransaction(boolean)` | `IPulsarClientBuilder.EnableTransaction(bool enableTransaction)` |
| *(conf)* `operationTimeoutMs` | `IPulsarClientBuilder.OperationTimeout(TimeSpan operationTimeout)` |
| `PulsarClient.newTransaction()` | `PulsarClientExtensions.NewTransaction(this IPulsarClient)` → `ITransactionBuilder` |
| *(impl)* `TransactionCoordinatorClient.newTransactionAsync` via builder | `IPulsarClient.CreateTransaction(TransactionOptions, CancellationToken)` |
| `TransactionBuilder.withTransactionTimeout(long, TimeUnit)` | `ITransactionBuilder.Timeout(TimeSpan timeout)` |
| `TransactionBuilder.build()` | `ITransactionBuilder.Create(CancellationToken)` (`ValueTask<ITransaction>`) |
| `Transaction.commit()` / `abort()` | `ITransaction.Commit(ct)` / `Abort(ct)` (`ValueTask`) |
| `Transaction.getTxnID()` | `ITransaction.Id` (`TransactionId`) |
| `Transaction.getState()` | `ITransaction.CurrentState` (`TransactionState`); `ITransaction.State` (`IState<TransactionState>`, from `IStateHolder`) |
| `Transaction.State.{OPEN,COMMITTING,ABORTING,COMMITTED,ABORTED,ERROR,TIME_OUT}` | `TransactionState.{Open,Committing,Aborting,Committed,Aborted,Error,TimedOut}` |
| `TxnID(most, least)` | `TransactionId(ulong mostSigBits, ulong leastSigBits)` |
| `Producer.newMessage(Transaction)` | `IMessageBuilder<T>.Transaction(ITransaction)` (via `producer.NewMessage().Transaction(txn)`) and `MessageMetadata.Transaction` |
| `Producer.newMessage(Schema<V>, Transaction)` (PIP-407) | Not applicable. DotPulsar producers are typed per instance, so create a producer with that schema. Recorded in [08](08-test-parity.md). |
| `Consumer.acknowledgeAsync(MessageId, Transaction)` | `IConsumer.Acknowledge(MessageId messageId, ITransaction transaction, CancellationToken)` |
| `Consumer.acknowledgeAsync(List<MessageId>, Transaction)` | `IConsumer.Acknowledge(IEnumerable<MessageId> messageIds, ITransaction transaction, CancellationToken)` |
| `Consumer.acknowledgeAsync(Messages<?>, Transaction)` | `ConsumerExtensions.Acknowledge(this IConsumer, IEnumerable<IMessage>, ITransaction, CancellationToken)` |
| `Consumer.acknowledgeCumulativeAsync(MessageId, Transaction)` | `IConsumer.AcknowledgeCumulative(MessageId messageId, ITransaction transaction, CancellationToken)` |
| *(convenience)* | `ConsumerExtensions.Acknowledge(this IConsumer, IMessage, ITransaction, ct)`, `AcknowledgeCumulative(this IConsumer, IMessage, ITransaction, ct)` |
| `PulsarClient.getTransactionCoordinatorClient()` | **Not public.** It is reachable for tests and advanced use through `PulsarClient.TransactionCoordinatorClient` (`public` property on the sealed class, not on the interface; typed `ITransactionCoordinatorClient?`). See §8. |

## 2. New files

### 2.1 `src/DotPulsar/TransactionId.cs`

```csharp
namespace DotPulsar;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/TxnID.java
/// <summary>
/// Unique identifier of a transaction.
/// </summary>
public readonly struct TransactionId : IEquatable<TransactionId>
{
    /// <summary>
    /// Initializes a new instance using the specified most and least significant bits.
    /// </summary>
    public TransactionId(ulong mostSigBits, ulong leastSigBits)
    {
        MostSigBits = mostSigBits;
        LeastSigBits = leastSigBits;
    }

    /// <summary>
    /// The most significant 64 bits of this transaction id. This is the id of the transaction coordinator owning the transaction.
    /// </summary>
    public ulong MostSigBits { get; }

    /// <summary>
    /// The least significant 64 bits of this transaction id.
    /// </summary>
    public ulong LeastSigBits { get; }

    public static bool operator ==(TransactionId x, TransactionId y) => x.Equals(y);
    public static bool operator !=(TransactionId x, TransactionId y) => !x.Equals(y);

    public override bool Equals(object? o) => o is TransactionId id && Equals(id);
    public bool Equals(TransactionId other) => MostSigBits == other.MostSigBits && LeastSigBits == other.LeastSigBits;
    public override int GetHashCode() => HashCode.Combine(MostSigBits, LeastSigBits);
    public override string ToString() => $"({MostSigBits},{LeastSigBits})";
}
```

`HashCode` exists on every target; netstandard2.0 gets it from `Microsoft.Bcl.HashCode`, which is already referenced.
Java caches the hash; a struct has no need to. `ToString` matches Java exactly, e.g. `(0,5)`.

### 2.2 `src/DotPulsar/TransactionState.cs`

```csharp
namespace DotPulsar;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/Transaction.java (State)
/// <summary>
/// The possible states of a transaction.
/// </summary>
public enum TransactionState : byte
{
    /// <summary>
    /// The transaction is open. Messages can be produced and acknowledged within it.
    /// </summary>
    Open,

    /// <summary>
    /// A commit has been requested and is in progress.
    /// </summary>
    Committing,

    /// <summary>
    /// An abort has been requested and is in progress.
    /// </summary>
    Aborting,

    /// <summary>
    /// The transaction was committed. This is a final state.
    /// </summary>
    Committed,

    /// <summary>
    /// The transaction was aborted. This is a final state.
    /// </summary>
    Aborted,

    /// <summary>
    /// The transaction coordinator reported that the transaction is not found or has an invalid status. This is a final state.
    /// </summary>
    Error,

    /// <summary>
    /// The transaction timed out on the client side before it was committed or aborted. This is a final state.
    /// </summary>
    TimedOut
}
```

The member order matches Java. The values are not sent on the wire.

### 2.3 `src/DotPulsar/TransactionOptions.cs`

```csharp
namespace DotPulsar;

/// <summary>
/// The transaction building options.
/// </summary>
public sealed class TransactionOptions
{
    /// <summary>
    /// The default transaction timeout.
    /// </summary>
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Initializes a new instance using the default values.
    /// </summary>
    public TransactionOptions()
    {
        Timeout = DefaultTimeout;
    }

    /// <summary>
    /// Set the transaction timeout. The transaction will be aborted by the broker if it is not committed or aborted within this time. The default is 1 minute.
    /// </summary>
    public TimeSpan Timeout { get; set; }
}
```

### 2.4 `src/DotPulsar/Abstractions/ITransaction.cs`

```csharp
namespace DotPulsar.Abstractions;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/Transaction.java
/// <summary>
/// A transaction abstraction.
/// </summary>
public interface ITransaction : IStateHolder<TransactionState>
{
    /// <summary>
    /// The id of the transaction.
    /// </summary>
    TransactionId Id { get; }

    /// <summary>
    /// The current state of the transaction.
    /// </summary>
    TransactionState CurrentState { get; }

    /// <summary>
    /// Commit the transaction. Waits for all pending sends and acknowledgments of the transaction to complete before committing.
    /// </summary>
    /// <remarks>
    /// If any send or acknowledgment of the transaction has failed, then the transaction is aborted and a TransactionHasOperationFailedException is thrown.
    /// </remarks>
    ValueTask Commit(CancellationToken cancellationToken = default);

    /// <summary>
    /// Abort the transaction. Waits for all pending sends and acknowledgments of the transaction to complete before aborting.
    /// </summary>
    ValueTask Abort(CancellationToken cancellationToken = default);
}
```

`CancellationToken` semantics are the same for `Commit`, `Abort` and `Create`. Cancelling stops the caller's await
only. It does **not** cancel the request already sent to the TC, which completes or times out in the handler.
This mirrors `CompletableFuture`, which has no cancellation. The transaction state stays where it is, e.g.
`Committing`, and the caller may retry.

### 2.5 `src/DotPulsar/Abstractions/ITransactionBuilder.cs`

```csharp
namespace DotPulsar.Abstractions;

// Upstream: apache/pulsar@f3846f4 pulsar-client-api/src/main/java/org/apache/pulsar/client/api/transaction/TransactionBuilder.java
/// <summary>
/// A transaction building abstraction.
/// </summary>
public interface ITransactionBuilder
{
    /// <summary>
    /// Set the transaction timeout. The default is 1 minute.
    /// </summary>
    ITransactionBuilder Timeout(TimeSpan timeout);

    /// <summary>
    /// Create the transaction.
    /// </summary>
    ValueTask<ITransaction> Create(CancellationToken cancellationToken = default);
}
```

The implementation is `src/DotPulsar/Internal/TransactionBuilder.cs`, mirroring `ProducerBuilder`; see
[05/transaction-builder](05-components/transaction-builder.md).

## 3. Changed public interfaces and classes

### 3.1 `IPulsarClient` — add member (breaking for external implementers)

```csharp
/// <summary>
/// Create a transaction.
/// </summary>
ValueTask<ITransaction> CreateTransaction(TransactionOptions options, CancellationToken cancellationToken = default);
```

Place it after `CreateReader`.

### 3.2 `PulsarClientExtensions` — add

```csharp
/// <summary>
/// Get a builder that can be used to configure and build a transaction.
/// </summary>
public static ITransactionBuilder NewTransaction(this IPulsarClient pulsarClient)
    => new TransactionBuilder(pulsarClient);
```

### 3.3 `IPulsarClientBuilder` — append

```csharp
/// <summary>
/// Enable transactions. The default is 'false'.
/// </summary>
IPulsarClientBuilder EnableTransaction(bool enableTransaction);

/// <summary>
/// The operation timeout used for transaction coordinator requests and acknowledgments awaiting a receipt. The default is 30 seconds.
/// </summary>
IPulsarClientBuilder OperationTimeout(TimeSpan operationTimeout);
```

Validation in `Build()`: `OperationTimeout` must be greater than `TimeSpan.Zero`; otherwise throw
`ConfigurationException("OperationTimeout must be greater than zero")`.

### 3.4 `IConsumer` — add members (breaking for external implementers)

```csharp
/// <summary>
/// Acknowledge the consumption of a single message using the message id as part of a transaction.
/// </summary>
ValueTask Acknowledge(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken = default);

/// <summary>
/// Acknowledge the consumption of multiple messages using the message ids as part of a transaction.
/// </summary>
ValueTask Acknowledge(IEnumerable<MessageId> messageIds, ITransaction transaction, CancellationToken cancellationToken = default);

/// <summary>
/// Acknowledge the consumption of all the messages in the topic up to and including the provided message id as part of a transaction.
/// </summary>
/// <remarks>
/// Cumulative acknowledgment is not allowed for subscriptions of type 'Shared' and 'KeyShared'.
/// </remarks>
ValueTask AcknowledgeCumulative(MessageId messageId, ITransaction transaction, CancellationToken cancellationToken = default);
```

Place each overload directly after the existing overload of the same name.

### 3.5 `ConsumerExtensions` — add

```csharp
/// <summary>
/// Acknowledge the consumption of a single message as part of a transaction.
/// </summary>
public static async ValueTask Acknowledge(this IConsumer consumer, IMessage message, ITransaction transaction, CancellationToken cancellationToken = default)
    => await consumer.Acknowledge(message.MessageId, transaction, cancellationToken).ConfigureAwait(false);

/// <summary>
/// Acknowledge the consumption of multiple messages as part of a transaction.
/// </summary>
public static async ValueTask Acknowledge(this IConsumer consumer, IEnumerable<IMessage> messages, ITransaction transaction, CancellationToken cancellationToken = default)
    => await consumer.Acknowledge(messages.Select(m => m.MessageId), transaction, cancellationToken).ConfigureAwait(false);

/// <summary>
/// Acknowledge the consumption of all the messages in the topic up to and including the provided message as part of a transaction.
/// </summary>
public static async ValueTask AcknowledgeCumulative(this IConsumer consumer, IMessage message, ITransaction transaction, CancellationToken cancellationToken = default)
    => await consumer.AcknowledgeCumulative(message.MessageId, transaction, cancellationToken).ConfigureAwait(false);
```

The `IEnumerable<IMessage>` overload is the counterpart of Java's `acknowledgeAsync(Messages<?>, Transaction)`. It does
not exist for non-transactional acks today, so add the plain `Acknowledge(this IConsumer, IEnumerable<IMessage>, ct)`
as well, for symmetry.

### 3.6 `IMessageBuilder<TMessage>` — add (after `SequenceId`, before `Send`)

```csharp
/// <summary>
/// Set the transaction the message is produced as part of. This is optional.
/// </summary>
IMessageBuilder<TMessage> Transaction(ITransaction transaction);
```

The implementation in `Internal/MessageBuilder.cs` is `_metadata.Transaction = transaction; return this;`.

### 3.7 `MessageMetadata` — add property

```csharp
/// <summary>
/// The transaction the message is produced as part of. The default is null (no transaction).
/// </summary>
public ITransaction? Transaction
{
    get => _transaction;
    set
    {
        _transaction = value;
        if (value is null)
        {
            Metadata.ClearTxnidMostBits();
            Metadata.ClearTxnidLeastBits();
        }
        else
        {
            var id = value.Id;
            Metadata.TxnidMostBits = id.MostSigBits;
            Metadata.TxnidLeastBits = id.LeastSigBits;
        }
    }
}
```

Keeping the transaction on the metadata means `ISend.Send(MessageMetadata, T)` and `ISendChannel.Send(...)` need no
new overloads. Every send path, including the send channel, can carry a transaction. `MessageMetadataObjectPool.Return`
must set `Transaction = null` ([05/producer-integration](05-components/producer-integration.md)).

### 3.8 `PulsarClient` (sealed class) — add property, not on the interface

```csharp
/// <summary>
/// The transaction coordinator client. Null if transactions are not enabled.
/// </summary>
public ITransactionCoordinatorClient? TransactionCoordinatorClient { get; }
```

`ITransactionCoordinatorClient` lives in the `DotPulsar.Internal.Abstractions` namespace, so it is advanced API, like
Java's `@Evolving` TC client. It exists so that the parity tests can commit or abort a transaction by
`TransactionId` from a second client (`TransactionEndToEndTest#txnMetadataHandlerRecoverTest`,
`TransactionCoordinatorClientTest`). Tests reach it with `((PulsarClient) client).TransactionCoordinatorClient!`.

## 4. New public exceptions (`src/DotPulsar/Exceptions/`)

| C# | Java | Thrown when |
|---|---|---|
| `TransactionNotFoundException` | `TransactionCoordinatorClientException.TransactionNotFoundException` | `ServerError.TransactionNotFound` from the TC |
| `TransactionHasOperationFailedException` | `PulsarClientException.TransactionHasOperationFailedException` | `Commit` found at least one failed send or ack; the transaction was aborted. Default message: `"Now allowed to commit the transaction due to failed operations of producing or acknowledgment"` (upstream typo "Now" kept verbatim) |
| `TransactionCoordinatorClientStateException` | `CoordinatorClientStateException` | TC client is not `Ready` (closed or not started); a request was made after the client was disposed |
| `TransactionCoordinatorClientException` | `TransactionCoordinatorClientException` (generic) | Generic TC failure, e.g. "The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized"; any TC `ServerError` that has no specific mapping |
| `MetaStoreHandlerNotExistsException` | `MetaStoreHandlerNotExistsException` | No handler for the TC id in `TransactionId.MostSigBits` |
| `MetaStoreHandlerNotReadyException` | `MetaStoreHandlerNotReadyException` | Handler is `Closing`, `Closed` or `Failed` when a request is sent |
| `OperationTimeoutException` | `PulsarClientException.TimeoutException` | A TC request or ack receipt exceeded `OperationTimeout`; also used by the send timeout ([06/send-timeout](06-prerequisites/send-timeout.md)) |

The existing `InvalidTransactionStatusException`, `TransactionConflictException`,
`TransactionCoordinatorNotFoundException` and `NotAllowedException` are reused. The existing `PulsarClientClosedException` gains a `(string message)` constructor and maps Java's `AlreadyClosedException`. Messages and the full mapping are in
[05/exceptions](05-components/exceptions.md).

## 5. Usage (README/docs snippet, written in WP-DOCS)

```csharp
await using var client = PulsarClient.Builder()
    .ServiceUrl(new Uri("pulsar://localhost:6650"))
    .EnableTransaction(true)
    .Build();

await using var producer = client.NewProducer(Schema.String).Topic("persistent://public/default/output").Create();
await using var consumer = client.NewConsumer(Schema.String)
    .SubscriptionName("my-sub")
    .Topic("persistent://public/default/input")
    .Create();

var transaction = await client.NewTransaction().Timeout(TimeSpan.FromSeconds(30)).Create();
var message = await consumer.Receive();
await producer.NewMessage().Transaction(transaction).Send(message.Value().ToUpperInvariant());
await consumer.Acknowledge(message, transaction);
await transaction.Commit();
```

## 6. Behavioural contract summary

| Situation | Result |
|---|---|
| `NewTransaction().Create()` while `EnableTransaction(false)` | `ConfigurationException("Transactions are not enabled")`. Java's `InvalidConfigurationException` maps to the existing DotPulsar `ConfigurationException`. |
| Broker without a TC (assign topic has 0 partitions) | `Create()` throws `TransactionCoordinatorClientException("The broker doesn't enable the transaction coordinator, or the transaction coordinator has not initialized")`; the next `Create()` retries discovery |
| Send or ack with a transaction that is not `Open` | `InvalidTransactionStatusException` with message `"[{id}] with unexpected state : {State}, expect Open state!"`, matching Java `InvalidTxnStatusException(txnId, actual, expected)` L79-82 |
| `Commit`/`Abort` in the wrong state | `InvalidTransactionStatusException` with message `"[{most}:{least}] with unexpected state: {State}, expect: [{expected...}]"`, matching Java `checkState` L269-279 |
| Transactional cumulative ack on `Shared`/`KeyShared` | `ConfigurationException("Cannot use cumulative acks on a non-exclusive/non-failover subscription")` |
| Conflicting transactional ack | `TransactionConflictException` (from `CommandAckResponse.error`) |
| Any `ITransaction` not created by DotPulsar is passed to send or ack | `ArgumentException("The transaction must be created by DotPulsar", nameof(transaction))` |

State names in messages use the C# enum names (`Open`, `TimedOut`, …). Tests assert exception **types**, never message
text, except where upstream asserts text; [08](08-test-parity.md) flags those cases.

## 7. CHANGELOG entries (WP-DOCS)

```
## [Unreleased]

### Added

- Support for transactions using IPulsarClient.CreateTransaction / IPulsarClient.NewTransaction()
- Transactional produce using IMessageBuilder.Transaction / MessageMetadata.Transaction
- Transactional acknowledgment using IConsumer.Acknowledge and IConsumer.AcknowledgeCumulative with an ITransaction
- EnableTransaction and OperationTimeout on IPulsarClientBuilder
- New exceptions: TransactionNotFoundException, TransactionHasOperationFailedException, TransactionCoordinatorClientException, TransactionCoordinatorClientStateException, MetaStoreHandlerNotExistsException, MetaStoreHandlerNotReadyException and OperationTimeoutException

### Changed

- Updated the protocol version from 14 to 19
- IPulsarClient and IConsumer have new members. This is a breaking change for custom implementations of these interfaces
```

Prerequisite features add their own entries (see each [06](06-prerequisites/) file).
