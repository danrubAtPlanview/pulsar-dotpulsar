# Component: `TransactionBuilder` and `PulsarClient.CreateTransaction`

| | |
|---|---|
| C# | `src/DotPulsar/Internal/TransactionBuilder.cs`: `public sealed class TransactionBuilder : ITransactionBuilder`; `PulsarClient.CreateTransaction` |
| Upstream | `java:pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/TransactionBuilderImpl.java` (L38-39 defaults, L54-80 `build`); `PulsarClientImpl#newTransaction` L2111-2113 |
| WP | WP3 |

## 1. `TransactionBuilder` (mirrors `Internal/ProducerBuilder.cs`)

```csharp
namespace DotPulsar.Internal;

using DotPulsar.Abstractions;

// Upstream: apache/pulsar@f3846f4 pulsar-client/src/main/java/org/apache/pulsar/client/impl/transaction/TransactionBuilderImpl.java
public sealed class TransactionBuilder : ITransactionBuilder
{
    private readonly IPulsarClient _pulsarClient;
    private TimeSpan _timeout;

    public TransactionBuilder(IPulsarClient pulsarClient)
    {
        _pulsarClient = pulsarClient;
        _timeout = TransactionOptions.DefaultTimeout;
    }

    public ITransactionBuilder Timeout(TimeSpan timeout)
    {
        _timeout = timeout;
        return this;
    }

    public async ValueTask<ITransaction> Create(CancellationToken cancellationToken)
    {
        if (_timeout <= TimeSpan.Zero)
            throw new ConfigurationException("TransactionOptions.Timeout must be greater than zero");

        var options = new TransactionOptions { Timeout = _timeout };
        return await _pulsarClient.CreateTransaction(options, cancellationToken).ConfigureAwait(false);
    }
}
```

Java does not validate the timeout. Rejecting a timeout ≤ 0 is a C# addition: the broker treats 0 as "use the
default", and a negative value would wrap when cast to `ulong`. Record this in the additional tests.

## 2. `PulsarClient.CreateTransaction` (Java `TransactionBuilderImpl#build` body)

```csharp
public async ValueTask<ITransaction> CreateTransaction(TransactionOptions options, CancellationToken cancellationToken)
{
    ThrowIfDisposed();

    if (_transactionCoordinatorClient is null)                                  // Upstream L55-58
        throw new ConfigurationException("Transactions are not enabled");

    await _transactionCoordinatorClient.EnsureStarted(cancellationToken).ConfigureAwait(false);  // lazy start, see 02 §4
    var id = await _transactionCoordinatorClient.NewTransaction(options.Timeout, cancellationToken).ConfigureAwait(false);
    return new Transaction(_transactionCoordinatorClient, id, options.Timeout);  // Upstream L75-76
}
```

- The `cancellationToken` passed to `NewTransaction` only abandons the caller's wait. A transaction already created
  on the TC is left to the broker timeout, as in Java.
- No retry happens here; retries live in the handler ([transaction-meta-store-handler](transaction-meta-store-handler.md)).

## 3. Tests

| Test | Type | Upstream |
|---|---|---|
| `Create_GivenTransactionsNotEnabled_ShouldThrowConfigurationException` | Unit (`PulsarClientFactory.CreatePulsarClient` with a substituted pool, no TC client) | `TransactionBuilderTest#checkClientEnableTransactionConfiguration` |
| `Create_GivenZeroTimeout_ShouldThrowConfigurationException` | Unit | additional |
| `Create_GivenDefaultTimeout_ShouldSendSixtySecondsInMilliseconds` | Unit (mock TC client receives `TimeSpan.FromMinutes(1)`) | `TransactionBuilderImpl` defaults L38-39 |
| `Create_ShouldReturnOpenTransactionWithIdFromCoordinator` | Unit | L75-76 |
