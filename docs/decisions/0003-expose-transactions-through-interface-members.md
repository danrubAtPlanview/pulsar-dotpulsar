# Expose transactions through interface members

## Context and Problem Statement

The Java client exposes transactions as `PulsarClient.newTransaction()`, `Producer.newMessage(txn)` and `Consumer.acknowledge(..., txn)`.
Adding members to `IPulsarClient`, `IPulsarClientBuilder`, `IConsumer`, `IConsumerBuilder` and `IMessageBuilder` is a breaking change for anyone who implements those interfaces outside DotPulsar.

## Decision Drivers

* We want the API to be discoverable and type-safe
* We want to follow the existing DotPulsar pattern of putting the capabilities on the interfaces and the convenience on extension methods
* We want to avoid breaking users that only consume the interfaces

## Considered Options

* Add interface members: `IPulsarClient.CreateTransaction(TransactionOptions)` with a `NewTransaction()` extension method and a builder, the `IConsumer` acknowledge overloads that take an `ITransaction`, and `IMessageBuilder.Transaction`
* Add extension methods only, downcasting to the internal types

## Decision Outcome

Chosen option: "Add interface members", because it is discoverable and type-safe, and DotPulsar has added interface members in feature releases before (for example `AllowOutOfOrderDeliver`).
The breaking change for custom implementations of the interfaces is called out in the changelog.

## Validation

The public API is covered by the unit tests of the client, the producer and the consumer, and by the integration tests that run against a broker with the transaction coordinator enabled.

## Pros and Cons of the Options

### Add interface members

* Good, because the API is visible in the IDE and the compiler checks it
* Good, because it follows the pattern that is already used
* Bad, because a custom implementation of one of the interfaces no longer compiles

### Add extension methods only

* Good, because the interfaces do not change
* Bad, because the extension methods can only work with the internal types, so a custom or decorated implementation fails at runtime
