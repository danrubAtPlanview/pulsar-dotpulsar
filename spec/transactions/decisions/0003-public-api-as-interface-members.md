# Expose transactions through interface members

## Context and Problem Statement

Java exposes transactions as `PulsarClient.newTransaction()`, `Producer.newMessage(txn)` and
`Consumer.acknowledge*(…, txn)`. Adding members to `IPulsarClient` and `IConsumer` is a breaking change for anyone
who implements those interfaces outside DotPulsar.

## Considered Options

* Interface members, following the DotPulsar pattern: `IPulsarClient.CreateTransaction(TransactionOptions)` + `NewTransaction()` extension + builder; `IConsumer` ack overloads; `IMessageBuilder.Transaction`; `MessageMetadata.Transaction`
* Extension methods only, downcasting to internal types

## Decision Outcome

Chosen option: "Interface members" (user decision). It is discoverable and type-safe, and DotPulsar has added
interface members in feature releases before (e.g. `AllowOutOfOrderDeliver`). The breaking change is called out in
the CHANGELOG.
