# Behaviour changes are opt-out

Status: **accepted on 2026-11-01** at the request of the requester ("Behavior changes - should become opt-out"). It follows
[0009](0009-no-breaking-public-api.md), which removed the breaking changes of the public API. **Implemented** in commit `d462dae8` (WP13); the as-built
names are in [wp13](../implementation-notes/wp13-behaviour-change-switches.md) and are the planned names below.

## Context and Problem Statement

After WP12 the public API is not broken, but 12 changes of behaviour that an existing program can observe remain
([followup 0020](../followups/0020-public-api-compatibility.md), item 1): what happens to a producer when the broker rejects a send, what
`RedeliverUnacknowledgedMessages` does with an empty list, what `Seek` does with the id of a message in a batch, whether a consumer delivers a
message that it cannot decrypt, whether it delivers the chunks of a chunked message as one message, the protocol version, and some fixes. The
requester decided that they become **opt-out**.

## Decision Outcome

**What "opt-out" means here (the working interpretation, stated so that it can be corrected):** the new behaviour stays the **default**, and a
program that needs the previous behaviour switches it off with an option. Nothing about the default changes by this decision; what changes is
that the previous behaviour is reachable again, so no program is forced to take a behaviour change.

If **opt-in** was meant (the previous behaviour stays the default, and the new one has to be asked for), the work is the same except for one
constant per option: every switch has a `Default...` constant on its options class, as the other options of the repo have
(`ProducerOptions.DefaultEnableBatching`), and flipping the constant and the three places that document it (the XML documentation, the CHANGELOG
line, this record) turns opt-out into opt-in. The requester should say if opt-in was meant.

### The rule (for this work and for later work)

1. A change of what an existing program observes, with a legitimate previous behaviour, gets a switch **in the same commit**, an option that
   restores the previous behaviour, with a `Default...` constant, XML documentation that says what the previous behaviour was, a method on the
   builder (an extension method, decision 0009), a CHANGELOG line under `Changed` that names the option, and a test of each value.
2. A **new feature** is not a behaviour change: it stays off by default (batching, chunking, the ack timeout, ack receipts, the send timeout,
   transactions, the dead letter policy, the negative acknowledge backoff).
3. A **fix of a defect** (a duplicate, a hang, a wrong state, data lost) has no switch, because the previous behaviour was wrong; it is in the
   CHANGELOG under `Fixed`.
4. A change that **cannot** be switched (the broker or the wire decides) is documented with the reason.

### What gets a switch, and what does not (the 12 changes of followup 0020)

The names are the as-built names.

| # | Change | Switch | Default | Opt-out gives |
|---|---|---|---|---|
| 1 | Protocol version 19, was 14 | none: the wire; the transactions need 19 and the ack receipt 17 (`Constants.ProtocolVersionAckReceipt`); the code does not use what a broker of a lower version lacks | - | - |
| 2 | A send that the broker rejects with `NotAllowedError` fails only that message | `ProducerOptions.FailOnlyTheRejectedMessage`, `IProducerBuilder<T>.FailOnlyTheRejectedMessage(bool)` | `true` | `false`: the producer faults, as before |
| 3 | A message larger than the maximum message size fails only its own send (`TooLargeMessageException`) | the same option as 2 | `true` | `false`: the producer faults, as before |
| 4 | `RedeliverUnacknowledgedMessages` with an empty list does nothing | `ConsumerOptions.RedeliverNothingForNoMessageIds`, `IConsumerBuilder<T>.RedeliverNothingForNoMessageIds(bool)` | `true` | `false`: an empty list redelivers everything, as before |
| 5 | `RedeliverUnacknowledgedMessages` with ids on an Exclusive or Failover subscription redelivers all unacknowledged messages | none: the broker only redelivers individual messages for Shared and KeyShared | - | - |
| 6 | `RedeliverUnacknowledgedMessages` sends the ids without the batch index, once per entry; drops the received and not delivered messages; gives the permits back | none: it fixes duplicates and lost permits | - | - |
| 7 | `Seek` with the id of a message in a batch replays the batch from that message | `ConsumerOptions.SeekToBatchIndex` and `ReaderOptions.SeekToBatchIndex`, and the builder methods | `true` | `false`: the seek ignores the position in the batch, and the broker replays the whole batch, as before |
| 8 | A message that cannot be decrypted is not delivered (`CryptoFailureAction.Fail`) | the existing `CryptoFailureAction` of consumer and reader | `Fail` | `ConsumerCryptoFailureAction.Consume` delivers the ciphertext, as before (no new option) |
| 9 | The chunks of a chunked message are delivered as one message | `ConsumerOptions.AssembleChunkedMessages` and `ReaderOptions.AssembleChunkedMessages`, and the builder methods | `true` | `false`: every chunk is delivered as a message, as before |
| 10 | The state of a partitioned consumer is `Active` only when every partition is; the state is published when the channel is in place; a partitioned producer reports `PartiallyConnected` | none: it fixes states that were wrong | - | - |
| 11 | Disposing waits at most `OperationTimeout` (30 s) for the close response | none: it only ends a hang; the time is the existing `OperationTimeout` of the client | - | `OperationTimeout` of the client |
| 12 | A producer without a name sends the name that the broker generated, a fixed producer id and a growing epoch, so the broker deduplicates | none: the previous behaviour stored a message twice after a reconnect | - | - |

Rows 5, 6, 10, 11 and 12 are fixes of a defect, or decided by the broker or the wire (rules 3 and 4); the requester can overrule any of them.

## Consequences

- The default behaviour does not change by this decision; the previous behaviour is available for rows 2, 3, 4, 7, 8 and 9.
- The public API grows by options, not by interface members: the options are properties of the options classes (additive), and the builder methods
  are extension methods with a public method on the builder class (decision 0009). `PublicApiTests` stays unchanged.
- Each switch is a new setting to document and to keep: six new properties (four options, two of them on both `ConsumerOptions` and `ReaderOptions`).
- For the consolidation of another branch: bring the options, the builder methods and the tests over with the commit of wp13, and apply rule 1 to
  every behaviour change that the other branch made after `fc04185d`.
