# Replace the batch acknowledgment tracker when an entry is delivered again

## Context and Problem Statement

With batch index acknowledgment, the consumer keeps one tracker per batch entry. The tracker has one bit for each message that is not yet acknowledged, and the entry is acknowledged when every bit is cleared.
When the broker delivers an entry again, the consumer already has a tracker for it. The first idea was to merge the tracker of the new delivery into the old one with a bitwise AND.

That loses messages after an aborted transaction:

1. A cumulative acknowledgment of index *k* in a transaction clears the bits `0..k` in the tracker
2. The transaction is aborted. The broker rolls back its pending acknowledgments and delivers the whole entry again
3. With AND, the bits `0..k` are still cleared. The batch is complete after fewer acknowledgments than it has messages, and the whole entry is acknowledged while some of its messages were never acknowledged again

The Java client does not have the problem, because each delivery of a batch gets a new acknowledger.

## Decision Drivers

* A message must never be acknowledged without the application acknowledging it
* The consumer should agree with the broker about which messages of the entry are still unacknowledged

## Considered Options

* Merge the tracker of the new delivery into the old one with AND
* Keep the old tracker and ignore the new one
* Replace the old tracker with one built from the `ack_set` and the batch size of the new delivery

## Decision Outcome

Chosen option: "Replace the old tracker", because the broker delivers the whole entry except the indexes it knows are acknowledged, so a tracker built from the new delivery matches the broker's view and the behavior of the Java client.
An acknowledgment of a message from the old delivery clears its own bit in the new tracker.

When the consumer drops the messages it has received but not yet delivered (redelivery of unacknowledged messages), it keeps the trackers, because dropping them would let an acknowledgment of a message from the old delivery acknowledge the whole entry.
They are replaced when the entry is delivered again.

## Validation

A unit test covers a cumulative acknowledgment in a transaction that is aborted and followed by a redelivery of the batch: the entry is acknowledged only when every index is acknowledged again.
Integration tests with batches and aborted transactions run against a broker with batch index acknowledgment enabled and disabled.

## Pros and Cons of the Options

### Merge with AND

* Good, because it keeps acknowledgments that were made before the redelivery
* Bad, because it loses messages after an aborted transaction

### Keep the old tracker

* Good, because it is the simplest
* Bad, because it loses messages in the same way, and keeps a tracker that disagrees with the broker

### Replace the old tracker

* Good, because it matches the broker and the Java client, and cannot lose messages
* Bad, because the progress of the old tracker is discarded, which is only correct because the broker delivers the entry again from its own state
