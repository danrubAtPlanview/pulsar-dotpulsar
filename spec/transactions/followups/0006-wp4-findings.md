# 0006 - Findings from WP4 (producer integration)

Status: items 1 and 2 fixed; item 3 open (broker bug apache/pulsar#27082, one test row skipped); item 4 unchanged and documented (section at the end).

## 1. A too-large message faults the producer (R-MAX-SIZE does not hold) - FIXED by the WP8 producer: `Fail only the send of a message that is too large`, see implementation-notes/wp8-producer-chunking.md

Spec (`05-components/producer-integration.md`) says a message larger than the broker's max message size fails only that send with `TooLargeMessageException`. Today `Connection.Send` throws inside the `SubProducer` dispatcher,
the producer faults and the pending send ends as `TaskCanceledException`. Pre-existing behaviour; the transactional test P7 passes anyway because its commit/abort assertions hold.
Action: WP8 reworks the `Connection.Send` size check for chunking; decide there whether the exception is raised per send (as Java does) and add the missing test. If WP8 does not cover it, raise it with the maintainers.

## 2. Second test container intermittently fails to start under load

The `TransactionWithoutBatchIndexAckFixture` container sometimes fails with "Failed to create token" or hangs past 5 minutes (`WaitForTransactionCoordinator` has no timeout shorter than 10 minutes).
In 8 attempts on the final WP4 code, 4 failed this way, before any test ran. WP5 saw `container ... is not running` (broker killed under memory pressure) in most of its runs.
Done in `Bound the transaction broker startup in tests` (not yet exercised against a real failure): `TransactionFixtureBase.InitializeAsync` starts the broker in attempts of at most 4 minutes (the coordinator wait is inside the attempt), repeats a failed or timed out attempt with a new container up to 3 times, and a final timeout is a `TimeoutException` with a clear message. Check in WP10 whether the repeated attempts reduce the failures.
Original action (WP10): give `WaitForTransactionCoordinator` a bounded timeout with a clear message. Sharing one broker between the two transaction fixtures is not possible: batch index ack is a broker setting.
Run the two transaction collections one at a time and never beside the "Integration" collection.

## 3. Out-of-order receipts on broker 4.0.9

`TopicTransactionBufferPublishOrderTests` (`recovering: false`) is skipped: the broker stores the plain message before the transactional one, receipts arrive out of order, DotPulsar reconnects and resends,
which duplicates messages with deduplication off. Broker fix: apache/pulsar#27082 (master only). Un-skip when the image is bumped.

## 4. NotAllowedError behind the head of the queue

DotPulsar reconnects and resends when a `NotAllowedError` arrives for an op that is not at the head of the queue; Java ignores it. Matches the spec for now; revisit with WP6 (grouped matching).

## Status at the end of WP10

Item 1 is fixed (WP8 producer). Item 2 is done: the broker start is bounded and repeated (`TransactionFixtureBase.StartClusterWithRetries`), and with the broker limit of `BrokerResources` no fixture failed to start in the final runs (4 runs of all the integration collections after the limit was fixed, and the 20 iterations of the WP10 loop). Item 3 stays open and is not ours: apache/pulsar#27082, one test row is skipped until the image has the fix. Item 4 is unchanged and documented: a `NotAllowedError` for a message behind the head of the queue still triggers a reconnect and a resend (`implementation-notes/wp6-producer-batching-send-timeout.md`, item 3 of the send timeout rules), where Java ignores it.
