# Follow-up 0002 — Ack receipts for a consumer the broker no longer has: no response on broker 4.0.9

| | |
|---|---|
| **Status** | Open. Two test rows are skipped until the test broker image is bumped (WP10 / separate PR). Review the client behaviour after parity. |
| **Found by** | WP1, porting `AckReceiptOnTopicUnloadTest#testDelayedAckReceipt` (`unload = true` rows) |
| **Spec impact** | `06-prerequisites/ack-receipt.md` §4.3 and §9 criterion 5 describe `ConsumerNotFoundException` for this case. That is only true on a broker with apache/pulsar#27128. |
| **Severity** | Low. It affects only acks in flight while the broker removes the consumer (topic unload, ownership change) and only on old brokers. |

## 1. Symptom

With `ConsumerOptions.AckReceiptEnabled = true`, an acknowledgment that is still in flight when the broker unloads the topic never
gets an `AckResponse`. The call fails after `IPulsarClientBuilder.OperationTimeout` (default 30 s) with `OperationTimeoutException`.
The spec expects `ConsumerNotFoundException` immediately after the broker's answer.

## 2. Root cause (verified in the broker source)

`ServerCnx#handleAck` on **`v4.0.9`** (`pulsar-broker/.../ServerCnx.java` L2077-L2119) only answers an ack that carries a `request_id` when the consumer
exists. If `consumers.get(consumerId)` is `null`, which is the case once the unload has closed the consumer, the command is **silently
discarded**: "Consumer future is not complete(not complete or error), but received command ack. so discard this command."

On the pinned upstream master (`f3846f4`, `ServerCnx.java` L3213-L3220) the same branch writes
`Commands.newAckResponse(requestId, ServerError.ConsumerNotFound, …)`. The change is
**apache/pulsar#27128, "[fix][broker] Return errors for discarded ACKs with receipts" (2026-10-27)**, which is newer than 4.0.9.

The client behaves correctly. The pending request is correctly *not* cancelled by `CommandCloseConsumer` (it is keyed by request id only), so
there is simply nothing to wait for. A trace of `Awaiter` cancellations confirmed the only cancel is the 30 s `OperationTimeout` timer.

## 3. What was done

- Both `unload: True` rows of `Acknowledge_GivenDelayedAckReceipt_ShouldCompleteOrFailWithConsumerNotFound` carry
  `Skip = "Requires broker fix apache/pulsar#27128 …"`, following the skip-with-reason rule in `09-test-infrastructure.md` and risk R11.
  The two `unload: False` rows run and pass, which exercises the delayed receipt path through Toxiproxy (`IntegrationFixture.DelayUpstream`).
- `IntegrationFixture.DelayUpstream(TimeSpan)` was added (a Toxiproxy `latency` toxic on the client-to-broker stream) because the upstream
  test delays the ack inside the broker, which is not observable from a client.

## 4. Review items after parity

1. **Bump the test image** (separate PR, per 09 "Cross-cutting rules") to a broker release that contains #27128, then remove the two `Skip`
   arguments and confirm `ConsumerNotFoundException`. Check the other skip-with-reason rows at the same time (risk R11).
2. **Decide whether the client should fail fast on older brokers.** Java's client has the same limitation (it times out after
   `operationTimeoutMs`), so parity holds. A DotPulsar-only improvement would complete pending ack receipts of a consumer with
   `ConsumerNotFoundException` as soon as `CommandCloseConsumer` for that consumer arrives and the ack was sent before it. That requires
   keying ack requests by consumer id as well, which the spec deliberately avoids (the broker still answers in-flight acks on a new broker).
   Recommendation: do not change it; document the timeout in the XML docs of `AckReceiptEnabled`.
3. **Transactional acks use the same path** (WP5). A transactional ack in flight during an unload therefore also ends in
   `OperationTimeoutException` on 4.0.9; make sure the WP5 integration tests that unload topics are written with that in mind.

## Where it stands at the end of WP10

Blocked by the broker image: the two rows run when the test image has apache/pulsar#27128 (a release after 4.0.9). Nothing in the client is left to do; the client behaves correctly.

Checked at the end of WP10 (Docker Hub tag list, 2026-11-01): the newest published image of `apachepulsar/pulsar` is `5.0.1`, built on 2026-10-26, and the fix (apache/pulsar#27128) was merged on 2026-10-27, so no published image has it yet, and a bump of the image of the tests would not un-skip the two rows today. The patch releases that are newer than the pinned `4.0.9` (up to `4.0.14`) were not tried, because the pin is a decision of the maintainers and every fixture is tuned to it.

What is verified without the broker fix: the client handles the answer of the fixed broker. `RequestResponseHandlerTests.Incoming_GivenAckResponseWithConsumerNotFoundAfterCloseConsumer_ShouldCompleteTaskWithErrorResponse` feeds an `AckResponse` with `ConsumerNotFound` to the handler, `SubConsumerTests.Acknowledge_GivenAckResponseWithConsumerNotFound_ShouldThrowConsumerNotFoundExceptionWithoutRetry` (and the transactional variant) shows that the acknowledgment throws `ConsumerNotFoundException` without a retry, and `CommandExtensionsTests` maps `ServerError.ConsumerNotFound` to the exception. What the two skipped integration rows add is only the broker side: that a broker answers a discarded acknowledgment at all.

## Closing

Decided by the requester on 2026-11-01: closed as an external item that is owned by the maintainers. The two skipped rows of `ConsumerTests` run when the test image has apache/pulsar#27128; the client side is covered by unit tests (see above). It is listed in the pull request description.
