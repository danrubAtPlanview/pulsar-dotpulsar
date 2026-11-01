# Use a separate test fixture for transactions

## Context and Problem Statement

Transactions need broker settings that differ from the shared `IntegrationFixture`: `transactionCoordinatorEnabled`
and `brokerDeduplicationEnabled`, plus a variant with batch-index ack disabled. One upstream test also needs a broker
**without** a coordinator.

## Decision Outcome

Add a `TransactionFixture` with collection `"Transaction"`, a `TransactionWithoutBatchIndexAckFixture` with
collection `"TransactionWithoutBatchIndexAck"`, and reuse `IntegrationFixture` (TC disabled) for the
"broker without TC" case. Existing tests are untouched. The cost is one or two extra containers in CI.
