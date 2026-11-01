# Follow-up 0005 — Pre-existing issues found while implementing (not caused by this work)

| | |
|---|---|
| **Status** | Items 1 and 2 fixed, item 3 is followup 0001, item 5 documented and worked around in the fixtures; item 4 was fixed after WP11-D (see "Update after WP11-D"). |
| **Severity** | Low to medium. None blocks a work package unless noted. |

| # | Where | Finding | Found by | Suggested handling |
|---|---|---|---|---|
| 1 | `ProducerChannel.Send`, schema version | `metadata.SchemaVersion is null` can never be true, because the protobuf getter returns a default value (`ByteString.Empty`) rather than null. The schema-version injection beside it looks like dead code. Batching's `HasSameSchema` uses the has-bits instead. | WP6a | Check whether the schema version is ever sent by the producer; fix with `HasSchemaVersion` and add a test. Separate PR. **FIXED in WP10** (`Send the schema version of a producer`: `HasSchemaVersion` test, `ClearSchemaVersion` after the send, same in `PlanChunking`; found by the port of `testSendAndAckWithSchema`; before, `IMessage.SchemaVersion` was null for every message DotPulsar produced). |
| 2 | `ProducerChannel.Send`, `CommandSend.NumMessages` | `NumMessages` is never reset after use, and `highest_sequence_id` is unset. Harmless today (no batching) but wrong for batches. | WP5a, WP6a | Fixed with the batching wiring (producer-batching.md §4.11). Also listed in `06-prerequisites/README.md` §6 #7. |
| 3 | `MessageProcessorTests` | Hangs in Release, also on `master`. | WP1 | See [0001](0001-release-mode-test-hang.md). |
| 4 | `IdLookup` / `ChannelManager` | An unknown consumer id outside the array (for example 42 when only id 0 exists) throws `IndexOutOfRangeException` rather than behaving like a missing channel (`null`). It cannot happen with ids the client hands out, but unit tests that fake an id hit it. | WP1 | Optional hardening: bounds check in `IdLookup`'s indexer. Low priority. |
| 5 | Test project | `Testcontainers.Pulsar` applies broker environment configuration only with `.WithAuthentication()`; the standalone broker loads the transaction coordinator only on the first `CommandTcClientConnectRequest`. Documented in the fixtures; relevant for anyone copying the fixture. | WP0 | Keep the comments in `TransactionFixtureBase`. |

## Status at the end of WP10

Items 1 and 2 are fixed (the schema version is sent since WP10, `NumMessages` is reset with the batching wiring). Item 3 is followup 0001. Item 4 (`IdLookup` / `ChannelManager` with a consumer id outside the array) is not changed and open. Item 5 (the environment variables only apply with `.WithAuthentication()`, and the transaction coordinator is loaded by the first connect request) is documented in the comments of `IntegrationFixture` and `TransactionFixtureBase`, and the fixtures work around it.

Item 4 (`IdLookup` with an id outside the array) is optional hardening of master, low priority: it can only happen with an id that the client never handed out (a faulty broker, or a unit test that fakes an id). It is left as it is, and can be an upstream issue.

## Update after WP11-D (item 4 fixed)

Item 4 was done on 2026-11-01 in the commit `Ignore ids that the lookup never gave out` (it was left open as optional hardening of master before). Looking at it again showed more than the exception:

- An id that is outside the array threw `IndexOutOfRangeException` in the indexer and in `Remove`, in the loop that reads the commands of the connection (`ChannelManager.Incoming(CommandMessage)`, `ReachedEndOfTopic`, `CloseConsumer`, and the removal of a handler of the transaction meta store).
- An id such as 4294967296 (2^32) did not throw: the cast `(int) id` turned it into 0, so the command reached the consumer that has the id 0, and a `CloseConsumer` for it closed that consumer (shown by the test before the fix).
- `IdLookup` used the array as the object of its lock and replaces the array in `Add`, so two callers could hold the lock of the old and of the new array at the same time. A stress test with 8 callers fails with `ArgumentException` (`CopyTo`) before the fix.

The indexer and `Remove` return `null` for an id outside the array (the callers already handle a missing channel), and the lock is an object of its own. The lookup was not replaced by `SparseIdLookup`: the consumer lookup is read for every message that arrives, and an array read is cheaper than a dictionary read there.

Tests: `IdLookupTests` (10 new rows) and `ChannelManagerConsumerIdTests` (8 rows, new class). All 18 fail before the fix and pass after it.
