# Follow-up 0005 — Pre-existing issues found while implementing (not caused by this work)

| | |
|---|---|
| **Status** | Items 1 and 2 fixed, item 3 is followup 0001, item 5 documented and worked around in the fixtures; item 4 is open (a separate upstream issue or PR). |
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
