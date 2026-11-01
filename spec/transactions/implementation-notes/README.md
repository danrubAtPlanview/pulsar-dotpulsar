# Implementation notes (as built)

Facts recorded by the engineers who built each work package: the exact internal API that the **next** work package must call, and the
choices that differ from the specs. Read the relevant note before wiring or extending a feature. If a note contradicts a spec, the note is
what the code does; fix the spec afterwards (WP10).

| Note | Work package | Used by |
|---|---|---|
| [wp2-wp3-transaction-api.md](wp2-wp3-transaction-api.md) | WP2, WP3 | WP4, WP5 |
| [wp4-producer-transactional-send.md](wp4-producer-transactional-send.md) | WP4 | WP6, WP7 (send timeout) |
| [wp5a-batch-index-ack.md](wp5a-batch-index-ack.md) | WP5a | WP5, WP6, WP7 |
| [wp5-consumer-transactional-ack.md](wp5-consumer-transactional-ack.md) | WP5 | WP6, WP7a, WP7 |
| [wp9a-encryption-core.md](wp9a-encryption-core.md) | WP9a | WP9 (wiring) |
| [wp8a-chunking-core.md](wp8a-chunking-core.md) | WP8a | WP8 (wiring) |
| [wp6a-batching-core.md](wp6a-batching-core.md) | WP6a | WP6 (wiring) |
| [wp7-trackers-dlq-core.md](wp7-trackers-dlq-core.md) | WP7 core | WP7a, WP7 (wiring) |
| [wp7a-wp7-consumer-wiring.md](wp7a-wp7-consumer-wiring.md) | WP7a, WP7 (consumer) | WP8, WP9 consumer wiring, WP10 |
| [wp8-consumer-chunking.md](wp8-consumer-chunking.md) | WP8 (consumer) | WP8 producer, WP9 consumer wiring, WP10 |
| [wp6-producer-batching-send-timeout.md](wp6-producer-batching-send-timeout.md) | WP6 (producer batching, send timeout) | WP8 producer, WP9 producer, WP10 |
| [wp9-consumer-decryption.md](wp9-consumer-decryption.md) | WP9 (consumer, reader) | WP9 producer, WP10 |
| [wp8-producer-chunking.md](wp8-producer-chunking.md) | WP8 (producer) | WP9 producer, WP10 |
| [wp10-test-ports.md](wp10-test-ports.md) | WP10 (tests blocked by batching, ack timeout, DLQ) | WP10 tail |
| [wp9-producer-encryption.md](wp9-producer-encryption.md) | WP9 (producer) | WP10 |
| [wp10-parity-audit.md](wp10-parity-audit.md) | WP10 audit of every row of the parity matrix | WP10 |
| [wp10-parity-completion.md](wp10-parity-completion.md) | WP10 (rows that the audit found missing or weakened) | final report |
| [wp12-non-breaking-public-api.md](wp12-non-breaking-public-api.md) | WP12 (the public interfaces are put back to what `master` has; the 40 members are extension methods; recipe for another branch) | every later change of the API |
| [deviations-summary.md](deviations-summary.md) | all | reviewers: the deviations from the spec in one place |
