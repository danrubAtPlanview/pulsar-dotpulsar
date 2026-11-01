# 0008 - Findings from WP8 (consumer chunking)

Status: done (item 1 fixed, item 2 verified in WP10, item 3 is not a client issue).

## 1. Unbounded array from the broker in `ChunkedMessageAssembler` (FIXED: the array grows as chunks arrive, test `Process_GivenBrokerThatClaimsAHugeNumberOfChunks_...`)

`ChunkedMessageAssembler` allocates `new MessageIdData[numberOfChunks]` where `numberOfChunks` is the broker-supplied `num_chunks_from_msg`. There is no upper bound, so a buggy or hostile broker can make the client allocate a huge array
(Java sizes `MessageId[]` from the same field, so this matches upstream, but the .NET client is a library inside user processes).
Proposed fix: validate `numberOfChunks` against a sane limit (for example reject values that need more than `MaxMessageSize`-based bounds, or allocate lazily/grow on demand) in the core, with a test. Low severity.

## 2. One unexplained Release-mode unit failure

The WP8 consumer agent saw 1 failing test in its first Release run after the wiring build and did not capture which one; 8 following Release runs and 2 runs after the merge were green.
Action (WP10): run the Release unit suite repeatedly (for example 20 times, `MessageProcessorTests` excluded) with a logger that records failures (`--logger "trx"`), and fix the race if it comes back.

## 3. The perf tool produces two chunked messages for `-m 1`

`pulsar-perf produce -ch` sent two chunked messages when asked for one. The consumer chunking tests tolerate extra messages. Not a client issue; the WP8 producer tests use the real producer.

## Status at the end of WP10

Item 1 is fixed. Item 2 is done: the Release unit suite ran more than 200 times in WP10 (normal runs and the stress rounds with 4 parallel processes and 12 busy loops), and every failure that came up was explained and fixed (followups 0009 and 0018); no unexplained failure is left. Item 3 is not a client issue.
