# 0014 - Disposing a producer, consumer or reader waits unbounded for the close response

Status: **fixed in WP10** (pre-existing on master, medium severity), in its own commit `Bound the wait for the close response on dispose`.

`SubProducer.InternalDispose` (line 159), `SubConsumer.cs:124` and `SubReader.cs:117` await the response to `CommandCloseProducer`/`CommandCloseConsumer` with `CancellationToken.None`. If the connection is dead but not yet detected, disposal waits until the keep-alive watchdog closes it: 91 s with the default keep-alive (the broker's ping at +30 s resets the client's watchdog, so detection takes two more 30 s intervals), found by the WP10 end-to-end tests with a relay that swallowed everything the client sent. The Java client bounds the close request with the operation timeout (30 s).
Suggested fix after parity: a bounded wait (operation timeout) in the three dispose paths, then proceed with the disposal as if closed. It touches producer, consumer and reader, so it needs its own change and tests.

## Fix (WP10)

The channels (`ConsumerChannel.ClosedByClient`, `ProducerChannel.ClosedByClient`) wait for the response to the close command with the token that they are given (`Task.WaitAsync(token)`), because the token of `Connection.Send` only guards the lock of the connection and the response was awaited without it. `SubConsumer`, `SubReader` and `SubProducer` give them a token that is cancelled after the operation timeout of the client (30 seconds by default, `IPulsarClientBuilder.OperationTimeout`), and go on with the disposal when it is cancelled; an exception of the close is ignored there, as before. `SubConsumer` had the operation timeout already. `Producer`, `Reader`, `SubProducer` and `SubReader` take it as an optional trailing parameter (no limit when it is not given, which is what the tests that build them did before), `PulsarClient` passes it, and so does the dead letter producer of the consumer.

Tests (`DisposeCloseTimeoutTests`): a consumer, a reader and a producer whose close command is never answered (a task that never completes and that ignores the token, like the real connection) are disposed within the operation timeout (500 ms in the test); with no limit the same three tests hang into their guard of 10 seconds (checked by setting the timeout of the test to zero). CHANGELOG: Fixed.
