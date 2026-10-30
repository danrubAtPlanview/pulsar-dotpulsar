# Raise the advertised protocol version from 14 to 19

## Context and Problem Statement

DotPulsar advertises protocol version 14 to the broker. Transactions need:

* acknowledgment receipts (`CommandAckResponse`), added in protocol version 17
* `CommandTcClientConnectRequest`, added in protocol version 19. The Java client only sends it to brokers that report version 19 or later

The broker changes some of its behavior depending on the protocol version a client advertises, for example it adds the broker entry metadata for version 18 and later.

## Decision Drivers

* We want to support transactions
* We do not want to change the behavior of existing users without noticing

## Considered Options

* Keep advertising protocol version 14 and send the new commands anyway
* Raise the advertised protocol version to 19

## Decision Outcome

Chosen option: "Raise the advertised protocol version to 19", because the commands that transactions need were introduced in protocol versions 17 and 19, and a client that sends them should advertise a version that includes them.
The changelog lists the change, because the broker may change some of its behavior for a client that advertises a newer version.

## Validation

Unit tests check that the client advertises version 19 and that the connection pool negotiates it. Integration tests against Pulsar 4.0.9 check the negotiated version and the acknowledgment receipts, and the integration tests that existed before run unchanged with the new version.

## Pros and Cons of the Options

### Keep advertising protocol version 14

* Good, because nothing changes for existing users
* Bad, because the client sends commands that are newer than the version it advertises

### Raise the advertised protocol version to 19

* Good, because the advertised version matches the commands that the client sends
* Bad, because the broker changes some of its behavior for a client that advertises a newer version
