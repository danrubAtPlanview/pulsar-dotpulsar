# Raise the advertised protocol version from 14 to 19

## Context and Problem Statement

DotPulsar advertises protocol v14. Transactions need:
- ack receipts (`CommandAckResponse`), added in v17;
- `CommandTcClientConnectRequest`, added in v19. The Java client only sends it to brokers that report v19 or later.

## Decision Outcome

Raise `Constants.ProtocolVersion` to 19, in a dedicated work package (WP1), with the regression checklist from
[04-protocol](../04-protocol.md). The checklist covers every broker behaviour that changes for v15-v19 clients,
notably broker entry metadata (v18) and ack receipts (v17).
