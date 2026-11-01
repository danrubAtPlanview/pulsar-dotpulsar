# Port only the v4 (PIP-31) transaction path

## Context and Problem Statement

Upstream master contains two transaction paths:
- v4: coordinator discovery via the `transaction_coordinator_assign` topic.
- v5 (PIP-473): metadata-driven discovery for scalable topics, enabled only by the internal v5 SDK.

## Decision Outcome

Port only v4. DotPulsar has no scalable-topics support, and v5 is internal and evolving. All `scalable` proto fields
stay unset, `WatchTcAssignmentsDiscovery` is not ported, and the `V5*` tests are excluded. `ITcDiscovery` is kept as
an abstraction so that v5 can be added later.
