# Mark ported code with upstream references

## Context and Problem Statement

The port must stay maintainable as upstream changes. DotPulsar code has few comments, so the traceability markers
need to be minimal.

## Decision Outcome

Use three mechanisms:
- a single-line `// Upstream: repo@sha path` comment above each ported type;
- `// Upstream: Class#method` above non-trivial ported members and tests, with no line numbers;
- a machine-readable `upstream-map.yaml` holding line ranges and status.

The procedure is in [11-upstream-sync](../11-upstream-sync.md).

Risk: maintainers may ask for the comments to be removed. In that case, keep only `upstream-map.yaml`. The map is
authoritative either way.
