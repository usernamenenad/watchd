# Watch API v1

This directory will contain the versioned, transport-neutral Watch API contract.

Planned concepts:

- `WatchRequest`: a source, a scope, and a resume cursor
- `Change`: a committed mutation for a key
- `Progress`: a statement that a scope is complete through a cursor
- `Resync`: an instruction to rebuild a projection from a snapshot
- `Snapshot`: a consistent source read and its resume cursor

The first implementation will choose a concrete wire format (likely protobuf/gRPC) only after the semantic contract is finalized.

