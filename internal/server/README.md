# Server

`Server` adapts the watch runtime (`internal/watch`) to the
[v1 gRPC API](../../api/watch/v1/README.md). It holds no state of its own:

- It translates each `watch.Event` into `WatchResponse` messages, packing
  snapshot rows into bounded messages and never splitting a `Batch`.
- It encodes cursors as `<source ID>@<position>`, so a client's cursor always
  names its source, and it refuses cursors of any other source.
- It maps errors to gRPC status codes. Unexpected errors are logged and
  reported as `INTERNAL` without detail.
- It serves the standard gRPC health service. The process sets it `SERVING`
  once the source has started, and `NOT_SERVING` when the source stops.

Authentication and authorization hooks are not implemented yet (issue #6):
until they are, serve only on a trusted network.
