# Observability

watchd exports OpenTelemetry metrics, serves health and readiness over HTTP, and can expose Go profiles. This document covers the ops endpoint, how to export metrics, the conventions every watchd instrument follows, and the metric catalogue.

## Ops endpoint

The ops endpoint is an HTTP server on its own listener, separate from the gRPC API, so a scrape or a probe never competes with watch streams. It is off unless `ops.listen_address` is set (see [the configuration](../cmd/watchd/README.md#configuration)).

| Path | Meaning |
| --- | --- |
| `/metrics` | Prometheus exposition of every metric below. |
| `/healthz` | Liveness: `200` while the process serves HTTP. It starts before the source, so it answers during startup. |
| `/readyz` | Readiness: `200` only while watchd serves its contract, else `503`. It is the same state as gRPC health `SERVING` and `watchd.serving`. |
| `/debug/pprof/*` | Go profiles, only when `ops.pprof` is `true`. |

The endpoint exposes no credentials, scopes, or row data. pprof is off by default because stack traces and command-line flags reveal internals. The mutex and block profiles stay empty unless `ops.mutex_profile_fraction` or `ops.block_profile_rate` is set. Both are sampled, so a value such as `5` (mutex) or `10000` (block, in nanoseconds) is cheap enough for a load test. Enable them to find lock contention.

## Exporting metrics

- **Prometheus (pull)**: scrape `/metrics`. It is served whenever the ops endpoint is on.
- **OTLP (push)**: set `OTEL_METRICS_EXPORTER=otlp`. The exporter reads the standard variables:

  | Variable | Meaning |
  | --- | --- |
  | `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | Collector address, for example `http://collector:4318`. |
  | `OTEL_EXPORTER_OTLP_PROTOCOL`, `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` | `http/protobuf` (default) or `grpc`. |
  | `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT`, and the other `OTEL_EXPORTER_OTLP_*` variables | As the OpenTelemetry specification defines them. |
  | `OTEL_METRIC_EXPORT_INTERVAL` | Push interval in milliseconds. Default `60000`. |

Both exporters can run at once. `OTEL_METRICS_EXPORTER` accepts `otlp`, `prometheus`, and `none`. Prometheus is always served on the ops endpoint rather than on a separate server, so `prometheus` and `none` only mean "no OTLP". An unknown exporter or protocol is a configuration error, and watchd exits with code 2.

A failing collector never affects CDC or serving. Export errors are logged at most once every 30 seconds. On shutdown, pending metrics are pushed within `shutdown_timeout`.

### Resource

Every metric carries these resource attributes. In Prometheus they appear on `target_info`.

| Attribute | Value |
| --- | --- |
| `service.name` | `watchd` |
| `service.version` | The module version stamped by the Go toolchain, or `dev`. |
| `service.instance.id` | A random UUID per process start. |
| `watchd.source.id` | The configured `source_id`. |

`OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` add to or override these, for example `OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=prod`.

## Conventions

Every watchd instrument follows these rules. `internal/telemetry` enforces the attribute rules in code.

- **Names** are `watchd.<component>.<name>`, lowercase, with dots between segments and underscores within one, for example `watchd.hub.replay.oldest_age`. Components are `cdc`, `hub`, `server`, and `sdk`. Process-wide metrics such as `watchd.serving` have no component. Prometheus sees dots as underscores and adds unit and `_total` suffixes: `watchd.cdc.reconnects` becomes `watchd_cdc_reconnects_total`.
- **Units** are [UCUM](https://ucum.org/): `s` for durations (always seconds, never milliseconds), `By` for bytes, and `{thing}` annotations for counts, such as `{transaction}`. A 0/1 state gauge has no unit.
- **Histogram boundaries** are chosen per instrument for its expected range, not left at the SDK default:
  - In-process latencies (decode, fan-out, send) span 50µs to 5s.
  - Round trips to PostgreSQL span 1ms to 30s.
  - Snapshots span 10ms to 10min.
  - Sizes follow powers of 4 up to their configured limit.
- **Attributes** come only from this allowlist:

  | Key | Values |
  | --- | --- |
  | `source` | The configured source ID. |
  | `projection` | A configured projection name. |
  | `reason` | A resync reason enum, such as `slow_watcher`. |
  | `error_class` | A CDC error class enum, such as `source_unavailable`. |
  | `state` | A state enum, such as `streaming`. |
  | `mode` | `bootstrap` or `snapshot`. |

  **Never** scope values, tenants, cursors or LSNs, principals, SQL, error messages, or row data. Each value comes from configuration or from a closed enum, so cardinality is bounded by configuration.

Enforcement:

- `telemetry.Attributes` builds every attribute set once, at construction, and panics on a key outside the allowlist. Measurements reuse pre-built sets, so the hot path neither allocates nor builds attributes from free strings.
- `telemetrytest.Reader.AssertAllowedAttributes` fails a test if any `watchd.*` metric carries a key outside the allowlist. Every instrumented package's tests call it.
- A component given no meter uses a no-op meter (`telemetry.MeterOrNoop`), so tests and embedders pay almost nothing.

Runtime and gRPC instrumentation keep their upstream OpenTelemetry names (`go.*`, `rpc.*`) and attributes, which are bounded by the Go runtime and by the API's method names.

## Catalogue

| Metric | Type | Unit | Attributes | Meaning |
| --- | --- | --- | --- | --- |
| `watchd.serving` | gauge | | | 1 while watchd serves its contract (gRPC health `SERVING`, `/readyz` ready), else 0. |
| `go.memory.used`, `go.memory.allocated`, `go.memory.allocations`, `go.memory.gc.goal`, `go.goroutine.count`, `go.processor.limit`, `go.config.gogc` | various | various | | Go runtime state, from the [runtime instrumentation](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/runtime). |
| `go.schedule.duration` | histogram | `s` | | How long runnable goroutines waited to be scheduled. Rising values mean CPU saturation. |
| `go.gc.pause.duration` | histogram | `s` | | GC stop-the-world pauses. |

Both runtime histograms are re-bucketed from the runtime's roughly 160 buckets onto 1µs–1s bounds. Their sums are estimated from bucket lower edges, because the runtime records none.

### CDC: replication, decode, and retention

These metrics come from `internal/cdc`. Every metric describes the process's one source, which the `watchd.source.id` resource attribute names, so none carries a `source` attribute.

| Metric | Type | Unit | Attributes | Meaning |
| --- | --- | --- | --- | --- |
| `watchd.cdc.stream.state` | gauge | | `state` | 1 for the replication stream's current state (`idle`, `connecting`, `streaming`, `backing_off`, `stopped`, `failed`), 0 for the others. |
| `watchd.cdc.lsn.received` | gauge | `By` | | WAL position of the newest message received. |
| `watchd.cdc.lsn.acknowledged` | gauge | `By` | | WAL position last acknowledged to PostgreSQL. |
| `watchd.cdc.lsn.server_wal_end` | gauge | `By` | | PostgreSQL's WAL end, as last reported on the stream (keepalives and WAL data). |
| `watchd.cdc.wal_lag` | gauge | `By` | | `server_wal_end − received`: how far PostgreSQL is ahead of watchd. It grows when PostgreSQL produces WAL faster than watchd reads it, or when decoding lags. |
| `watchd.cdc.ack_lag` | gauge | `By` | | `received − acknowledged`: work received but not yet accepted by the hub. A slow watcher never causes it, because the hub drops slow watchers instead of blocking. |
| `watchd.cdc.reconnects` | counter | `{reconnect}` | | Reconnect attempts after a retryable failure. |
| `watchd.cdc.stream.errors` | counter | `{error}` | `error_class` | Replication connections that ended in an error. |
| `watchd.cdc.standby_status.duration` | histogram | `s` | | Time to send one acknowledgement. The protocol has no keepalive round trip to measure. |
| `watchd.cdc.standby_status.interval` | histogram | `s` | | Time between acknowledgements. |
| `watchd.cdc.messages` | counter | `{message}` | | pgoutput messages decoded. |
| `watchd.cdc.wal.bytes` | counter | `By` | | pgoutput payload bytes decoded. |
| `watchd.cdc.transactions` | counter | `{transaction}` | | Transactions accepted and acknowledged. |
| `watchd.cdc.transaction.duration` | histogram | `s` | | From a transaction's BEGIN to its COMMIT being received and decoded. For a large transaction this includes streaming it from PostgreSQL. |
| `watchd.cdc.transaction.changes` | histogram | `{change}` | | Row changes per transaction. The top bucket is `MaxTransactionChanges`. |
| `watchd.cdc.transaction.size` | histogram | `By` | | Estimated in-memory size per transaction. The top bucket is `MaxTransactionBytes`. |
| `watchd.cdc.decode.errors` | counter | `{error}` | | Messages that could not be decoded. |
| `watchd.cdc.sink.duration` | histogram | `s` | | Time the hub takes to accept a transaction. Replication waits for it before acknowledging, so this is watchd's backpressure point. |
| `watchd.cdc.commit_to_accept` | histogram | `s` | | From commit in PostgreSQL to acceptance by the hub. Subject to clock skew between PostgreSQL and watchd. |
| `watchd.cdc.slot.retained_wal` | gauge | `By` | | WAL PostgreSQL retains for the slot: its current WAL position minus the slot's `restart_lsn`. |
| `watchd.cdc.slot.safe_wal_size` | gauge | `By` | | WAL that can still be written before the slot is invalidated. Absent when the server's retention is unbounded. |
| `watchd.cdc.slot.wal_status` | gauge | | `state` | 1 for the slot's `wal_status` (`reserved`, `extended`, `unreserved`, `lost`), 0 for the others. |
| `watchd.cdc.retention.budget` | gauge | `By` | | The effective retained-WAL budget: the stricter of `MaxRetainedWALBytes` and `max_slot_wal_keep_size`. |
| `watchd.cdc.retention.state` | gauge | | `state` | 1 for the retention policy's current state (`ok`, `warn`, `degrade`, `terminal`), 0 for the others. See [the retention policy](cdc-lifecycle.md#retention-policy). |
| `watchd.cdc.retention.transitions` | counter | `{transition}` | `state` | Retention policy transitions, by the state entered. |

The slot and retention gauges are sampled from `pg_replication_slots` when the stream starts and then every 30 seconds, on a short-lived connection. They are absent until the first sample. `error_class` is one of:

- `retention_budget_exceeded`, `slot_invalidated`, `slot_in_use`, `slot_not_found`, `snapshot_window_closed`
- `insufficient_privileges`, `source_unavailable`, `replication_ended`, `sink_rejected`
- `transaction_too_large`, `malformed_data`, `unsupported_change`
- `invalid_config`, `postgres_server`, `timeout`, `other`

### CDC: scope reads

| Metric | Type | Unit | Attributes | Meaning |
| --- | --- | --- | --- | --- |
| `watchd.cdc.snapshot.duration` | histogram | `s` | `mode` | Time to read one scope, for reads that succeed. `mode` is `bootstrap` (the read that creates the slot) or `snapshot`. |
| `watchd.cdc.snapshot.page.duration` | histogram | `s` | `mode` | Time to fetch one keyset page (`SnapshotBatchRows` rows). |
| `watchd.cdc.snapshot.rows` | counter | `{row}` | `mode` | Rows read. |
| `watchd.cdc.snapshot.bytes` | counter | `By` | `mode` | Text bytes of rows read. |
| `watchd.cdc.snapshot.active` | up-down counter | `{snapshot}` | `mode` | Scope reads in progress. |
| `watchd.cdc.snapshot.errors` | counter | `{error}` | `mode`, `error_class` | Scope reads that failed. |

### Hub: fan-out, replay, and watchers

These metrics come from `internal/watch`. Per-watcher values are histograms sampled across all watchers, never series per watcher, so cardinality does not grow with watchers or scopes.

| Metric | Type | Unit | Attributes | Meaning |
| --- | --- | --- | --- | --- |
| `watchd.hub.accept.duration` | histogram | `s` | | From a transaction reaching the hub until every relevant watcher has it queued, including the lock wait. Routing scans every watcher, so this grows with the watcher count. |
| `watchd.hub.lock.wait` | histogram | `s` | | Time `Accept` waited for the hub's lock, held meanwhile by watchers registering, resuming, or reporting progress. `accept.duration − lock.wait` is fan-out work. |
| `watchd.hub.replay.occupancy` | gauge | `{transaction}` | | Transactions in the replay window. |
| `watchd.hub.replay.capacity` | gauge | `{transaction}` | | `MaxReplayTransactions`. |
| `watchd.hub.replay.oldest_age` | gauge | `s` | | Age of the oldest transaction in the window, from its commit time: how far back a client can resume without a snapshot. |
| `watchd.hub.replay.evictions` | counter | `{transaction}` | | Transactions evicted from the window. |
| `watchd.hub.watchers` | gauge | `{watcher}` | | Registered watchers. |
| `watchd.hub.watcher.queue_depth` | histogram | `{transaction}` | | Transactions still queued for a watcher each time it takes one. Near `WatcherQueue`, the watcher is about to be dropped. |
| `watchd.hub.watcher.lag` | histogram | `By` | | WAL between the hub's newest transaction and each transaction a watcher takes from its queue. |
| `watchd.hub.progress` | counter | `{event}` | | Progress events sent. |
| `watchd.hub.resyncs` | counter | `{event}` | `reason` | Watches ended with Resync: `cursor_unavailable`, `slow_watcher` (a slow-watcher disconnect), `snapshot_window_closed`, or `source_stopped`. |

### Server: the gRPC API

| Metric | Type | Unit | Attributes | Meaning |
| --- | --- | --- | --- | --- |
| `watchd.server.streams.active` | up-down counter | `{stream}` | | Watch streams in progress. |
| `watchd.server.send.duration` | histogram | `s` | | Time one stream message took to send. Long sends mean a slow reader: gRPC flow control holds the send until the client catches up. |
| `watchd.server.snapshot.bytes` | counter | `By` | | Encoded bytes of snapshot rows sent. |
| `watchd.server.batch.commit_to_send` | histogram | `s` | | From commit in PostgreSQL to the batch being sent. Subject to clock skew. |
| `rpc.server.call.duration` | histogram | `s` | upstream: `rpc.method`, `rpc.response.status_code`, `rpc.system.name` | Per-RPC duration from the [OpenTelemetry gRPC instrumentation](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc). A `Watch` call lasts as long as its stream. Only registered methods are recorded, so a client cannot add `rpc.method` values. |

Every `Batch` on the wire also carries `commit_time`, so clients can measure commit to apply themselves.

### Go SDK

These metrics come from `sdk/go/watchd`, recorded in the client application through the meter it passes as `SyncConfig.Meter`. They carry the `projection` name and never the scope, so several syncs of one projection share their series.

| Metric | Type | Unit | Attributes | Meaning |
| --- | --- | --- | --- | --- |
| `watchd.sdk.commit_to_apply` | histogram | `s` | `projection` | From commit in PostgreSQL to the batch applied to the store: the end-to-end latency. Subject to clock skew between PostgreSQL and the client. |
| `watchd.sdk.apply.duration` | histogram | `s` | `projection` | Time `Store.Apply` took for one batch. |
| `watchd.sdk.snapshot.install.duration` | histogram | `s` | `projection` | From `SnapshotBegin` to the snapshot committed to the store. |
| `watchd.sdk.time_to_fresh` | histogram | `s` | `projection` | From opening a stream, after a start, reconnect, or resync, to the first `Progress`. |
| `watchd.sdk.syncs` | up-down counter | `{sync}` | `projection` | `Sync` calls running. |
| `watchd.sdk.fresh` | up-down counter | `{sync}` | `projection` | `Sync` calls whose projection is fresh. `syncs − fresh` are stale. |
| `watchd.sdk.resyncs` | counter | `{event}` | `projection`, `reason` | Resyncs the server asked for. |
| `watchd.sdk.stream.errors` | counter | `{error}` | `projection`, `error_class` | Streams that ended in an error. `error_class` is the lowercase gRPC status code, such as `unavailable`. |

## Where time goes

Read the stages in order to locate latency or a bottleneck:

1. **PostgreSQL → watchd**: `watchd.cdc.wal_lag` and `watchd.cdc.transaction.duration`.
2. **Accepting into the hub**: `watchd.cdc.sink.duration`, split into `watchd.hub.lock.wait` and fan-out work in `watchd.hub.accept.duration`. Replication waits on this.
3. **Hub → stream**: `watchd.hub.watcher.queue_depth` and `watchd.hub.watcher.lag`.
4. **Stream → client**: `watchd.server.send.duration`. Long sends mean a slow reader.
5. **Client apply**: `watchd.sdk.apply.duration`.

End to end, the latency is `watchd.cdc.commit_to_accept`, then `watchd.server.batch.commit_to_send`, then `watchd.sdk.commit_to_apply`. For CPU, allocation, and lock contention, profile through `/debug/pprof` (see [Ops endpoint](#ops-endpoint)). Hot paths found while instrumenting are tracked in #68, #69, and #70.

## Overhead

Instrumentation adds no allocations on the hot path: each instrument and attribute set is created once, and counts are added once per transaction, not once per change. `BenchmarkConsumeTransaction` in `internal/cdc` measures the reader's per-transaction path (14 pgoutput messages: relation, BEGIN, 10 inserts, COMMIT; decode, sink, acknowledge):

| Meter | Time | Allocations |
| --- | --- | --- |
| No-op | ~13.9µs | 301 allocs, 13.3kB |
| SDK | ~16.3µs | 301 allocs, 13.3kB |

The instrumented path costs about 2µs per transaction, mostly histogram recording and clock reads, and allocates nothing. The allocations are decoding's own. Measured with Go 1.27 on a 16-thread x86-64 machine. Run `go test ./internal/cdc -run '^$' -bench ConsumeTransaction -benchmem`.

`BenchmarkHubAccept` in `internal/watch` measures fanning one transaction out to N watchers:

| Watchers | No-op meter | SDK meter | Allocations |
| --- | --- | --- | --- |
| 1 | ~310ns | ~480ns | 0 |
| 100 | ~3.1µs | ~3.4µs | 0 |
| 1,000 | ~28.6µs | ~29.2µs | 0 |

The instruments cost a constant ~170ns per transaction: two histogram records and three clock reads. Routing itself is linear in watchers, because every watcher's scope is checked against every transaction. That is the first hot path to optimize under #57.

The SDK instruments add two histogram records and two clock reads per batch applied.
