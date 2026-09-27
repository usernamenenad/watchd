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

The instruments for the ingest-to-serve path are added by #56.
