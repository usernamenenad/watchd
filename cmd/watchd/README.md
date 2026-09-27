# watchd service

`watchd` serves the [Watch API](../../api/watch/v1/README.md) for one
PostgreSQL source.

```bash
make build
WATCHD_DATABASE_URL=postgres://watchd_replicator:watchd_replicator@127.0.0.1:54329/watchd \
  ./bin/watchd --config examples/postgres/watchd.json
```

## Configuration

A JSON or YAML file (`--config` or `-c`, default `watchd.json`); the extension
(`.json`, `.yaml`, or `.yml`) selects the format. Both forms have the same
fields and are validated identically: unknown fields and repeated keys are
rejected, so a mistyped setting fails loudly instead of being ignored.

```yaml
source_id: local
slot_name: watchd_example
publication_name: watchd_publication
listen_address: 127.0.0.1:7070
shutdown_timeout: 10s
ops:
  listen_address: 127.0.0.1:9090
projections:
  tenant_permissions:
    schema: public
    table: tenant_permissions_projection
    scope_column: tenant_id
    primary_key: [tenant_id, user_id]
```

The same configuration in JSON:

```json
{
  "source_id": "local",
  "slot_name": "watchd_example",
  "publication_name": "watchd_publication",
  "listen_address": "127.0.0.1:7070",
  "shutdown_timeout": "10s",
  "ops": {
    "listen_address": "127.0.0.1:9090"
  },
  "projections": {
    "tenant_permissions": {
      "schema": "public",
      "table": "tenant_permissions_projection",
      "scope_column": "tenant_id",
      "primary_key": ["tenant_id", "user_id"]
    }
  }
}
```

| Field | Meaning |
| --- | --- |
| `source_id` | Names the source. It is part of every client cursor, so changing it makes every client resync. |
| `slot_name` | The logical replication slot watchd owns. It is created when the first client asks for a scope, and resumed on every later start. |
| `publication_name` | The PostgreSQL publication to stream. It must publish `INSERT`, `UPDATE`, and `DELETE`, not `TRUNCATE`, and include every projection table. |
| `listen_address` | Where the gRPC API listens. |
| `shutdown_timeout` | How long a graceful shutdown may take. Default `10s`. |
| `ops.listen_address` | Where the ops HTTP endpoint listens: `/metrics`, `/healthz`, `/readyz`. Omit it to disable the endpoint. |
| `ops.pprof` | Serve `/debug/pprof/*` on the ops endpoint. Default `false`: profiles reveal internals. |
| `ops.mutex_profile_fraction`, `ops.block_profile_rate` | Sampling rates for the mutex and block profiles (see `runtime.SetMutexProfileFraction` and `runtime.SetBlockProfileRate`). Default `0`, which leaves those profiles empty. They require `ops.pprof`. |
| `retention.max_retained_wal_bytes` | The most WAL PostgreSQL may retain for the slot before watchd drops it. Default 1 GiB. The server's `max_slot_wal_keep_size` applies too, if it is stricter. |
| `retention.warn_fraction`, `retention.degrade_fraction` | Fractions of that budget where watchd logs a warning, then an error. Default `0.5` and `0.8`. |
| `retention.sample_interval` | How often retained WAL is checked. Default `30s`. |
| `projections` | The projections clients may watch, by name. `scope_column` must be one of `primary_key`. |

The database URL comes only from `WATCHD_DATABASE_URL`, so the file never
holds a credential. The role needs `REPLICATION` and `SELECT` on the
projection tables.

## Observability

Metrics, health, and profiling are described in
[docs/observability.md](../../docs/observability.md). In short: Prometheus
scrapes `/metrics` on the ops endpoint, and setting
`OTEL_METRICS_EXPORTER=otlp` also pushes to an OpenTelemetry collector
configured by the standard `OTEL_EXPORTER_OTLP_*` variables.

## Lifecycle

- **Startup:** validate the configuration, start the source (resume its slot,
  or wait for the first client to create it), serve the API, then report
  `SERVING` on the gRPC health service and ready on `/readyz`. The ops
  endpoint starts first, so `/healthz` answers throughout.
- **Shutdown** (SIGINT or SIGTERM): report `NOT_SERVING` and not ready, end
  every watch with `Resync`, drain the server within `shutdown_timeout`, then
  stop the source. Only transactions the process accepted are acknowledged to
  PostgreSQL, so the next start resumes exactly where this one stopped.
  Pending OTLP metrics are pushed within the same timeout.
- **Source failure** (for example an invalidated slot): the process shuts
  down the same way and exits with code 3.
- **Retention budget reached**: retained WAL grew to
  `retention.max_retained_wal_bytes`, because something held acknowledgement
  back. watchd drops its slot to protect PostgreSQL's disk, tells every
  client to resync, and exits with code 3. The next start builds a new slot,
  and every client rebuilds. See
  [the retention policy](../../docs/cdc-lifecycle.md#retention-policy).

| Exit code | Meaning |
| --- | --- |
| 0 | Clean shutdown |
| 1 | Internal error |
| 2 | Invalid configuration |
| 3 | The PostgreSQL source failed |

watchd is single-process soft state: after a restart, every client rebuilds
once from a snapshot. Running two processes against one slot is not
supported; the second waits for the slot and eventually exits with code 3.
The API has no authentication yet (#6): serve it only on a trusted network.
