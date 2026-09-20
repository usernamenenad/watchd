# Local PostgreSQL source

This compose environment is for development and integration tests only. It exposes PostgreSQL on `127.0.0.1:54329` with logical replication enabled.

## Roles

| Role | Password | Purpose |
| --- | --- | --- |
| `postgres` | `postgres` | Local administrator only |
| `watchd_app` | `watchd_app` | Writes to the sample projection |
| `watchd_replicator` | `watchd_replicator` | Reads the sample projection and opens logical replication slots |

## Sample projections

`tenant_permissions_projection` demonstrates the v0 contract:

- `(tenant_id, user_id)` is the stable primary key.
- `tenant_id` is the configured v0 scope key.
- `permissions` is the safe, consumer-facing projected state.

`value_encoding_projection` exercises PostgreSQL type classes beyond text and
jsonb - enum (`status`), array (`tags`), bytea (`payload`), boolean
(`is_active`), and numeric (`quantity`, `amount`) - so the value encoding
contract in [`docs/semantics.md`](../../docs/semantics.md) can be tested
against a real source, not just synthetic pgoutput messages.

`type_matrix_projection` is a wider, one-column-per-type fixture (integers,
`numeric`, floats, `timestamp`/`date`/`time`, `interval`, `inet`/`cidr`, an
array, `char(n)`, and a domain type) used to measure every type class's
actual decoded text directly, rather than assume it from a SQL `column::text`
cast - the two disagree for at least `boolean` (`"t"` vs `"true"`).

All three tables share the `watchd_publication` publication.

## Commands

```bash
make postgres-up
make postgres-logs
make postgres-down
```

`make postgres-down` removes the local volume, so the initialization script runs again on the next start.

