# Local PostgreSQL source

This compose environment is for development and integration tests only. It exposes PostgreSQL on `127.0.0.1:54329` with logical replication enabled.

## Roles

| Role | Password | Purpose |
| --- | --- | --- |
| `postgres` | `postgres` | Local administrator only |
| `watchd_app` | `watchd_app` | Writes to the sample projection |
| `watchd_replicator` | `watchd_replicator` | Reads the sample projection and opens logical replication slots |

## Sample projection

`tenant_permissions_projection` demonstrates the v0 contract:

- `(tenant_id, user_id)` is the stable primary key.
- `tenant_id` is the configured v0 scope key.
- `permissions` is the safe, consumer-facing projected state.

The configured publication is named `watchd_publication`.

## Commands

```bash
make postgres-up
make postgres-logs
make postgres-down
```

`make postgres-down` removes the local volume, so the initialization script runs again on the next start.

