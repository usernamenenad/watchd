# PostgreSQL quickstart

Run watchd against the local PostgreSQL and watch a client's projection stay
fresh, go stale, and rebuild. You need Docker and Go.

## 1. Start PostgreSQL

```bash
make postgres-up
```

This starts PostgreSQL 17 on `127.0.0.1:54329` with logical replication, a
`tenant_permissions_projection` table, and the `watchd_publication`
publication (see `testing/postgres/init.sql`).

## 2. Start watchd

```bash
make build
WATCHD_DATABASE_URL=postgres://watchd_replicator:watchd_replicator@127.0.0.1:54329/watchd \
  ./bin/watchd -config examples/postgres/watchd.yaml
```

`watchd.json` is the same configuration in JSON; either works.

## 3. Start a client

In a second terminal:

```bash
go run ./examples/postgres/client -tenant 00000000-0000-0000-0000-000000000001
```

The client prints every state change:

```text
STALE  cursor=                         rows=0
FRESH  cursor=local@0/1AC1200          rows=0
```

The first client's snapshot creates watchd's replication slot (Bootstrap).
Clients for other tenants that start later take a snapshot against that
slot while it streams (Snapshot).

## 4. Change data

In a third terminal:

```bash
docker compose exec postgres psql -U postgres -d watchd -c "
  INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
  VALUES ('00000000-0000-0000-0000-000000000001',
          '00000000-0000-0000-0000-000000000101', '{\"role\": \"editor\"}')"
```

The client receives the change and is fresh at a new cursor:

```text
FRESH  cursor=local@0/1AC1300          rows=1
       user=00000000-0000-0000-0000-000000000101 permissions={"role": "editor"} version=1
```

## 5. Restart watchd

Stop watchd with Ctrl-C. The client turns `STALE` straight away: it no longer
has proof it is up to date. Change a row while watchd is down, then start
watchd again.

watchd resumes its slot where it stopped (Run). It keeps no history across
restarts, so it tells the client to resync. The client rebuilds from a new
snapshot, including the change made while watchd was down, and is `FRESH`
again:

```text
STALE  cursor=local@0/1AC1300          rows=1
STALE  cursor=                         rows=1
FRESH  cursor=local@0/1AC13B8          rows=1
       user=00000000-0000-0000-0000-000000000101 permissions={"role": "owner"} version=1
```

## Clean up

The slot keeps PostgreSQL WAL while watchd is stopped. Drop it when you are
done, and reset the database with `make postgres-down`:

```bash
docker compose exec postgres psql -U postgres -d watchd \
  -c "SELECT pg_drop_replication_slot('watchd_example')"
```
