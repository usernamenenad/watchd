# Integration tests

Run the local PostgreSQL source first, then execute:

```bash
make postgres-up
make integration
```

`bootstrap_test.go` proves the first correctness property: a row contained in an exported PostgreSQL snapshot is visible to the snapshot reader, and a row committed after that snapshot is visible in the logical replication stream. It is intentionally a bootstrap spike, not the completed watch runtime.

Future tests will cover decoding changes into a projection, transaction atomicity, duplicate delivery, bounded replay, service restarts, retention loss, and client resynchronization.
