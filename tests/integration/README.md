# Integration tests

Run the local PostgreSQL source first, then execute:

```bash
make postgres-up
make integration
```

The bootstrap tests prove that snapshot plus stream exactly matches PostgreSQL
across writes before, during, and after snapshot reading. They also cover
cancellation cleanup, expired snapshots, invalid configuration, and database
permission failures. The watch runtime remains intentionally outside these
tests.

Future tests will cover decoding changes into a projection, transaction atomicity, duplicate delivery, bounded replay, service restarts, retention loss, and client resynchronization.
