# CDC adapter

This package contains the PostgreSQL logical-decoding building blocks.

`Decoder` accepts decoded `pgoutput` messages, remembers table metadata, buffers row mutations between `BEGIN` and `COMMIT`, and emits a `Transaction` only after commit. It does not open a database connection yet; a later reader will feed raw replication messages into it.

Responsibilities still to implement: replication connection lifecycle, raw message parsing, slot status acknowledgements, bootstrap coordination, cursor persistence, and source-retention failures.
