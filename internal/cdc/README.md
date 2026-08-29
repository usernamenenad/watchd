# CDC adapter

Future PostgreSQL logical-decoding adapter.

Responsibilities will include consistent bootstrap boundaries, transaction-aware decoding, cursor handling, and source-retention failures. This package must not expose partial transactions to the watch layer.

