// Package cdc captures committed PostgreSQL changes for watchd.
//
// It owns a logical-replication connection, converts pgoutput protocol
// messages into committed Transaction batches, and will eventually coordinate
// source snapshots and replication-slot acknowledgements. It deliberately does
// not expose a network API or manage consumer subscriptions.
package cdc
