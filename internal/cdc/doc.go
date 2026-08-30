// Package cdc captures committed PostgreSQL changes for watchd.
//
// It owns a logical-replication connection, converts pgoutput protocol
// messages into committed Transaction batches, and acknowledges source
// positions only after the local sink accepts them. Snapshot coordination,
// replay retention, network APIs, and consumer subscriptions belong to other
// package boundaries.
package cdc
