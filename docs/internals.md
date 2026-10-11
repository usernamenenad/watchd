# Internals: a guided tour of the code

[Architecture](architecture.md) says what each package is for. This document opens every package and walks through its files, its important functions, and **why** they are written the way they are. Read it top to bottom once. After that, use it as a map when you open a file.

It assumes you have read [the CDC lifecycle](cdc-lifecycle.md), which explains the PostgreSQL words used here (WAL, LSN, slot, publication, MVCC snapshot) from zero, and [the semantics](semantics.md), which is the contract all of this code exists to keep.

## The one idea everything serves

A client wants a local copy of some rows, for example "all permission rows of tenant `acme`". It also wants to know whether that copy is **complete up to some point**, or possibly missing something.

PostgreSQL already records every committed change in its WAL. watchd reads that log, cuts it into committed transactions, and fans each transaction out to the clients that care about it. Every design decision below protects one of three promises:

1. **No gap.** Every committed change reaches the client, either in the initial snapshot or in the stream after it. Changes may arrive twice, but they are never lost.
2. **No lie about freshness.** watchd says "you are complete up to cursor X" (`Progress`) only when that is true.
3. **No harm to PostgreSQL.** A slow client or a stuck watchd must never make PostgreSQL run out of disk.

When code looks more careful than seems necessary, it is usually protecting one of these.

## Following one row from end to end

Before going package by package, here is what happens to one `INSERT`.

1. A producer runs `INSERT INTO permissions ... ; COMMIT;`. PostgreSQL writes the commit to its WAL.
2. PostgreSQL's walsender reads the WAL for watchd's **replication slot**. It decodes the WAL with the `pgoutput` plugin into messages (`Relation`, `Begin`, `Insert`, `Commit`) and sends them over the replication connection.
3. `cdc.Reader.receive` (`internal/cdc/stream.go`) receives each message. `consumeCopyData` parses it with `pglogrepl` and passes it to `cdc.Decoder.Consume` (`internal/cdc/decoder.go`).
4. The decoder buffers changes between `Begin` and `Commit`. At `Commit` it returns one `cdc.Transaction`.
5. The reader passes the transaction to its **sink**, `watch.Hub.Accept` (`internal/watch/hub.go`). The hub keeps it in a replay ring, then puts each watcher's part of it on that watcher's queue.
6. Only after `Accept` returns nil does the reader **acknowledge** the transaction's end LSN to PostgreSQL. PostgreSQL may then discard that WAL.
7. Each watcher's goroutine (`Hub.live`) takes the transaction off its queue and calls `send`. In `internal/server/server.go`, `send` converts it to a protobuf `Batch` and writes it to the gRPC stream.
8. The Go SDK (`sdk/go/watchd/client.go`) receives the `Batch` and calls `Store.Apply`. When a `Progress` follows, it saves the cursor and reports the projection as fresh.

Steps 5 and 6 come in that order on purpose. If watchd crashed between them, PostgreSQL would resend the transaction. That is why delivery is **at-least-once**, and why every consumer must apply changes idempotently.

---

## `cmd/watchd` — the executable

`main.go` is deliberately thin. It holds no logic that a test would want to call.

- **`run()`**
  - Parses `-c/--config`.
  - Builds a JSON `slog` logger.
  - Loads the configuration with `daemon.LoadConfig`.
  - Turns SIGINT/SIGTERM into context cancellation (`signal.NotifyContext`).
  - Calls `daemon.Run`.
- **Exit codes.** The returned error becomes an exit code: 0 clean, 2 configuration, 3 source, 1 anything else. *Why distinct codes:* an orchestrator such as Kubernetes or systemd, or an operator, can tell "fix your config" from "PostgreSQL is broken" from "a bug" without parsing logs. The code uses `errors.Is` against the sentinel errors `daemon.ErrInvalidConfig` and `daemon.ErrSource` to decide.
- **The database URL comes only from `WATCHD_DATABASE_URL`, never from the config file.** *Why:* config files get committed, copied into ConfigMaps, and pasted into issues. Credentials belong in a secret-backed environment variable.

## `internal/daemon` — assembling one process

### `config.go`

- **`Config`, `ProjectionConfig`, `OpsConfig`, `RetentionConfig`.** The file format, as JSON-tagged structs. Each field's comment states its default.
- **`LoadConfig`.** Picks JSON or YAML by file extension.
- **`ParseYAMLConfig`.** Converts YAML to JSON first, using `yaml.YAMLToJSONStrict`, then calls `ParseJSONConfig`. *Why:* one decoder and one validator means the two formats can never disagree about what is valid.
- **`ParseJSONConfig`**
  - Uses `DisallowUnknownFields` and rejects repeated keys. It detects repeats by running the strict YAML parser over the JSON, because `encoding/json` silently keeps the last duplicate.
  - Rejects trailing data.
  - *Why so strict:* a typo such as `shutdown_timout` would otherwise be ignored, and the operator would believe a setting is active when it isn't.
- **`validate`.** Validates by **constructing the real components** (`cdc.NewReader`, `cdc.ValidateProjectionSpec`, `server.New`) rather than duplicating their rules. *Why:* each package stays the single owner of its own rules. The daemon only translates errors into `ErrInvalidConfig`.
- **`readerConfig`, `projectionSpecs`, `hubConfig`.** Translate the file format into each package's own config type. *Why:* the packages don't know about JSON or files, so they stay usable in tests and in other binaries.
- **`noWatcher`.** A stub that lets `validate` build a `server.Server` without a hub.

### `daemon.go`

- **`Run`.** Opens the listeners, then calls `Serve`. *Why split:* tests call `Serve` with listeners on port 0.
- **`Serve`.** This is the startup and shutdown choreography, and the order matters.

  Startup:

  1. Telemetry starts first, and its flush is deferred, so even a failed start pushes metrics.
  2. The source, hub, and server are created. The source's sink is a closure over `hub`, and the hub's snapshotter is the source, so the two are wired to each other: the closure captures the `hub` variable before it is assigned.
  3. The ops endpoint starts **before** the source. *Why:* `/healthz` must answer during a slow start, for example while waiting on the slot, or Kubernetes would kill the pod.
  4. `source.Start` runs. Then the gRPC server starts serving, and only then is `setServing(true)` called. Health and `/readyz` flip to serving at the same moment.

  Shutdown, in reverse:

  1. `setServing(false)`.
  2. `hub.Stop()` ends every watch with `Resync`, so every client knows to rebuild.
  3. `GracefulStop`, bounded by `ShutdownTimeout`, then a hard `Stop`.
  4. Stop the source.
  5. Stop the ops endpoint.

  *Why the source outlives the server:* the source's context is `context.WithoutCancel(ctx)`, so it keeps streaming and acknowledging while the server drains. It only acknowledges what the hub accepted, so nothing is ever acknowledged without being delivered.
- **`registerServingGauge`.** One `atomic.Bool` backs gRPC health, `/readyz`, and the `watchd.serving` metric, so the three can never disagree.
- **`startOps`, `stopOps`.** A failing ops endpoint is only logged; it never stops watchd. Mutex and block profiling rates are set only when pprof is enabled.
- **`newGRPCServer`.** Adds OpenTelemetry gRPC metrics, with a filter that records only **registered** methods. *Why:* the method name comes from the client's request, so recording unknown methods would let any client create unlimited metric series.

## `internal/ops` — the operational HTTP endpoint

`ops.go` defines `Handler`, which serves:

- `/metrics`, the Prometheus exposition.
- `/healthz`, which answers 200 whenever the process can answer HTTP at all.
- `/readyz`, which answers 200 only while `Ready()` is true.
- `/debug/pprof/*`, only when enabled.

*Why a separate listener from the API:* metrics scrapes and profiling must never compete with, or be exposed alongside, the client API. *Why pprof is off by default:* it reveals stack traces and command lines.

## `internal/telemetry` — the metrics pipeline

- **`telemetry.go`.**
  - `Setup` builds one OpenTelemetry `MeterProvider` with:
    - a Prometheus pull exporter, served by the ops endpoint;
    - an optional OTLP push exporter, enabled with the standard `OTEL_METRICS_EXPORTER=otlp` environment variable;
    - Go runtime metrics.
  - `MeterOrNoop` lets every component accept a nil meter and record nothing at almost no cost.
  - Export errors are rate-limited (`errorHandler`, at most one log every 30s), so an unreachable collector cannot flood the log.
- **`attrs.go`.** This is the cardinality guard. Only six attribute keys may appear on any watchd metric: `source`, `projection`, `reason`, `error_class`, `state`, and `mode`.
  - `Attributes` **panics** on any other key.
  - *Why:* a metric labelled by tenant, scope, or cursor would grow without bound and leak customer data into monitoring. Components build their attribute sets once, at construction, so recording a metric never allocates.
- **`runtime.go`.** Emits Go scheduler latency and GC pause histograms, which the standard runtime instrumentation lacks. It re-buckets the runtime's roughly 160 buckets down to 13, so scrapes stay small.
- **`telemetrytest/`.** A test helper for asserting on recorded metrics.

---

## `internal/cdc` — PostgreSQL in, committed transactions out

This is the largest and most delicate package. Only this package knows about PostgreSQL. Everything above it sees only `Transaction`, `Change`, `Cursor`, and `Snapshot`.

### `doc.go`, `cdc.go` — the replication connection

- **`Connect`.** Opens a `pgconn` connection with `replication=database` added. *Why add it here:* operators supply one normal PostgreSQL URL, and watchd derives both connection kinds from it.
- **Malformed URLs.** On a parse failure, `Connect` returns `ErrInvalidDatabaseURL` **without wrapping** the parser's error, because the parser's message may contain the password. You'll see this "never leak the URL" rule in several places.
- **`CDC`.** A small wrapper around the connection with an idempotent `Close`.

### `connections.go`

watchd uses two kinds of connection:

- **Replication connection.** Speaks the streaming protocol: it creates slots and receives WAL.
- **Management connection.** Ordinary SQL: it validates publications, reads rows, and reads slot state.

`connectManagement` **removes** `replication` from the URL's parameters. Every connect is bounded by `ConnectionTimeout`. Errors pass through `classifyConnectionError` (see `errors.go`).

### `config.go`

- **`ReaderConfig`.** All tunables, each documented.
- **`normalizeReaderConfig`.** Fills defaults for zero values. *Why:* callers can set only what they care about.
- **`validateReaderConfig`.** Rejects anything unsafe:
  - Slot and publication names must match `postgresIdentifier`, `^[a-z_][a-z0-9_$]{0,62}$`. *Why:* the publication name is spliced into the `START_REPLICATION` arguments as a string. The regexp guarantees it cannot break out of its quotes.
  - Retention fractions must satisfy `0 < warn < degrade < 1`.
  - `MaxValueBytes` must not exceed `MaxTransactionBytes`.

### `types.go` — the vocabulary every other package uses

- **`Change`**
  - `Operation` and `Table` (`schema.table`).
  - `Key map[string]string`: the replica-identity columns. Always text, never NULL.
  - `Columns []string` and `Values []Value`: parallel slices in table column order; nil for a delete. `Columns` is **shared** by every change of the same relation and must not be modified.
  - `Change.Value(name)` looks up one column by name.
- **Why `Key` and `Values` overlap.** For an insert, the key columns also appear in `Values`, so the data looks duplicated. It isn't redundant:
  - `Key` is the row's *identity*, guaranteed complete and non-null.
  - `Values` is its *state*, which can contain NULLs and omitted TOAST values. For a delete, it is absent entirely.

  Routing (`hub.go`) and applying (`store.go`) need the identity for every operation. Delete carries only the key.
- **`Value`, `ValueKind`.** A column value is exactly one of three kinds:
  - `ValueNull`: SQL NULL.
  - `ValueText`: the PostgreSQL text form.
  - `ValueUnchangedToast`: a large column an UPDATE didn't resend. The consumer keeps its old value.

  *Why a struct instead of `any`:* storing a string in an `any` allocates, and three explicit kinds can't be confused. With `any`, nil versus `""` versus a sentinel would be easy to mix up.
- **`Transaction`**
  - `Cursor`: the position just after the commit.
  - `Changes`.
  - `CommitTime`: informational only.
  - Two private fields: `commitLSN` (where the commit record starts) and `xid` (the source transaction ID).
  - `After(c)` reports whether a consumer resuming from `c` still needs this transaction. It uses `commitLSN >= c`, which is PostgreSQL's own rule for a replication start position.
- **`Cursor`.** A source ID plus an LSN, with every field private.
  - `Compare` refuses to compare cursors from different sources.
  - `BytesAfter` measures lag. It is never used for ordering.
  - `String` and `ParseCursor` round-trip the persisted form.
  - *Why opaque:* clients must not do arithmetic on positions. Keeping the fields private means the representation can change later.
- **`MinCursor`.** The "consistent cut" across several scopes of one source.
- **`ProjectionSpec`, `Scope`.** Trusted configuration. A client can only name a projection; it can never send SQL.
- **`Snapshot` and `Snapshot.Covers(t)`.** The heart of the no-gap promise. A snapshot remembers the MVCC visibility (`xmin:xmax:in-progress`) of the read that produced its rows. `Covers` decides **by transaction ID** whether a streamed transaction's effects are already in those rows.
  - *Why not compare LSNs:* a commit can be written to WAL before it becomes visible to new snapshots, for example while waiting on a synchronous standby. Comparing positions would then give the wrong answer. Visibility is exact.
- **`TransactionSink`, `SnapshotRowSink`.** Function types for "where decoded data goes". Neither is an interface: a function is the smallest contract, and tests can pass a closure.
- **`qualifiedTable`.** Joins `schema.table` with string concatenation. *Why not `fmt.Sprintf`:* it is cheaper, and it now runs once per relation instead of once per change.

### `decoder.go` — pgoutput messages to transactions

The `Decoder` is a state machine fed one message at a time. It never touches the network, which is why its unit tests need no PostgreSQL.

- **State.**
  - `relations`: relation ID → `relationInfo`.
  - `pending`: the open transaction's changes. A **nil** `pending` means no transaction is open.
  - The open transaction's xid and commit time.
  - Byte and count accounting.
  - Limits.
- **`relationInfo` and `newRelationInfo`.**
  - PostgreSQL sends a `Relation` message (table name and column list) before the first row of a table, and again whenever the table changes.
  - The decoder precomputes, once per relation: the qualified table name, the column-name slice, and the positions of the key columns (columns with `Flags == 1`).
  - *Why:* the alternative is redoing that work for every row.
- **`Consume`** handles each message type:
  - `Relation`: replaces the `relationInfo`. Changes already emitted keep their old `Columns` slice, which is why that slice is never modified in place.
  - `Begin`: opens `pending`, preallocated from the previous transaction's size (`pendingCapacityHint`, capped at 1024). A nested Begin is an error.
  - `Insert`, `Update`, `Delete`: decode one change and `append` it.
  - `Commit`: hands `pending` to a new `Transaction` **without copying** and resets state. The `Cursor` carries no source ID yet; the reader stamps it.
  - `Type`, `Origin`: ignored, because they never change a projection.
  - **Anything else is an error.** That includes TRUNCATE and logical-decoding messages. *Why:* silently ignoring a TRUNCATE would leave every client's projection wrong forever. Failing loudly is the safe default.
- **`append`.** Enforces `MaxTransactionChanges` and `MaxTransactionBytes` while the transaction is still open. *Why:* without a bound, one huge source transaction could exhaust watchd's memory before its commit even arrived.
- **`insert`, `update`, `delete`.**
  - `insert` decodes the new row and derives the key from it.
  - `delete` derives the key from the old-key tuple. It never has values.
  - `update` decodes the new row. If PostgreSQL also sent the old key (`'K'`) or the whole old row (`'O'`, under `REPLICA IDENTITY FULL`), it validates that and compares it with the new key. A difference returns `ErrPrimaryKeyChangeUnsupported`. *Why reject:* a v0 `Change` has one key. Reporting a key change as an update would leave the old row behind in every projection.
- **`decodeRow`.** Validates every column, then copies **all of the row's text into one string** and slices each `Value.Text` out of it.
  - *Why:* one allocation per row instead of one per column.
  - *Trade-off, documented in the code:* a retained substring keeps the whole row's string alive.
- **`checkRow`, `checkColumn`, `checkKeyColumn`.** Enforce the value contract:
  - Only the text, null, and unchanged-TOAST encodings are accepted. Binary is rejected with `ErrUnsupportedColumnEncoding`.
  - Values are capped at `MaxValueBytes`.
  - Key columns must be non-empty text.
- **`keyFromValues`, `keyFromTuple`, `keyColumn`, `checkKeyTuple`.**
  - These build the `Key` map from the key positions, either from already-decoded values or from an old tuple.
  - Old tuples come in two shapes: key-only, or a full row. `keyColumn` hides the difference.
- **`estimatedChangeBytes`, `estimatedValueBytes`.** A cheap, deterministic size estimate used for the transaction byte bound. It isn't exact memory use, but it is stable, so the limit behaves predictably.

### `stream.go` — the streaming loop

- **`Run`**
  - Reconnects with bounded exponential backoff and jitter (`retryDelay`).
  - Starts the retention monitor (`retention.go`) beside the stream.
  - Uses `context.WithCancelCause` so the monitor can stop the stream with a *reason*. `Run` checks that reason to tell "retention budget hit" apart from a cancelled context.
  - Context cancellation counts as a clean shutdown and returns nil.
  - Errors pass through `isRetryable`. Malformed data, oversized transactions, an invalidated slot, and the like are terminal. *Why:* retrying them would fail the same way forever.
- **`read`.** Owns exactly one connection and creates a **new decoder** for it. *Why:* after a reconnect, PostgreSQL resends `Relation` messages and restarts any partly sent transaction, so old decoder state would be wrong.
  - If `Bootstrap` just handed over a stream (`takeBootstrapStream`), `read` uses it. Otherwise it connects and runs `requireSlot`, then `startReplication`.
- **`startReplication`.** Requests `proto_version '1'` and the publication. It deliberately **does not** request the `binary` option, so every value arrives as text (see the semantics doc).
- **`receive`.** Loops on `ReceiveMessage` with a deadline. When the deadline passes on an idle stream, it sends a standby status update. *Why:* PostgreSQL disconnects a replication client that stays silent too long.
- **`consumeCopyData`.** The core of the reader.
  - For a `'w'` (WAL data) message:
    1. Parse the message and decode it.
    2. At Commit, stamp the source ID on the cursor.
    3. Call the **sink**.
    4. Only if the sink returns nil, set `safeLSN = TransactionEndLSN` and acknowledge.

    *Why `TransactionEndLSN` and not the server's WAL end:* the server may already be ahead. Acknowledging past what the hub accepted would lose data on a crash.
  - For a `'k'` (keepalive) message: between transactions, `safeLSN` advances to the server's WAL end.
    - *Why:* if the published tables are idle while other tables are busy, no commit of ours ever advances `safeLSN`, and the slot would retain WAL forever.
    - *Why only between transactions:* inside one, the open transaction isn't accepted yet.
  - Any other discriminator is an error. Unknown protocol data is never skipped.
- **`acknowledge`, `observeServerWALEnd`.** Send the status update and record metrics. `observeServerWALEnd` uses compare-and-swap so the value never moves backwards across a reconnect.

### `reader.go` — the `Reader` object and its statistics

- **`Reader`.** Holds config, the sink, and injectable functions (`connect`, `sendStandbyStatus`, `wait`, `now`, `random`). *Why injectable:* unit tests drive reconnects, time, and jitter without a network or real clocks.
- **Statistics.** Values that change on every message use atomics. The two rarely-changing states (connection and retention) share a small mutex.
  - *Why:* the streaming path takes no lock.
  - LSNs are stored as numbers and formatted only when `Stats()` is called.
  - `lsnStat` keeps a separate "set" flag because LSN `0/0` is a real value, distinct from "never set".
- **`NewReader`.** Normalizes and validates config, and creates metric instruments once. It opens no connections.

### `source.go` — deciding which primitive each moment needs

A `Source` wraps one `Reader` and its single slot. The reader has three primitives, and `Source` picks among them:

| Situation | Primitive |
|---|---|
| Slot exists at start | `Run` (resume streaming) |
| No slot, first client arrives | `Bootstrap` (create slot + snapshot), then `Run` |
| Slot exists, another client arrives | `Reader.Snapshot` (snapshot against the existing slot) |

- **`Start`.** If a slot exists, streaming resumes immediately. If not, the source **waits for the first scope** instead of creating an empty slot. *Why:* a slot created without a snapshot has no snapshot paired with its starting point.
- **`Snapshot`.** Holds `mu` across `Bootstrap`. *Why:* two simultaneous first clients would otherwise both try to create the slot. If another process created the slot meanwhile (`ErrBootstrapSlotExists`), `Snapshot` falls back to resuming it.
- **`Done`, `Err`.** Report when and why the stream ended. The daemon treats that as a source failure and exits with code 3.
- The function fields (`slotExists`, `bootstrap`, `snapshot`, `run`) exist so unit tests can replace the PostgreSQL-touching primitives.

### `bootstrap.go` — the first snapshot, which creates the slot

1. Validate the publication and the projection. Warn if PostgreSQL's own `max_slot_wal_keep_size` is a concern.
2. `createBootstrapSlot` runs `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`. PostgreSQL returns the slot's **consistent point** (an LSN) and the name of a snapshot that sees exactly the transactions committed before that point.
3. `readSnapshot` imports that snapshot on the management connection and reads the scope's rows.
4. `startReplication` runs on the same replication connection at the consistent point. The stream is handed to `Run` through `bootstrapStream`.

*Why this order:* the exported snapshot is valid only until the replication connection runs its next command. So the rows must be read before replication starts.

*Why refuse an existing slot:* this call can only vouch for a gap-free boundary it created itself.

*Why drop the slot on failure:* an orphaned slot would make PostgreSQL retain WAL forever.

### `snapshot.go` — later snapshots against the existing slot

- **`Reader.Snapshot`**
  1. Reads `pg_current_wal_insert_lsn()` as the cursor **before** fixing the snapshot. Any transaction the snapshot can't see commits after this point, so resuming from the cursor can only replay extra transactions, never skip one.
  2. Reads the rows (`readSnapshot`).
  3. Runs `checkReplayWindow`. If the slot's `restart_lsn` has already passed the cursor, the WAL between them may be gone, so it returns `ErrSnapshotWindowClosed` and the client retries.
- **`readSnapshot`.** Runs a `REPEATABLE READ, READ ONLY` transaction, which fixes one MVCC snapshot for every page read. It records `pg_current_snapshot()` as the visibility used by `Covers`. Two test hooks make race tests deterministic.
- **`scanScopedRows`.** Uses **keyset pagination**: `WHERE (pk) > (last pk) ORDER BY pk LIMIT n`.
  - *Why not OFFSET:* OFFSET re-reads everything before the page. Keyset is constant cost per page.
  - *Why pages at all:* a large tenant would otherwise be loaded into memory at once.
  - Rows are requested in **text format**, so snapshot values use exactly the same encoding as streamed values.
  - Table and column identifiers are escaped with `pgx.Identifier.Sanitize`, and every value is a bind parameter.

### `visibility.go` — the MVCC snapshot

- **`parseVisibility`.** Parses `xmin:xmax:xip,...`.
- **`includes(xid)`.** A transaction is in the snapshot if it is below `xmin`, or below `xmax` and not in progress.
- **`widen`.** pgoutput sends 32-bit xids, while snapshots use 64-bit ones. `widen` reconstructs the full ID relative to `xmax`. This is safe because PostgreSQL's wraparound protection keeps every live transaction within 2³¹ of it.

### `validation.go` — checking the source matches the configuration

- **`validatePublication`.** The publication must exist, must publish insert, update, and delete, and must **not** publish TRUNCATE, which v0 cannot represent.
- **`validateProjectionSpec`.** Checks against the live database: the table exists, the scope column exists, the primary key exactly matches the configured one, and the table is in the publication.
- **`validateProjectionSpecConfig`.** Static checks: identifiers, no duplicate key columns, and **the scope column must be part of the primary key**. *Why:* a DELETE carries only the key. If the scope column weren't in the key, a delete couldn't be routed to the tenant whose projection holds the row.

### `slot.go`

- **`requireSlot`.** Used when resuming. The slot must exist, be logical, use `pgoutput`, and not be active (another process using it gives `ErrSlotInUse`, which is retryable).
- **`lookupSlot`.** Reads `pg_replication_slots`. The resume position is `confirmed_flush_lsn`, the last LSN this slot acknowledged.

### `retention.go`, `retention_policy.go` — protecting PostgreSQL's disk

A slot makes PostgreSQL keep all WAL since the slot's last acknowledgement. If watchd stalls, WAL grows without limit. The defence has two layers:

- **`monitorRetentionBudget`.** A goroutine beside `Run`. It samples retained WAL from `pg_replication_slots` every `RetentionSampleInterval`, and immediately at start. Every `RetentionCheckInterval` it re-reads `max_slot_wal_keep_size`, so a live config reload is noticed. Each sample uses its own short-lived connection, independent of the stream's reconnects.
- **`effectiveRetentionBudget`.** The stricter of watchd's `MaxRetainedWALBytes` (default 1 GiB) and the server's limit. PostgreSQL's default limit is unbounded, which is why watchd has its own.
- **`nextRetentionState`.** A pure function: ok → warn (50%) → degrade (80%) → terminal (100%).
  - It rises immediately and falls only with 5% hysteresis, so a slot hovering at a threshold doesn't flap.
  - Terminal is final.
  - It is a pure function so it can be tested exhaustively.
- **`applyRetentionPolicy`.** Logs and counts each transition. At terminal, it stops the stream with `ErrRetainedWALBudgetExceeded`.
- **`dropSlotForBudget`.** Drops the slot, retrying while PostgreSQL still marks it active. It uses its own timeout, not the caller's context. *Why:* once the decision is made, protecting the source must finish even during shutdown.
  - Every client must then rebuild. That is deliberate: losing a projection is recoverable, while PostgreSQL running out of disk is not.

### `errors.go` — what is retryable

- Every failure maps to a sentinel error (`ErrSlotInvalidated`, `ErrSourceUnavailable`, ...). Callers use `errors.Is`, never string matching.
- **`classifiedError`.** Carries a kind, a SQLSTATE, and a **retryable** flag.
- **`classifySQLState`** maps PostgreSQL error codes:
  - `42501` (insufficient privilege): terminal.
  - `55006` (slot in use): retryable.
  - `57P0x` (server shutting down): retryable.
  - "requested WAL segment removed": the slot is invalidated, so terminal.
- **`isRetryable`.** Decides whether `Run` tries again. *The rule:* retry only when a retry could succeed. Bad data, bad config, and an invalidated slot never improve with a retry.

### `metrics.go`

- **`readerMetrics`.** Every instrument and every attribute set is created once in `newReaderMetrics`. Recording never allocates.
- **Gauges.** Read atomics through callbacks, so the stream doesn't push values.
- **`snapshotObserver`.** Wraps the row sink to count snapshot rows and bytes.

The full catalogue is in [observability.md](observability.md).

### `testing.go`

Constructors (`NewTransactionForTest`, `NewSnapshotForTest`, `NewCursorForTest`) let other packages' tests build values with private fields, without exporting those fields to production code.

---

## `internal/watch` — fan-out, replay, and watcher lifecycle

This package knows nothing about PostgreSQL or gRPC. It sees only `cdc` types and an `Event` callback.

### `event.go`

A watch stream takes one of three shapes:

- `SnapshotBegin`, `SnapshotRows...`, `SnapshotEnd`, then live events;
- live events only, when resuming from a cursor;
- `Resync`, after which the stream ends.

The live events are `Batch` (one transaction's relevant changes, never split) and `Progress` (complete through this cursor).

- **Why `SnapshotEnd` carries no cursor:** the first `Progress` after it does. Freshness is announced in exactly one way.
- **`ResyncReason`.** Says why: the cursor is unavailable, the watcher is too slow, the snapshot window closed, or the source stopped.

### `hub.go`

**State, all under `mu`:**

- `ring`: the most recent `MaxReplayTransactions` transactions, oldest first.
- `received`: the cursor of the newest accepted transaction.
- `floor`: the oldest cursor the ring can still serve.
- `watchers` and `routes`.
- `tick`: a channel closed on each new transaction, which wakes every waiting watcher at once.

**`Accept(transaction)`** is the source's sink. It must be fast and must never block on a client.

1. Drop duplicates. After a reconnect, PostgreSQL may resend a transaction the hub already has. `order <= 0` means nothing new.
2. Append to the ring and evict the oldest entries. Eviction raises `floor`.
3. Routing: for each change, look up `routes[table]`. That gives the watchers grouped by scope column, then by scope value: `change.Key[scopeColumn]`. *Why index by scope:* with 10,000 watchers, Accept visits only the few that care, instead of all of them.
4. Group each scope's changes into one shared `delivery`. Every watcher of that scope gets the same slice.
5. Do a non-blocking send to each watcher's queue. **If the queue is full, the watcher is dropped** (`dropLocked`) and gets `Resync(SlowWatcher)`. *Why:* the alternative is that one slow client stalls ingestion for everyone and makes PostgreSQL retain WAL.
6. The scratch slices and the map are reused between calls. A transaction no watcher cares about allocates nothing.
7. `tick` is closed **after** unlocking, so woken watchers don't immediately contend for the lock.

**`deliveryLocked`.** Finds a scope's delivery by linear scan while there are 8 or fewer, and switches to a map beyond that. *Why:* most transactions touch one or two scopes, and a scan beats hashing there.

**`Watch(req, send)`** handles one client stream:

- With a resume cursor, it calls `resume`.
- Without one, it calls `snapshot`.
- If the hub is stopped, it sends `Resync` immediately.

**`resume`.**

- If the cursor is below `floor` (`replayableLocked`), it sends `Resync(CursorUnavailable)`.
- Otherwise it collects the backlog from the ring and registers the watcher **in the same critical section**. *Why:* nothing can slip between "copied the backlog" and "started receiving live transactions".

**`snapshot`.** This is where the no-gap promise is enforced:

1. Under the lock, copy the ring's transactions relevant to this scope, and **register before reading**. From that moment, every new transaction queues for the watcher.
2. Read the snapshot (`Snapshotter.Snapshot`, which is `cdc.Source`). Rows stream out as they arrive.
3. If the queue overflowed during the read, send `Resync`. *Why:* there's no point installing rows that are already stale.
4. Of the copied ring transactions, send only those the snapshot doesn't `Cover`. Live transactions get the same filter in `live`. *Why also check older ring transactions:* a commit can be streamed before it is visible to new snapshots (the synchronous-standby case), so the rows may lack it.
5. `recordSnapshotLocked` sets the replay floor at the first snapshot. Before any snapshot, the hub can't vouch that its ring reaches back far enough, so every resume gets `Resync`.

**`live`.** The steady-state loop:

1. Drain the queue, sending the `Batch` events that pass `need`.
2. Send `Progress` at the newest position that is fully delivered.
3. Wait for the next `tick`, overflow, stop, or cancellation.

*Why Progress can use the hub's `received` position:* every transaction accepted up to it has already been queued for this watcher, so once the queue is drained the watcher is complete through it, even if none of those transactions concerned it.

`base` and `baseSafe` handle the moment before the hub has received anything.

**`registerLocked` and `removeLocked`.** Scope slices are **replaced, never modified in place** (`slices.Clip`, `slices.Clone`). *Why:* `Accept` may be ranging over the old slice while a watcher drops out.

**`projection.changes`.** The slow-path filter used for the ring, by table and `Key[scopeColumn]`.

### `metrics.go`

Ring occupancy, oldest-entry age, watcher count, queue depth, watcher lag, evictions, lock wait, Accept duration, and counts of Progress and Resync events. `countingSend` wraps `send`, so every path that emits those events is counted in one place.

---

## `internal/server` — gRPC adapter

### `server.go`

- **`Server`.** Implements `watch.v1.WatchService` and gRPC health.
- **`Watch`**
  - Decodes the client's cursor, calls `watcher.Watch`, and converts each event in `send`.
  - The context comes from the gRPC stream, so a client disconnect ends the watch.
- **Wire cursors are `"<sourceID>@<lsn>"`** (`encodeCursor`, `decodeCursor`). *Why:* a client that switches servers can't accidentally resume with a cursor from a different database. The server refuses it, and the SDK starts over with a snapshot.
- **`CompareCursors`.** Clients compare cursors through this RPC instead of parsing them.
- **`sendRows`.** Packs snapshot rows into messages of about `MaxRowsMessageBytes` (1 MiB). *Why:* gRPC has message size limits, and big messages hurt latency.
- **Batches are never split**, because one batch is one atomic transaction. They are bounded by the source's `MaxTransactionBytes`, which is why the SDK raises its receive limit to 64 MiB.
- **`convertChange`, `convertChangeValues`.** Turn `cdc.Value` kinds into the protobuf `Value` oneof (text, null, or unchanged TOAST).
- **`convertRow`, `convertValues`.** The same for snapshot rows. Those rows can never contain unchanged TOAST.
- **`status`.** Maps errors to gRPC codes:
  - unknown projection → `NotFound`
  - bad cursor → `InvalidArgument`
  - source down → `Unavailable`
  - misconfigured source → `FailedPrecondition`
  - anything else → `Internal`, logged server-side only

  *Why hide details:* internal errors may mention tables or state the client has no business seeing.

### `metrics.go`

Active streams, send duration (which reveals slow-reading clients through gRPC flow control), snapshot bytes, and commit-to-send latency.

## `api/watch/v1` — the wire contract

`watch.proto` is the versioned protocol: `WatchRequest`, a `WatchResponse` oneof that mirrors `watch.Event`, `Change`, `Value` (oneof text, null, or unchanged TOAST), and `CompareCursors`.

*Why a version in the package path:* breaking changes go in `v2`, and old clients keep working.

## `sdk/go/watchd` — the client library

- **`types.go`.** Public, protobuf-free types: `Value{Text, Null, Unchanged}`, `Row`, `Change`, `Batch`, and `State{Fresh, Cursor}`.
- **`client.go`**
  - **`Dial`.** Raises the max message size, because batches are never split.
  - **`Sync`.** Loops forever. Each pass opens a stream with `stream` and decides how to recover from the error:
    - **Resync** → take a new snapshot. The first resync retries immediately; repeated ones use backoff.
    - **`InvalidArgument` on a stored cursor** → the cursor belongs to another source, so start over with a snapshot.
    - **Permanent codes** (NotFound, FailedPrecondition, ...) → return the error.
    - **Store failure** → return the error. *Why:* the SDK can't fix the user's storage.
    - **Anything else** → reconnect with exponential backoff and jitter, resuming from the stored cursor.
  - **`stream`.** Applies the events in order:
    - Snapshot rows go into a `SnapshotWriter`, which is committed atomically at `SnapshotEnd` or aborted if the stream fails.
    - Each `Batch` is applied with `Store.Apply`.
    - Each `Progress` saves the cursor, **then** marks the projection fresh.
    - Any event out of order is a protocol error.
  - **`setState`.** The projection is fresh only between a `Progress` and the next disconnect or resync. That is promise 2, enforced on the client.
- **`store.go`**
  - **`Store`.** The interface users implement: `Cursor`, `SaveCursor`, `Apply`, `BeginSnapshot`.
  - **`MemoryStore`.** A reference implementation:
    - Rows are keyed by primary-key values joined with `\x00`.
    - `Apply` keeps the old value of `Unchanged` columns, and re-applying a batch is harmless (idempotent).
    - A snapshot builds a new map beside the current one and swaps it in at `Commit`, so readers never see a half-installed snapshot.
- **`metrics.go`.** Client-side metrics carry the projection name, **never the scope**: the scope is usually a tenant ID.

---

## Decisions that recur everywhere

| Decision | Where | Why |
|---|---|---|
| Acknowledge only after the sink accepts | `stream.go` | A crash never loses a transaction, at the cost of at-least-once delivery. |
| Fail loudly on anything unknown | decoder, `receive`, config parsing | Silently skipping a TRUNCATE, an unknown message, or a typo'd setting corrupts projections or hides misconfiguration. |
| Bound everything | transaction bytes and changes, value size, watcher queue, replay ring, retained WAL, snapshot pages | No input, client, or stall can grow memory or disk without limit. |
| Drop slow watchers with `Resync` | `Hub.Accept` | One slow client must not slow the source or other clients. Resync is always a safe fallback. |
| Decide snapshot coverage by xid, not LSN | `Snapshot.Covers` | Commit visibility can race WAL order; visibility is exact. |
| Opaque cursors tagged with the source | `cdc.Cursor`, server wire format | Clients can't misuse positions or mix databases. |
| Pure functions for policies | `nextRetentionState`, `Decoder` | Policies can be tested exhaustively without PostgreSQL. |
| Injectable time, connect, and random functions | `Reader`, `Source` | Unit tests drive reconnects and races deterministically. |
| Allowlisted metric attributes, sets built once | `telemetry/attrs.go` | Bounded cardinality, no tenant data in monitoring, no allocation per measurement. |
| Never log the database URL or row data | `cdc.go`, config errors, logger docs | Logs travel further than secrets should. |
| Soft state, durable slot | hub and source | On restart only the slot survives. Every client resyncs once, which is simpler and safer than persisting the replay window. |

## Where to look in the tests

- `internal/cdc/decoder_test.go`: every pgoutput edge case, without PostgreSQL.
- `internal/cdc/*_integration_test.go`: the snapshot boundary races, type encoding, and retention, against real PostgreSQL (`make postgres-up && make integration`).
- `internal/watch/hub_test.go`: replay, resync, slow-watcher, and coverage rules.
- `tests/integration`: the whole process end to end.
