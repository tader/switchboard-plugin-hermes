# Retention contract for clients

Additive pre-publication update. No existing operation ID, request field, native identity, idempotency key, or accepted action receipt has been renamed/reset. The authoritative list remains `lib/operations.js` (15 operations). REST transport, not `websocket/call`.

## New operations

| ID | POST bridge path | Input | Effect |
| --- | --- | --- | --- |
| `hermes.retention.status` | `/operations/hermes.retention.status` | `{profile}` | Local read. Policy, scoped hot/eligible/retired run counts, receipt/uncertain/pending-admission counts, global free hot slots and database bytes. No gateway call. |
| `hermes.run.retire` | `/operations/hermes.run.retire` | `{profile,runId}` | Local, idempotent scoped retirement. Returns a permanent tombstone; repeat returns the same tombstone. No gateway POST, stop, delete or model action. |

Both are normal declared OpenAPI operations under the same Switchboard connection permission boundary. Retirement is a local write and must not be marked read-only. No `operationId` argument is needed: the immutable `(scope,runId)` tombstone is its idempotent result.

Retirement refuses a live collector, any non-settled run, `unknown` outcome, and any reserved/uncertain control action for that run. Pre-retention uncertain stop/answer receipts without a run ID conservatively protect their whole scope. Unknown run/profile returns 404/403 respectively; protected run returns 409. No admin override bypasses uncertainty.

## Additive `hermes.run.events` result

On an already retired run, the operation performs no upstream request or collector restart and returns:

```json
{
  "runId": "original-native-run-id",
  "retired": true,
  "state": "retired",
  "status": "completed",
  "retiredAt": 1790000000000,
  "retirementReason": "oldest_terminal_capacity_retirement",
  "events": [],
  "hasMore": false,
  "gap": true,
  "resyncRequired": true,
  "observing": false,
  "upstreamStatus": "not_requested",
  "coverage": {
    "scope": "retired_snapshot",
    "complete": false,
    "reason": "terminal_snapshot_retired"
  }
}
```

Other existing metadata/cursors, original/canonical session IDs, final status and increased snapshot revision are retained; `droppedThrough` advances through the retired high sequence. Normal ahead-of-snapshot cursor/revision rejection remains. Explicit retirement uses reason `explicit_scoped_retirement`. The example timestamp/ID are illustrative, not actual profile/run inventory.

Clients must recognize this state, stop paging/reopening this snapshot and preserve already ingested events plus the retention marker. Do not interpret an empty retired page as an empty transcript or delete cached messages. If history is needed, use the existing Switchboard `hermes.session.history` and native status operations. Event loss is explicit; retirement does not delete gateway sessions or transcripts. `hermes.run.status` can still read native status, but cannot resurrect the retired local snapshot.

Replaying an old `hermes.run.create` (including after process restart) returns the **exact stored action receipt** and does not POST, reallocate a slot or restart its retired collector. Changed input with the same operation identity still conflicts. `hermes.action.get` remains authoritative for the immutable receipt, including uncertain admissions/answers.

## Sustained-use policy

64 limits **hot snapshots + unresolved admission reservations**, not lifetime turns. At capacity, a new admission transaction retires the oldest eligible settled terminal snapshot **within its acting connection/principal/profile scope** before reserving a reusable slot. It never evicts another scope's data. Snapshot content stays bounded (1,000 events / 512 KiB each). Active/pending/paused/unknown/uncertain slots are preserved; if all available slots are protected, new admission fails 507 before native POST. Use scoped status/retire to inspect/release eligible snapshots, not database deletion.

Immutable receipts, prompt claims and compact retirement tombstones are preserved indefinitely in the safety ledger; there is no 2,048-action lifetime limit. Keeping all immutable receipts necessarily grows disk metadata. The previous 128 MiB total-file ceiling is removed, including on reopening an older database; normal filesystem/SQLite exhaustion still fails closed. Back up the complete SQLite database and provision/monitor its reported bytes. Copy/move the complete quiescent data directory for more storage; never drop/truncate action tables or tombstones. No export-and-forget flow is implemented or implied.

Tests cover 130 completed turns with 64 hot slots, 2,055 preserved receipts, old action replay/conflicts after retirement and restart, prompt claims after retirement, protected/uncertain slots, and cross-account refusal. Released-core wiring is verified by `npm run test:integration` against immutable Switchboard 0.9.0 and 0.8.3, including two-core peer transport.
