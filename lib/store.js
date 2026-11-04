import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs';
import path from 'node:path';
import { Fault } from './operations.js';

// Hot snapshots are bounded; immutable receipts/tombstones are a growing safety ledger.
// No lifetime turn/action-count ceiling. Filesystem exhaustion still fails closed.
export const DEFAULT_LIMITS = { runs: 64, actions: Infinity, events: 1000, eventBytes: 524288, pageBytes: 524288 };
const settledStatuses = new Set(['completed', 'failed', 'cancelled', 'interrupted']);
const parse = row => row ? JSON.parse(row.value) : undefined;
const stable = v => v && typeof v === 'object' ? Array.isArray(v) ? v.map(stable) : Object.fromEntries(Object.keys(v).sort().map(k => [k, stable(v[k])])) : v;
function ranges(input) {
  const result = [];
  for (const [a, b] of input.sort((x, y) => x[0] - y[0])) {
    if (result.length && a <= result.at(-1)[1] + 1) result.at(-1)[1] = Math.max(result.at(-1)[1], b);
    else result.push([a, b]);
  }
  return result;
}
export class Store {
  constructor(dir, limits = {}) {
    this.limits = { ...DEFAULT_LIMITS, ...limits };
    this.isBusy = () => false;
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    const file = path.join(dir, 'hermes.sqlite');
    this.db = new DatabaseSync(file); fs.chmodSync(file, 0o600);
    this.db.exec(`PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
      PRAGMA max_page_count=1073741823;
      CREATE TABLE IF NOT EXISTS actions(scope TEXT,key TEXT,fingerprint TEXT,value TEXT,PRIMARY KEY(scope,key));
      CREATE INDEX IF NOT EXISTS actions_unresolved ON actions(scope,json_extract(value,'$.status'),json_extract(value,'$.kind'));
      CREATE TABLE IF NOT EXISTS control_claims(scope TEXT,target TEXT,key TEXT,PRIMARY KEY(scope,target));
      CREATE TABLE IF NOT EXISTS run_slots(scope TEXT,slot TEXT,PRIMARY KEY(scope,slot));
      CREATE TABLE IF NOT EXISTS runs(scope TEXT,id TEXT,value TEXT,PRIMARY KEY(scope,id));
      CREATE TABLE IF NOT EXISTS retired_runs(scope TEXT,id TEXT,value TEXT,PRIMARY KEY(scope,id));
      CREATE TABLE IF NOT EXISTS events(scope TEXT,run TEXT,seq INTEGER,value TEXT,bytes INTEGER,PRIMARY KEY(scope,run,seq));`);
  }
  transaction(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try { const v = fn(); this.db.exec('COMMIT'); return v; }
    catch (e) { this.db.exec('ROLLBACK'); throw e; }
  }
  action(scope, key) { return parse(this.db.prepare('SELECT value FROM actions WHERE scope=? AND key=?').get(scope, key)); }
  reserve(scope, key, fingerprint, meta) {
    return this.transaction(() => {
      const old = this.db.prepare('SELECT fingerprint,value FROM actions WHERE scope=? AND key=?').get(scope, key);
      if (old) {
        if (old.fingerprint !== fingerprint) throw new Fault('operation_identity_conflict', 409);
        const receipt = JSON.parse(old.value);
        // A crash may have happened after the gateway accepted the reserved action.
        if (receipt.status === 'reserved') receipt.status = 'uncertain';
        return { fresh: false, receipt };
      }
      if (this.db.prepare('SELECT COUNT(*) n FROM actions').get().n >= this.limits.actions) throw new Fault('action_storage_full', 507);
      if (meta.kind === 'run.create') {
        this.makeRoom(scope);
        this.db.prepare('INSERT INTO run_slots VALUES(?,?)').run(scope, `action:${key}`);
      }
      if (meta.controlTarget) {
        if (this.db.prepare('SELECT 1 FROM control_claims WHERE scope=? AND target=?').get(scope, meta.controlTarget)) throw new Fault('prompt_already_attempted', 409);
        this.db.prepare('INSERT INTO control_claims VALUES(?,?,?)').run(scope, meta.controlTarget, key);
      }
      const receipt = { operationId: key, status: 'reserved', createdAt: Date.now(), ...meta };
      this.db.prepare('INSERT INTO actions VALUES(?,?,?,?)').run(scope, key, fingerprint, JSON.stringify(receipt));
      return { fresh: true, receipt };
    });
  }
  finish(scope, key, receipt) {
    const value = JSON.stringify(receipt);
    if (Buffer.byteLength(value) > 131072) throw new Fault('receipt_too_large', 502);
    return this.transaction(() => {
      this.db.prepare('UPDATE actions SET value=? WHERE scope=? AND key=?').run(value, scope, key);
      if (receipt.kind === 'run.create' && receipt.status === 'rejected') this.db.prepare('DELETE FROM run_slots WHERE scope=? AND slot=?').run(scope, `action:${key}`);
      if (receipt.controlTarget && receipt.status === 'rejected') this.db.prepare('DELETE FROM control_claims WHERE scope=? AND target=?').run(scope, receipt.controlTarget);
      return receipt;
    });
  }
  run(scope, id) { return parse(this.db.prepare('SELECT value FROM runs WHERE scope=? AND id=?').get(scope, id)); }
  retired(scope, id) { return parse(this.db.prepare('SELECT value FROM retired_runs WHERE scope=? AND id=?').get(scope, id)); }
  protection(scope, id) {
    const row = this.run(scope, id);
    if (!row) return 'run_not_observed';
    if (this.isBusy(scope, id)) return 'collector_active';
    if (row.state !== 'settled' || !settledStatuses.has(row.status)) return 'run_not_settled';
    // Pre-retention versions lack runId on controls: retain the entire affected scope
    // conservatively rather than guessing which run owns an uncertain answer/stop.
    for (const a of this.db.prepare("SELECT value FROM actions WHERE scope=? AND json_extract(value,'$.status') IN ('reserved','uncertain') AND json_extract(value,'$.kind') LIKE 'run.%'").all(scope).map(parse)) {
      if (['reserved', 'uncertain'].includes(a.status) && a.kind?.startsWith('run.') &&
          (a.runId === id || a.result?.run_id === id || (!a.runId && !a.result?.run_id && a.kind !== 'run.create'))) return 'uncertain_action';
    }
    return undefined;
  }
  retireInside(scope, id, reason) {
    const existing = this.retired(scope, id); if (existing) return existing;
    const blocked = this.protection(scope, id); if (blocked) throw new Fault(blocked, blocked === 'run_not_observed' ? 404 : 409);
    const row = this.run(scope, id);
    const tombstone = { ...row, retired: true, state: 'retired', retiredAt: Date.now(), retirementReason: reason,
      droppedThrough: row.highSeq, snapshotRevision: (row.snapshotRevision ?? 0) + 1 };
    this.db.prepare('INSERT INTO retired_runs VALUES(?,?,?)').run(scope, id, JSON.stringify(tombstone));
    this.db.prepare('DELETE FROM events WHERE scope=? AND run=?').run(scope, id);
    this.db.prepare('DELETE FROM runs WHERE scope=? AND id=?').run(scope, id);
    this.db.prepare('DELETE FROM run_slots WHERE scope=? AND slot=?').run(scope, `run:${id}`);
    return tombstone;
  }
  retire(scope, id) { return this.transaction(() => this.retireInside(scope, id, 'explicit_scoped_retirement')); }
  makeRoom(scope) {
    if (this.db.prepare('SELECT COUNT(*) n FROM run_slots').get().n < this.limits.runs) return;
    // Only evict an eligible snapshot in the acting scope, never another account/profile.
    const candidates = this.db.prepare('SELECT id,value FROM runs WHERE scope=? ORDER BY rowid').all(scope)
      .sort((a, b) => (JSON.parse(a.value).settledAt ?? 0) - (JSON.parse(b.value).settledAt ?? 0));
    const candidate = candidates.find(r => !this.protection(scope, r.id));
    if (!candidate) throw new Fault('run_storage_full', 507);
    this.retireInside(scope, candidate.id, 'oldest_terminal_capacity_retirement');
  }
  retention(scope) {
    const rows = this.db.prepare('SELECT id FROM runs WHERE scope=?').all(scope);
    return { policy: 'oldest_eligible_terminal_within_scope', hotSnapshotLimit: this.limits.runs,
      scopedHotRuns: rows.length, scopedEligibleRuns: rows.filter(r => !this.protection(scope, r.id)).length,
      scopedRetiredRuns: this.db.prepare('SELECT COUNT(*) n FROM retired_runs WHERE scope=?').get(scope).n,
      scopedReceipts: this.db.prepare('SELECT COUNT(*) n FROM actions WHERE scope=?').get(scope).n,
      scopedUncertainActions: this.db.prepare("SELECT COUNT(*) n FROM actions WHERE scope=? AND json_extract(value,'$.status') IN ('reserved','uncertain')").get(scope).n,
      scopedPendingAdmissionSlots: this.db.prepare("SELECT COUNT(*) n FROM run_slots WHERE scope=? AND slot LIKE 'action:%'").get(scope).n,
      globalFreeHotSlots: Math.max(0, this.limits.runs - this.db.prepare('SELECT COUNT(*) n FROM run_slots').get().n),
      immutableReceiptsPreserved: true, receiptCountLimit: Number.isFinite(this.limits.actions) ? this.limits.actions : null,
      ledgerBytes: Number(this.db.prepare('PRAGMA page_count').get().page_count) * Number(this.db.prepare('PRAGMA page_size').get().page_size) };
  }
  ensureRun(scope, id, sessionId, actionKey) {
    this.transaction(() => {
      if (this.retired(scope, id)) return;
      const reserved = actionKey && this.db.prepare('SELECT 1 FROM run_slots WHERE scope=? AND slot=?').get(scope, `action:${actionKey}`);
      if (this.run(scope, id)) {
        if (reserved) this.db.prepare('DELETE FROM run_slots WHERE scope=? AND slot=?').run(scope, `action:${actionKey}`);
        return;
      }
      if (!reserved) this.makeRoom(scope);
      if (reserved) this.db.prepare('DELETE FROM run_slots WHERE scope=? AND slot=?').run(scope, `action:${actionKey}`);
      this.db.prepare('INSERT INTO run_slots VALUES(?,?)').run(scope, `run:${id}`);
      this.db.prepare('INSERT INTO runs VALUES(?,?,?)').run(scope, id, JSON.stringify({ runId: id, sessionId, originalSessionId: sessionId, highSeq: -1, droppedThrough: -1, gaps: [], state: 'idle', createdAt: Date.now() }));
    });
  }
  updateRun(scope, id, patch) {
    if (this.retired(scope, id)) return; // Tombstones never rehydrate or become mutable live state.
    const row = this.run(scope, id); if (!row) throw new Fault('run_not_observed', 404);
    const next = { ...row, ...patch };
    if (next.state === 'settled' && settledStatuses.has(next.status)) next.settledAt ??= Date.now();
    this.db.prepare('UPDATE runs SET value=? WHERE scope=? AND id=?').run(JSON.stringify(next), scope, id);
  }
  append(scope, id, event) {
    return this.transaction(() => {
      const row = this.run(scope, id); if (!row) throw new Fault('run_not_observed', 404);
      const seq = event.seq;
      if (!Number.isSafeInteger(seq) || seq < 0) throw new Fault('invalid_event_sequence', 502);
      const value = JSON.stringify(stable(event)), bytes = Buffer.byteLength(value);
      if (bytes > 65536) throw new Fault('event_too_large', 502);
      const old = this.db.prepare('SELECT value FROM events WHERE scope=? AND run=? AND seq=?').get(scope, id, seq);
      if (old) {
        if (old.value !== value) throw new Fault('event_sequence_conflict', 502);
        return false;
      }
      if (seq <= row.droppedThrough) return false;
      if (seq > row.highSeq + 1) row.gaps.push([row.highSeq + 1, seq - 1]);
      row.gaps = ranges(row.gaps);
      row.highSeq = Math.max(seq, row.highSeq);
      row.snapshotRevision = (row.snapshotRevision ?? 0) + 1;
      if (seq < row.highSeq) {
        row.repairRevision = row.snapshotRevision;
        row.repairFromSeq = Math.min(row.repairFromSeq ?? seq, seq);
      }
      row.gaps = row.gaps.flatMap(([a, b]) => seq < a || seq > b ? [[a, b]] : [...(seq > a ? [[a, seq - 1]] : []), ...(seq < b ? [[seq + 1, b]] : [])]);
      // Compact old gap metadata by advancing the explicit loss boundary, never hide a gap.
      while (row.gaps.length > 128) row.droppedThrough = Math.max(row.droppedThrough, row.gaps.shift()[1]);
      this.db.prepare('DELETE FROM events WHERE scope=? AND run=? AND seq<=?').run(scope, id, row.droppedThrough);
      this.db.prepare('INSERT INTO events VALUES(?,?,?,?,?)').run(scope, id, seq, value, bytes);
      let counts = this.db.prepare('SELECT COUNT(*) n,COALESCE(SUM(bytes),0) bytes FROM events WHERE scope=? AND run=?').get(scope, id);
      while (counts.n > this.limits.events || counts.bytes > this.limits.eventBytes) {
        const first = this.db.prepare('SELECT seq,bytes FROM events WHERE scope=? AND run=? ORDER BY seq LIMIT 1').get(scope, id);
        this.db.prepare('DELETE FROM events WHERE scope=? AND run=? AND seq=?').run(scope, id, first.seq);
        row.droppedThrough = Math.max(row.droppedThrough, first.seq); counts.n--; counts.bytes -= first.bytes;
      }
      row.gaps = row.gaps.filter(([, b]) => b > row.droppedThrough).map(([a, b]) => [Math.max(a, row.droppedThrough + 1), b]);
      this.db.prepare('UPDATE runs SET value=? WHERE scope=? AND id=?').run(JSON.stringify(row), scope, id);
      return true;
    });
  }
  gap(scope, id, oldest) {
    const row = this.run(scope, id);
    if (!Number.isSafeInteger(oldest) || oldest < 0) throw new Fault('invalid_replay_gap', 502);
    // Upstream eviction does not erase events already persisted here.
    const gaps = oldest > row.highSeq + 1 ? ranges([...row.gaps, [row.highSeq + 1, oldest - 1]]) : row.gaps;
    this.updateRun(scope, id, { gaps });
  }
  page(scope, id, after = -1, limit = 100, revision = 0) {
    const retired = this.retired(scope, id);
    if (retired && after > retired.highSeq) throw new Fault('cursor_ahead_of_snapshot', 409);
    if (retired && revision > retired.snapshotRevision) throw new Fault('revision_ahead_of_snapshot', 409);
    if (retired) return { ...retired, events: [], nextSeq: Math.max(after, retired.highSeq), hasMore: false, gap: true,
      resyncRequired: true, coverage: { scope: 'retired_snapshot', complete: false, droppedThrough: retired.highSeq, missingRanges: [], reason: 'terminal_snapshot_retired' } };
    const row = this.run(scope, id); if (!row) throw new Fault('run_not_observed', 404);
    if (after > row.highSeq) throw new Fault('cursor_ahead_of_snapshot', 409);
    if (revision > (row.snapshotRevision ?? 0)) throw new Fault('revision_ahead_of_snapshot', 409);
    const rows = this.db.prepare('SELECT value,bytes FROM events WHERE scope=? AND run=? AND seq>? ORDER BY seq LIMIT ?').all(scope, id, after, limit);
    const events = []; let bytes = 0;
    for (const r of rows) { if (bytes + r.bytes > this.limits.pageBytes) break; events.push(JSON.parse(r.value)); bytes += r.bytes; }
    const nextSeq = events.at(-1)?.seq ?? Math.max(after, row.droppedThrough);
    return { ...row, events, nextSeq, hasMore: this.db.prepare('SELECT 1 FROM events WHERE scope=? AND run=? AND seq>? LIMIT 1').get(scope, id, nextSeq) !== undefined,
      gap: after < row.droppedThrough || row.gaps.some(([, b]) => b > after),
      snapshotRevision: row.snapshotRevision ?? 0,
      resyncRequired: (row.repairRevision ?? 0) > revision && after >= row.repairFromSeq,
      rewindAfterSeq: row.repairFromSeq === undefined ? undefined : row.repairFromSeq - 1,
      coverage: { scope: 'retained_observed_sequences', complete: after >= row.droppedThrough && !row.gaps.some(([, b]) => b > after), droppedThrough: row.droppedThrough, missingRanges: row.gaps.filter(([, b]) => b > after) } };
  }
  pending(scope, runId, requestId, kind) {
    // Controls must be observed on this exact profile/run, never guessed from composer text.
    const rows = this.db.prepare('SELECT value FROM events WHERE scope=? AND run=? ORDER BY seq DESC').all(scope, runId);
    for (const r of rows) {
      const e = JSON.parse(r.value);
      if (e.request_id === requestId || e.prompt_id === requestId) {
        if (e.event === `${kind}.request` || e.event === `${kind}.requested`) return e;
        if (e.event === `${kind}.responded` || e.event === `${kind}.resolved` || e.event === `${kind}.settled`) return undefined;
      }
    }
  }
  close() { this.db.close(); }
}
