import test from 'node:test';
import assert from 'node:assert/strict';
import { Engine, profiles } from '../lib/engine.js';
import { Bridge } from '../index.js';
import { operations, openapi } from '../lib/operations.js';
import { sse } from '../lib/sse.js';
import { Store } from '../lib/store.js';
import { fixture, connection, secret, event, frames, stream, collected } from './mock.js';

test('live discovery verifies only authorized configured profiles and exposes no keys', async t => {
  const { engine, conn, gateway } = fixture(t);
  const result = await engine.call(conn, 'hermes.profiles.list', {});
  assert.deepEqual(result.profiles.map(p => p.id), ['default']); assert.equal(result.profiles[0].available, true);
  assert.equal(result.exhaustive, false); assert.ok(!JSON.stringify(result).includes(secret));
  gateway.auth = false;
  assert.equal((await engine.call(conn, 'hermes.profiles.list', {})).profiles[0].error, 'gateway_auth_required');
  await assert.rejects(engine.call(conn, 'hermes.capabilities.get', { profile: 'unconfigured' }), /profile_not_authorized/);
});

test('profile configuration rejects arbitrary paths, secrets in URLs and insecure remote endpoints', () => {
  for (const baseUrl of ['https://user:pass@example.com', 'http://example.com', 'https://example.com/v1', 'https://example.com?token=secret']) assert.throws(() => profiles({ profiles: [{ id: 'default', baseUrl }] }));
  assert.throws(() => profiles({ profiles: [{ id: 'lab', baseUrl: 'https://example.com/p/other' }] }), /profile_route_mismatch/);
  assert.throws(() => profiles({ profiles: [{ id: 'lab', baseUrl: 'https://example.com' }] }), /named_profile_requires_route/);
});

test('session creation is durable and exact operation replay never duplicates or changes payload', async t => {
  const { engine, gateway, call, dir } = fixture(t);
  const first = await call('session.create', { operationId: 'create-1', title: 'a' });
  assert.equal(first.status, 'accepted'); assert.equal(first.result.session.id, first.sessionId);
  assert.deepEqual(await call('session.create', { operationId: 'create-1', title: 'a' }), first);
  await assert.rejects(call('session.create', { operationId: 'create-1', title: 'changed' }), /operation_identity_conflict/);
  assert.equal(gateway.writes().length, 1);
  await engine.close();
  const restarted = new Engine(dir, { fetch: gateway.fetch }); t.after(() => restarted.close());
  assert.deepEqual(await restarted.call(connection(), 'hermes.session.create', { profile: 'default', operationId: 'create-1', title: 'a' }), first);
  assert.equal(gateway.writes().length, 1);
});

test('lost session/run responses remain uncertain across replay without upstream resubmission', async t => {
  const { gateway, call } = fixture(t); gateway.failWrite = true;
  for (const name of ['session.create', 'run.create']) {
    const args = { operationId: name, ...(name === 'run.create' ? { sessionId: 's', input: 'hello' } : {}) };
    const result = await call(name, args); assert.equal(result.status, 'uncertain');
    await call(name, args); assert.equal((await call('action.get', { operationId: name })).status, 'uncertain');
  }
  assert.equal(gateway.writes().length, 2);
  assert.equal(gateway.writes()[1].headers['Idempotency-Key'], 'run.create');
});

test('crash after reservation is uncertain rather than a fresh write', async t => {
  const { engine, call } = fixture(t); await call('session.create', { operationId: 'crash' });
  const c = engine.context(connection(), 'default'); const r = engine.store.action(c.scope, 'crash');
  engine.store.finish(c.scope, 'crash', { ...r, status: 'reserved' });
  assert.equal((await call('session.create', { operationId: 'crash' })).status, 'uncertain');
});

test('concurrent run replay sends once with exact key and consumes SSE into durable batches', async t => {
  const { gateway, engine, call } = fixture(t);
  const args = { operationId: 'key/Exact:123', sessionId: 'session-native', input: 'line1\nline2' };
  const results = await Promise.all([call('run.create', args), call('run.create', args)]);
  await collected(engine);
  assert.equal(gateway.writes().length, 1); assert.equal(gateway.writes()[0].headers['Idempotency-Key'], args.operationId);
  assert.deepEqual(gateway.writes()[0].body, { session_id: args.sessionId, input: args.input });
  const c = engine.context(connection(), 'default');
  const page = engine.store.page(c.scope, results[0].result.run_id, -1, 1);
  assert.equal(page.events[0].delta, 'hello'); assert.equal(page.hasMore, true); assert.equal(page.nextSeq, 0);
  assert.equal(engine.store.page(c.scope, results[0].result.run_id, 0).events[0].event, 'run.completed');
  assert.ok(results.some(r => r.status === 'accepted'));
});

test('run admission refuses memory-only idempotency and invalid keys before any write', async t => {
  const { gateway, call } = fixture(t); gateway.cap.features.runs_idempotency.durable = false;
  await assert.rejects(call('run.create', { operationId: 'key', sessionId: 's', input: 'x' }), /durable_run_idempotency_required/);
  await assert.rejects(call('run.create', { operationId: 'has space', sessionId: 's', input: 'x' }), /invalid_operation_identity/);
  assert.equal(gateway.writes().length, 0);
});

test('event storage orders late events, deduplicates, reports missing ranges and prevents conflicting replacements', async t => {
  const { engine } = fixture(t); const s = engine.store, scope = 'scope'; s.ensureRun(scope, 'r');
  s.append(scope, 'r', event('r', 0)); s.append(scope, 'r', event('r', 2));
  assert.deepEqual(s.page(scope, 'r').coverage.missingRanges, [[1, 1]]);
  const oldRevision = s.page(scope, 'r').snapshotRevision;
  s.append(scope, 'r', event('r', 1)); assert.equal(s.append(scope, 'r', event('r', 1)), false);
  assert.equal(s.append(scope, 'r', Object.fromEntries(Object.entries(event('r', 1)).reverse())), false);
  assert.deepEqual(s.page(scope, 'r').events.map(e => e.seq), [0, 1, 2]); assert.equal(s.page(scope, 'r').gap, false);
  assert.throws(() => s.append(scope, 'r', event('r', 1, 'different')), /event_sequence_conflict/);
  assert.throws(() => s.page(scope, 'r', 99), /cursor_ahead/);
  const repaired = s.page(scope, 'r', 2, 100, oldRevision);
  assert.equal(repaired.resyncRequired, true); assert.equal(repaired.rewindAfterSeq, 0);
  assert.equal(s.page(scope, 'r', 2, 100, repaired.snapshotRevision).resyncRequired, false);
});

test('bounded eviction and upstream replay truncation are explicit and survive reopening', async t => {
  const { engine, dir } = fixture(t, { limits: { events: 2 } });
  const s = engine.store; s.ensureRun('scope', 'r'); for (let i = 0; i < 5; i++) s.append('scope', 'r', event('r', i));
  let page = s.page('scope', 'r'); assert.equal(page.gap, true); assert.equal(page.droppedThrough, 2); assert.deepEqual(page.events.map(e => e.seq), [3, 4]);
  s.gap('scope', 'r', 8); await engine.close();
  const reopened = new Store(dir, { events: 2 }); t.after(() => reopened.close());
  page = reopened.page('scope', 'r'); assert.equal(page.droppedThrough, 2); assert.deepEqual(page.coverage.missingRanges, [[5, 7]]); assert.equal(page.coverage.complete, false);
});

test('SSE parser handles split UTF8, CRLF, multiline data and comments without yielding incomplete tail', async () => {
  const text = ': heartbeat\r\nid: 0\r\ndata: {"seq":0,\r\ndata: "delta":"é😀"}\r\n\r\ndata: {"unfinished":true}';
  const events = []; for await (const e of sse(stream(text, { split: 1 }).body)) events.push(e);
  assert.deepEqual(events, [{ id: '0', data: { seq: 0, delta: 'é😀' } }]);
});

test('collector resumes exact native cursor after EOF, persists truncation and ordered tool/question identity', async t => {
  const { engine, gateway, call } = fixture(t);
  gateway.onStream = (id, init) => gateway.streamCalls === 1
    ? stream(frames([event(id, 0, 'tool.started', { tool_call_id: 'native-call', tool: 'test' })]))
    : stream(frames([{ event: 'replay.truncated', run_id: id, oldest_retained_seq: 2 }, event(id, 2, 'clarify.request', { prompt_id: 'native-question', execution_generation: 7 }), event(id, 3, 'run.completed')]));
  const result = await call('run.create', { operationId: 'resume', sessionId: 's', input: 'hi' }); await collected(engine);
  const streams = gateway.calls.filter(c => c.url.endsWith('/events')); assert.equal(streams[1].headers['Last-Event-ID'], '0');
  const page = engine.store.page(engine.context(connection(), 'default').scope, result.result.run_id);
  assert.equal(page.gap, true); assert.equal(page.events[1].prompt_id, 'native-question'); assert.equal(page.events[1].execution_generation, 7);
});

test('collector pauses on gateway auth failure without retry loops or secret leakage', async t => {
  const { engine, gateway, call } = fixture(t);
  gateway.onStream = () => new Response(secret, { status: 401 });
  const result = await call('run.create', { operationId: 'auth', sessionId: 's', input: 'hi' }); await collected(engine);
  const row = engine.store.run(engine.context(connection(), 'default').scope, result.result.run_id);
  assert.equal(row.error, 'gateway_auth_required'); assert.equal(gateway.streamCalls, 1); assert.ok(!JSON.stringify(row).includes(secret));
});

test('collector rejects foreign run IDs and id/seq mismatches', async t => {
  for (const [name, e] of [['foreign', event('other', 0)], ['mismatch', event('run_mismatch', 0)]]) {
    const f = fixture(t); f.gateway.onStream = () => name === 'foreign' ? stream(frames([e])) : stream(`id: 1\ndata: ${JSON.stringify(e)}\n\n`);
    await f.call('run.create', { operationId: name, sessionId: 's', input: 'x' }); await collected(f.engine);
    const row = f.engine.store.run(f.engine.context(connection(), 'default').scope, `run_${name}`);
    assert.equal(row.error, name === 'foreign' ? 'invalid_event_run_identity' : 'event_cursor_mismatch');
    assert.equal(f.engine.store.page(f.engine.context(connection(), 'default').scope, `run_${name}`).events.length, 0);
  }
});

test('durable event replay works while gateway is offline', async t => {
  const { engine, gateway, call } = fixture(t);
  await call('run.create', { operationId: 'offline', sessionId: 's', input: 'x' }); await collected(engine);
  gateway.online = false;
  const page = await call('run.events', { runId: 'run_offline', afterSeq: 0 });
  assert.equal(page.events[0].event, 'run.completed'); assert.equal(page.upstreamStatus, 'unavailable'); assert.equal(page.error, 'gateway_transport_error');
});

test('profile, connection and changed credential principal isolate receipts and snapshots', async t => {
  const { engine, gateway, call } = fixture(t);
  await call('session.create', { operationId: 'same' });
  await assert.rejects(engine.call(connection('other-account'), 'hermes.action.get', { profile: 'default', operationId: 'same' }), /action_not_found/);
  const lab = connection('account-a', [{ id: 'lab', baseUrl: 'https://gateway.example/p/lab' }]);
  await engine.call(lab, 'hermes.session.create', { profile: 'lab', operationId: 'same' });
  const ids = gateway.writes().map(w => w.body.id); assert.notEqual(ids[0], ids[1]);
  const changed = connection(); changed.credentials.tokens.default = 'a-different-principal-token';
  await assert.rejects(engine.call(changed, 'hermes.action.get', { profile: 'default', operationId: 'same' }), /action_not_found/);
});

test('exact authority clarification and approval bodies, stale generation and unsupported clarify gates', async t => {
  const { gateway, call } = fixture(t);
  gateway.runs.set('default:r', { run_id: 'r', status: 'running', session_id: 's', execution_generation: 7, pending_controls: [{ kind: 'clarify', prompt_id: 'q' }, { kind: 'approval', prompt_id: 'a' }] });
  const args = { runId: 'r', operationId: 'answer-q', requestId: 'q', executionGeneration: 7, answer: 'yes' };
  assert.equal((await call('run.clarify', args)).status, 'accepted');
  assert.deepEqual(gateway.writes()[0].body, { request_id: 'q', execution_generation: 7, answer: 'yes' });
  await call('run.clarify', args); assert.equal(gateway.writes().length, 1);
  await assert.rejects(call('run.clarify', { ...args, operationId: 'another-q' }), /prompt_already_attempted/);
  await assert.rejects(call('run.approval', { runId: 'r', operationId: 'stale', requestId: 'a', executionGeneration: 6, choice: 'once' }), /stale_execution_generation/);
  await call('run.approval', { runId: 'r', operationId: 'approve', requestId: 'a', executionGeneration: 7, choice: 'deny' });
  assert.deepEqual(gateway.writes()[1].body, { request_id: 'a', execution_generation: 7, choice: 'deny' });
  gateway.cap.features.run_unknown_resolution = false;
  await assert.rejects(call('run.clarify', { ...args, operationId: 'unsupported' }), /unsupported_run_unknown_resolution/);
  gateway.cap.features.run_unknown_resolution = true; delete gateway.runs.get('default:r').pending_controls;
  await assert.rejects(call('run.clarify', { ...args, operationId: 'classic' }), /unsupported_clarify_runtime/);
});

test('uncertain answers cannot repeat under same or different action identities, including concurrent clicks', async t => {
  const { gateway, call } = fixture(t);
  gateway.runs.set('default:r', { run_id: 'r', status: 'running', session_id: 's', execution_generation: 1, pending_controls: [{ kind: 'clarify', prompt_id: 'q' }] }); gateway.failWrite = true;
  const args = { runId: 'r', operationId: 'one', requestId: 'q', executionGeneration: 1, answer: 'hi' };
  const results = await Promise.allSettled([call('run.clarify', args), call('run.clarify', { ...args, operationId: 'two' })]);
  assert.equal(results.filter(r => r.status === 'fulfilled').length, 1); assert.equal(gateway.writes().length, 1);
  assert.equal((await call('run.clarify', args)).status, 'uncertain'); assert.equal(gateway.writes().length, 1);
});

test('native history paging preserves tool IDs and compression-tip session identity; stop has a durable receipt', async t => {
  const { gateway, call } = fixture(t); gateway.sessions.set('default:s', { id: 's' });
  const h = await call('session.history', { sessionId: 's', limit: 2, offset: 4, order: 'oldest' });
  assert.equal(h.session_id, 's_tip'); assert.equal(h.data[0].tool_call_id, 'call1'); assert.equal(h.pagination.offset, '4');
  gateway.runs.set('default:r', { run_id: 'r', status: 'running' });
  await call('run.stop', { runId: 'r', operationId: 'stop' }); await call('run.stop', { runId: 'r', operationId: 'stop' });
  assert.equal(gateway.writes().length, 1); assert.ok(gateway.writes()[0].url.endsWith('/stop'));
});

test('private bridge enforces one-shot route-bound authorization and never exposes gateway credentials', async t => {
  const { engine, conn, gateway } = fixture(t); const b = new Bridge(engine), origin = 'http://127.0.0.1:9999';
  const req = { method: 'POST', url: new URL(origin + '/operations/hermes.profiles.list'), headers: new Headers() };
  b.authorize(req, conn, origin); const auth = req.headers.get('authorization'); assert.ok(!auth.includes(secret));
  const result = await b.dispatch('POST', req.url.pathname, auth, {}); assert.equal(result.profiles[0].available, true);
  await assert.rejects(b.dispatch('POST', req.url.pathname, auth, {}), /bridge_auth_required/);
  assert.throws(() => b.authorize({ ...req, url: new URL('https://other.example/operations/hermes.profiles.list') }, conn, origin), /bridge_route_not_allowed/);
  b.authorize(req, conn, origin); const other = req.headers.get('authorization'); b.revoke(conn.id);
  await assert.rejects(b.dispatch('POST', req.url.pathname, other, {}), /bridge_auth_required/);
  assert.equal(gateway.writes().length, 0);
});

test('OpenAPI declares each actual custom REST operation; unknown/unbounded arguments fail closed', async t => {
  const { call } = fixture(t); const doc = openapi('http://127.0.0.1:9999');
  assert.equal(Object.keys(doc.paths).length, operations.length);
  for (const op of operations) assert.equal(doc.paths[`/operations/${op.id}`].post.operationId, op.id);
  await assert.rejects(call('sessions.list', { url: 'https://arbitrary.example' }), /unknown_argument/);
  await assert.rejects(call('run.events', { runId: 'r', limit: 10000 }), /invalid_arguments/);
  await assert.rejects(call('run.approval', { runId: 'r', operationId: 'x', requestId: 'p', choice: 'always' }), /invalid_arguments/);
  await assert.rejects(call('session.get', { sessionId: '..' }), /invalid_arguments/);
});

test('storage quota refuses a new action before an upstream write', async t => {
  const { gateway, call } = fixture(t, { limits: { actions: 1 } });
  await call('session.create', { operationId: 'one' });
  await assert.rejects(call('session.create', { operationId: 'two' }), /action_storage_full/);
  assert.equal(gateway.writes().length, 1);
});

test('concurrent distinct run admissions reserve capacity before contacting gateway', async t => {
  const { gateway, engine, call } = fixture(t, { limits: { runs: 1 } });
  const results = await Promise.allSettled(['one', 'two'].map(operationId => call('run.create', { operationId, sessionId: 's', input: 'hello' })));
  await collected(engine);
  assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
  assert.equal(gateway.writes().length, 1);
  assert.equal(results.find(r => r.status === 'rejected').reason.code, 'run_storage_full');
});

test('completed native runs still backfill retained events rather than losing terminal history', async t => {
  const { gateway, engine, call } = fixture(t);
  gateway.runs.set('default:r', { run_id: 'r', status: 'completed', session_id: 's' });
  await call('run.events', { runId: 'r' }); await collected(engine);
  assert.equal(gateway.streamCalls, 1);
  const page = await call('run.events', { runId: 'r' });
  assert.equal(page.events.length, 2); assert.equal(page.state, 'settled'); assert.equal(gateway.streamCalls, 1);
});

test('restart resumes unsettled collectors with persisted native sequence, without any write', async t => {
  const { gateway, engine, dir } = fixture(t);
  const c = engine.context(connection(), 'default'); engine.store.ensureRun(c.scope, 'r', 's');
  engine.store.append(c.scope, 'r', event('r', 0)); engine.store.updateRun(c.scope, 'r', { state: 'observing', status: 'running' });
  await engine.close();
  gateway.runs.set('default:r', { run_id: 'r', status: 'running', session_id: 's' });
  gateway.onStream = id => stream(frames([event(id, 1, 'run.completed')]));
  const restarted = new Engine(dir, { fetch: gateway.fetch }); t.after(() => restarted.close());
  await restarted.call(connection(), 'hermes.run.events', { profile: 'default', runId: 'r' }); await collected(restarted);
  const page = restarted.store.page(c.scope, 'r'); assert.deepEqual(page.events.map(e => e.seq), [0, 1]);
  assert.equal(gateway.calls.find(c => c.url.endsWith('/events')).headers['Last-Event-ID'], '0');
  assert.equal(gateway.writes().length, 0);
});

test('oversized and secret-bearing events are bounded/redacted before durable persistence', async t => {
  const { gateway, engine, call } = fixture(t);
  gateway.onStream = id => stream(frames([event(id, 0, 'message.delta', { delta: `echo ${secret}`, access_token: secret }), event(id, 1, 'message.delta', { delta: 'a'.repeat(70000) })]));
  await call('run.create', { operationId: 'limits', sessionId: 's', input: 'hello' }); await collected(engine);
  const page = engine.store.page(engine.context(connection(), 'default').scope, 'run_limits');
  assert.equal(page.error, 'event_too_large'); assert.equal(page.events.length, 1);
  assert.equal(page.events[0].delta, 'echo [redacted]'); assert.equal(page.events[0].access_token, '[redacted]');
  assert.ok(!JSON.stringify(page).includes(secret));
});

test('connection revocation cancels only that connection event reader', async t => {
  const { gateway, engine, call } = fixture(t);
  let cancelled = 0;
  gateway.onStream = () => new Response(new ReadableStream({ cancel() { cancelled++; } }), { headers: { 'content-type': 'text/event-stream' } });
  await call('run.create', { operationId: 'cancel', sessionId: 's', input: 'hello' });
  const jobs = [...engine.jobs.values()].map(j => j.promise); await Promise.resolve();
  engine.revoke('unrelated'); assert.equal(cancelled, 0);
  engine.revoke('account-a'); await Promise.all(jobs); assert.equal(cancelled, 1);
  assert.equal(gateway.writes().length, 1); // Revocation does not invoke gateway stop or any new run.
});

test('non-generation legacy approval uses exact request ID; stale/wrong-profile controls cannot write', async t => {
  const { gateway, call, engine } = fixture(t);
  gateway.runs.set('default:r', { run_id: 'r', status: 'waiting_for_approval', approval: { request_id: 'a' } });
  await call('run.approval', { operationId: 'legacy', runId: 'r', requestId: 'a', choice: 'once' });
  assert.deepEqual(gateway.writes()[0].body, { request_id: 'a', choice: 'once' });
  await assert.rejects(call('run.approval', { operationId: 'wrong', runId: 'r', requestId: 'foreign', choice: 'once' }), /prompt_not_pending/);
  const conn = connection('other', [{ id: 'lab', baseUrl: 'https://gateway.example/p/lab' }]);
  await assert.rejects(engine.call(conn, 'hermes.run.approval', { profile: 'lab', operationId: 'cross', runId: 'r', requestId: 'a', choice: 'once' }), /gateway_http_404/);
  assert.equal(gateway.writes().length, 1);
});

test('every declared operation uses a finite JSON bridge response suitable for peer buffering', async t => {
  const { gateway, engine, conn } = fixture(t), b = new Bridge(engine), origin = 'http://127.0.0.1:9999';
  gateway.runs.set('default:r', { run_id: 'r', status: 'completed', session_id: 's' });
  const req = { method: 'POST', url: new URL(origin + '/operations/hermes.run.events'), headers: new Headers() };
  b.authorize(req, conn, origin);
  await b.dispatch(req.method, req.url.pathname, req.headers.get('authorization'), { profile: 'default', runId: 'r' }); await collected(engine);
  b.authorize(req, conn, origin);
  const result = await b.dispatch(req.method, req.url.pathname, req.headers.get('authorization'), { profile: 'default', runId: 'r', limit: 1 });
  const peerWire = Buffer.from(JSON.stringify(result)).toString('base64');
  assert.equal(JSON.parse(Buffer.from(peerWire, 'base64').toString()).events[0].seq, 0);
  assert.ok(Buffer.byteLength(JSON.stringify(result)) < 1024 * 1024); assert.equal(result.hasMore, true);
});

test('130 terminal turns reuse 64 hot slots without ever losing old action protection, including restart', async t => {
  const { gateway, engine, call, dir } = fixture(t);
  let first;
  for (let i = 0; i < 130; i++) {
    const receipt = await call('run.create', { operationId: `turn-${i}`, sessionId: 's', input: `turn ${i}` });
    first ??= receipt; await collected(engine);
  }
  const stats = await call('retention.status');
  assert.equal(stats.scopedHotRuns, 64); assert.equal(stats.scopedRetiredRuns, 66); assert.equal(stats.scopedReceipts, 130);
  assert.equal(stats.receiptCountLimit, null); assert.equal(stats.immutableReceiptsPreserved, true);
  assert.deepEqual(await call('run.create', { operationId: 'turn-0', sessionId: 's', input: 'turn 0' }), first);
  await assert.rejects(call('run.create', { operationId: 'turn-0', sessionId: 's', input: 'changed' }), /operation_identity_conflict/);
  const beforeReads = gateway.calls.length, page = await call('run.events', { runId: first.result.run_id });
  assert.equal(page.retired, true); assert.equal(page.coverage.complete, false); assert.equal(page.coverage.reason, 'terminal_snapshot_retired');
  assert.equal(page.events.length, 0); assert.equal(gateway.calls.length, beforeReads); assert.equal(engine.jobs.size, 0);
  await engine.close();
  const reopened = new Engine(dir, { fetch: gateway.fetch }); t.after(() => reopened.close());
  assert.deepEqual(await reopened.call(connection(), 'hermes.run.create', { profile: 'default', operationId: 'turn-0', sessionId: 's', input: 'turn 0' }), first);
  assert.equal(gateway.writes().length, 130); assert.equal(reopened.jobs.size, 0);
});

test('explicit retirement is scoped, repeatable, offline and preserves action and prompt claims', async t => {
  const { gateway, engine, call } = fixture(t);
  const receipt = await call('run.create', { operationId: 'retire', sessionId: 's', input: 'hello' }); await collected(engine);
  const c = engine.context(connection(), 'default');
  const claim = engine.store.reserve(c.scope, 'old-answer', 'answer-fingerprint', { kind: 'run.approval', runId: receipt.result.run_id, controlTarget: 'original-prompt' }).receipt;
  engine.store.finish(c.scope, 'old-answer', { ...claim, status: 'accepted' });
  const first = await call('run.retire', { runId: receipt.result.run_id });
  gateway.online = false;
  assert.deepEqual(await call('run.retire', { runId: receipt.result.run_id }), first);
  assert.deepEqual(await call('action.get', { operationId: 'retire' }), receipt);
  await assert.rejects(engine.call(connection('other'), 'hermes.run.retire', { profile: 'default', runId: receipt.result.run_id }), /run_not_observed/);
  assert.equal(engine.store.db.prepare('SELECT COUNT(*) n FROM events WHERE scope=?').get(c.scope).n, 0);
  assert.throws(() => engine.store.reserve(c.scope, 'repeat-answer', 'new-fingerprint', { kind: 'run.approval', runId: receipt.result.run_id, controlTarget: 'original-prompt' }), /prompt_already_attempted/);
  assert.equal(gateway.writes().length, 1);
});

test('active, pending, unknown and uncertain-answer snapshots cannot be retired', async t => {
  const { engine, call } = fixture(t, { limits: { runs: 4 } }), c = engine.context(connection(), 'default');
  for (const [id, state, status] of [['active', 'observing', 'running'], ['pending', 'paused', 'waiting_for_approval'], ['unknown', 'settled', 'unknown'], ['answer', 'settled', 'completed']]) {
    engine.store.ensureRun(c.scope, id); engine.store.updateRun(c.scope, id, { state, status });
    if (id === 'answer') engine.store.reserve(c.scope, 'uncertain-answer', 'fp', { kind: 'run.clarify', runId: id, controlTarget: 'native-prompt' });
    await assert.rejects(call('run.retire', { runId: id }), /run_not_settled|uncertain_action/);
  }
  await assert.rejects(call('run.create', { operationId: 'full', sessionId: 's', input: 'hi' }), /run_storage_full/);
  assert.equal((await call('retention.status')).scopedHotRuns, 4);
});

test('an uncertain admission retains its slot and receipt while unrelated completed turns retire', async t => {
  const { gateway, engine, call } = fixture(t, { limits: { runs: 2 } }); gateway.failWrite = true;
  const uncertain = await call('run.create', { operationId: 'lost', sessionId: 's', input: 'lost' }); gateway.failWrite = false;
  for (let i = 0; i < 8; i++) { await call('run.create', { operationId: `done-${i}`, sessionId: 's', input: 'ok' }); await collected(engine); }
  assert.deepEqual(await call('run.create', { operationId: 'lost', sessionId: 's', input: 'lost' }), uncertain);
  const stats = await call('retention.status');
  assert.equal(stats.scopedPendingAdmissionSlots, 1); assert.equal(stats.scopedUncertainActions, 1); assert.equal(stats.scopedHotRuns, 1); assert.equal(stats.scopedRetiredRuns, 7);
  assert.equal(gateway.writes().length, 9);
});

test('capacity retirement never crosses account/profile scope or an in-flight collector', async t => {
  const { engine, call, gateway } = fixture(t, { limits: { runs: 1 } });
  const c = engine.context(connection(), 'default'); engine.store.ensureRun(c.scope, 'done'); engine.store.updateRun(c.scope, 'done', { state: 'settled', status: 'completed' });
  await assert.rejects(engine.call(connection('other'), 'hermes.run.create', { profile: 'default', operationId: 'x', sessionId: 's', input: 'hi' }), /run_storage_full/);
  const key = `${c.scope}:done`; engine.jobs.set(key, { controller: new AbortController(), promise: Promise.resolve(), connectionId: connection().id });
  await assert.rejects(call('run.retire', { runId: 'done' }), /collector_active/);
  await assert.rejects(call('run.create', { operationId: 'x', sessionId: 's', input: 'hi' }), /run_storage_full/);
  engine.jobs.delete(key); assert.equal(gateway.writes().length, 0);
});

test('immutable ledger has no 2048-action lifetime ceiling; no receipt is expired to free capacity', async t => {
  const { engine } = fixture(t), s = engine.store;
  for (let i = 0; i < 2055; i++) {
    const { receipt } = s.reserve('scope', `key${i}`, `fingerprint${i}`, { kind: 'session.create' });
    s.finish('scope', `key${i}`, { ...receipt, status: 'accepted', sessionId: `session${i}` });
  }
  assert.equal(s.retention('scope').scopedReceipts, 2055);
  assert.equal(s.reserve('scope', 'key0', 'fingerprint0', {}).fresh, false);
  assert.throws(() => s.reserve('scope', 'key0', 'different', {}), /operation_identity_conflict/);
});
