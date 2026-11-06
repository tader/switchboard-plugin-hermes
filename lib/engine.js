import crypto from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Store } from './store.js';
import { Fault, operations, validate } from './operations.js';
import { sse } from './sse.js';

const hash = s => crypto.createHash('sha256').update(s).digest('hex');
const canonical = v => v && typeof v === 'object' ? Array.isArray(v) ? v.map(canonical) : Object.fromEntries(Object.keys(v).sort().map(k => [k, canonical(v[k])])) : v;
const fingerprint = v => hash(JSON.stringify(canonical(v)));
const MAX_RUN_IMAGE_BYTES = 4 * 1024 * 1024;
function validateRunInput(input) {
  if (typeof input === 'string') return; // Preserve the established text-only API exactly.
  let imageBytes = 0, imageCount = 0, textBytes = 0;
  for (const block of input) {
    if (block.type === 'input_text') {
      textBytes += Buffer.byteLength(block.text, 'utf8');
      if (textBytes > 65536) throw new Fault('run_input_too_large');
      continue;
    }
    imageCount++;
    const match = /^data:image\/(png|jpeg|webp);base64,([A-Za-z0-9+/]*={0,2})$/.exec(block.image_url);
    if (!match || !match[2] || match[2].length % 4 !== 0) throw new Fault('invalid_image_data');
    const bytes = Buffer.from(match[2], 'base64');
    if (bytes.toString('base64') !== match[2]) throw new Fault('invalid_image_data');
    imageBytes += bytes.length;
    if (imageBytes > MAX_RUN_IMAGE_BYTES || imageCount > 4) throw new Fault('run_images_too_large');
  }
}
const terminal = new Set(['completed', 'failed', 'cancelled', 'interrupted', 'unknown']);
const safeId = x => typeof x === 'string' && x.length > 0 && x.length <= 255 && !/[\x00-\x1f\x7f]/.test(x);
export function profiles(config) {
  let rows;
  try { rows = typeof config.profiles === 'string' ? JSON.parse(config.profiles) : config.profiles; }
  catch { throw new Fault('invalid_profile_configuration'); }
  if (!Array.isArray(rows) || rows.length < 1 || rows.length > 16) throw new Fault('invalid_profile_configuration');
  const seen = new Set();
  return rows.map(row => {
    if (!row || Object.keys(row).some(k => !['id', 'baseUrl'].includes(k)) || !/^[A-Za-z0-9_-]{1,64}$/.test(row.id) || seen.has(row.id)) throw new Fault('invalid_profile_configuration');
    seen.add(row.id); let u;
    try { u = new URL(row.baseUrl); } catch { throw new Fault('invalid_gateway_url'); }
    if (!['http:', 'https:'].includes(u.protocol) || u.username || u.password || u.search || u.hash || !/^\/(?:p\/[A-Za-z0-9_-]+\/?)?$/.test(u.pathname)) throw new Fault('invalid_gateway_url');
    // HTTP is only permitted on loopback. Remote gateway traffic must use TLS.
    if (u.protocol === 'http:' && !['127.0.0.1', '[::1]', 'localhost'].includes(u.hostname)) throw new Fault('gateway_tls_required');
    const prefix = u.pathname.split('/').filter(Boolean);
    if (prefix.length && prefix[1] !== row.id) throw new Fault('profile_route_mismatch');
    if (!prefix.length && row.id !== 'default') throw new Fault('named_profile_requires_route');
    return { id: row.id, baseUrl: u.href.replace(/\/$/, '') };
  });
}
export function tokens(value, rows) {
  let map; try { map = typeof value === 'string' ? JSON.parse(value) : value; } catch { throw new Fault('invalid_gateway_credentials'); }
  if (!map || typeof map !== 'object' || Array.isArray(map) || Object.keys(map).some(k => !rows.some(r => r.id === k)) || rows.some(r => typeof map[r.id] !== 'string' || map[r.id].length < 16 || map[r.id].length > 4096 || /[\r\n\x00]/.test(map[r.id]))) throw new Fault('invalid_gateway_credentials');
  return Object.fromEntries(rows.map(r => [r.id, map[r.id]]));
}
export function scrub(value, secrets) {
  if (typeof value === 'string') { for (const s of secrets) if (s) value = value.split(s).join('[redacted]'); return value; }
  if (Array.isArray(value)) return value.map(v => scrub(v, secrets));
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, /^(authorization|password|api_key|access_token|refresh_token|token)$/i.test(k) ? '[redacted]' : scrub(v, secrets)]));
  return value;
}
async function boundedJson(response) {
  const reader = response.body?.getReader(); if (!reader) throw new Fault('invalid_gateway_response', 502);
  let bytes = 0; const parts = [];
  try { for (;;) { const r = await reader.read(); if (r.done) break; bytes += r.value.length; if (bytes > 524288) throw new Fault('gateway_response_too_large', 502); parts.push(Buffer.from(r.value)); } }
  finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
  try { return JSON.parse(Buffer.concat(parts).toString('utf8')); } catch { throw new Fault('invalid_gateway_json', 502); }
}
export class Engine {
  constructor(dir, { fetch: fetcher = globalThis.fetch, limits, reconnectMs = 1000, streamMs = 90000 } = {}) {
    this.store = new Store(dir, limits); this.fetch = fetcher; this.jobs = new Map(); this.identities = new Map(); this.errors = new Map(); this.closed = false;
    this.store.isBusy = (scope, runId) => this.jobs.has(`${scope}:${runId}`);
    this.reconnectMs = reconnectMs; this.streamMs = streamMs;
  }
  observe(conn) {
    const rows = profiles(conn.config), keys = tokens(conn.credentials.tokens, rows);
    const identity = fingerprint({ rows, keys });
    if (this.identities.has(conn.id) && this.identities.get(conn.id) !== identity) this.revoke(conn.id);
    this.identities.set(conn.id, identity);
    return { rows, keys };
  }
  context(conn, profile) {
    const { rows, keys } = this.observe(conn); const row = rows.find(r => r.id === profile);
    if (!row) throw new Fault('profile_not_authorized', 403);
    return { ...row, token: keys[row.id], secrets: Object.values(keys), scope: fingerprint({ connection: conn.id, profile: row.id, baseUrl: row.baseUrl, principal: hash(keys[row.id]) }), connectionId: conn.id };
  }
  async request(c, method, route, body, signal, headers = {}) {
    let response;
    try { response = await this.fetch(`${c.baseUrl}${route}`, { method, headers: { authorization: `Bearer ${c.token}`, accept: 'application/json', ...(body ? { 'content-type': 'application/json' } : {}), ...headers }, body: body ? JSON.stringify(body) : undefined,
      redirect: 'error', signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(15000)]) : AbortSignal.timeout(15000) }); }
    catch { throw new Fault('gateway_transport_error', 502); }
    if (!response.ok) { await response.body?.cancel().catch(() => {}); throw new Fault(response.status === 401 || response.status === 403 ? 'gateway_auth_required' : `gateway_http_${response.status}`, response.status === 401 || response.status === 403 ? response.status : 502); }
    return scrub(await boundedJson(response), c.secrets);
  }
  async capabilities(c, signal) {
    const cap = await this.request(c, 'GET', '/v1/capabilities', undefined, signal);
    if (cap.object !== 'hermes.api_server.capabilities' || cap.platform !== 'hermes-agent' || !cap.features || !cap.endpoints) throw new Fault('unsupported_gateway_contract', 409);
    // The API's feature keys are not custom Switchboard operation IDs.
    return cap;
  }
  require(cap, feature) { if (cap.features[feature] !== true) throw new Fault(`unsupported_${feature}`, 409); }
  async mutation(c, name, a, body, route, preflight, signal, meta = {}) {
    if (!/^[\x21-\x7e]{1,255}$/.test(a.operationId)) throw new Fault('invalid_operation_identity');
    const fp = fingerprint({ name, body, route });
    const previous = this.store.action(c.scope, a.operationId);
    if (previous) return this.store.reserve(c.scope, a.operationId, fp, {}).receipt;
    await preflight();
    const { fresh, receipt } = this.store.reserve(c.scope, a.operationId, fp, { kind: name, ...meta });
    if (!fresh) return receipt;
    try {
      const result = await this.request(c, 'POST', route, body, signal, name === 'run.create' ? { 'Idempotency-Key': a.operationId } : {});
      return this.store.finish(c.scope, a.operationId, { ...receipt, status: 'accepted', result });
    } catch (error) {
      // Even HTTP 5xx, unreadable replies and aborts can follow a committed upstream action.
      const definitive = error.code === 'gateway_auth_required' || /^gateway_http_(400|404|409|422|429)$/.test(error.code ?? '');
      return this.store.finish(c.scope, a.operationId, { ...receipt, status: definitive ? 'rejected' : 'uncertain', error: error instanceof Fault ? error.code : 'local_receipt_failure' });
    }
  }
  async status(c, runId, signal) {
    const status = await this.request(c, 'GET', `/v1/runs/${encodeURIComponent(runId)}`, undefined, signal);
    if (status.run_id !== runId || typeof status.status !== 'string') throw new Fault('invalid_run_identity', 502);
    this.store.ensureRun(c.scope, runId, status.session_id);
    this.store.updateRun(c.scope, runId, { status: status.status, ...(safeId(status.session_id) ? { sessionId: status.session_id } : {}) });
    return status;
  }
  async call(conn, operationId, args, signal) {
    if (this.closed) throw new Fault('plugin_stopping', 503);
    const op = operations.find(o => o.id === operationId); if (!op) throw new Fault('unknown_operation', 404);
    validate(op.input, args);
    if (operationId === 'hermes.run.create') validateRunInput(args.input);
    if (operationId === 'hermes.profiles.list') {
      const { rows } = this.observe(conn), results = [];
      for (const row of rows) {
        try { results.push({ id: row.id, available: true, capabilities: await this.capabilities(this.context(conn, row.id), signal) }); }
        catch (e) { results.push({ id: row.id, available: false, error: e instanceof Fault ? e.code : 'gateway_unavailable' }); }
      }
      return { profiles: results, inventory: 'configured_authorized_endpoints_verified_live', exhaustive: false };
    }
    const c = this.context(conn, args.profile), name = operationId.slice(7);
    if (name === 'retention.status') return this.store.retention(c.scope);
    if (name === 'run.retire') {
      const retired = this.store.retire(c.scope, args.runId);
      this.errors.delete(`${c.scope}:${args.runId}`);
      return retired;
    }
    if (name === 'action.get') {
      const r = this.store.action(c.scope, args.operationId); if (!r) throw new Fault('action_not_found', 404);
      return { ...r, status: r.status === 'reserved' ? 'uncertain' : r.status };
    }
    if (name === 'capabilities.get') return this.capabilities(c, signal);
    if (name === 'sessions.list') {
      const cap = await this.capabilities(c, signal); this.require(cap, 'session_resources');
      return this.request(c, 'GET', `/api/sessions?limit=${args.limit ?? 50}&offset=${args.offset ?? 0}`, undefined, signal);
    }
    if (name === 'session.get' || name === 'session.history') {
      const cap = await this.capabilities(c, signal); this.require(cap, 'session_resources');
      const route = `/api/sessions/${encodeURIComponent(args.sessionId)}` + (name === 'session.history' ? `/messages?limit=${args.limit ?? 100}&offset=${args.offset ?? 0}&order=${args.order ?? 'latest'}` : '');
      return this.request(c, 'GET', route, undefined, signal);
    }
    if (name === 'session.create') {
      const sessionId = `sb_${hash(`${c.scope}:${args.operationId}`).slice(0, 40)}`;
      const body = { id: sessionId, source: 'api_server', ...(args.title !== undefined ? { title: args.title } : {}) };
      const receipt = await this.mutation(c, name, args, body, '/api/sessions', async () => { const cap = await this.capabilities(c, signal); this.require(cap, 'session_resources'); }, signal, { sessionId });
      if (receipt.status === 'accepted' && receipt.result?.session?.id !== sessionId) return this.store.finish(c.scope, args.operationId, { ...receipt, status: 'uncertain', error: 'session_identity_mismatch' });
      return receipt;
    }
    if (name === 'run.create') {
      // The gateway /v1/runs API expects multimodal parts inside a user message.
      // Fingerprint the exact wire body so changed images conflict on replay.
      const input = Array.isArray(args.input) ? [{ role: 'user', content: args.input }] : args.input;
      const body = { session_id: args.sessionId, input };
      const receipt = await this.mutation(c, name, args, body, '/v1/runs', async () => {
        const cap = await this.capabilities(c, signal); this.require(cap, 'run_submission'); this.require(cap, 'run_events_sse');
        if (cap.features.runs_idempotency?.supported !== true || cap.features.runs_idempotency?.durable !== true) throw new Fault('durable_run_idempotency_required', 409);
        // The reserve transaction makes room only from eligible settled snapshots.
      }, signal);
      if (receipt.status === 'accepted') {
        const runId = receipt.result?.run_id;
        if (!safeId(runId)) return this.store.finish(c.scope, args.operationId, { ...receipt, status: 'uncertain', error: 'missing_run_identity' });
        if (!this.store.retired(c.scope, runId)) { this.store.ensureRun(c.scope, runId, args.sessionId, args.operationId); this.collect(c, runId); }
      }
      return receipt;
    }
    if (name === 'run.status' || name === 'run.events') {
      if (name === 'run.events' && this.store.retired(c.scope, args.runId)) return { ...this.store.page(c.scope, args.runId, args.afterSeq ?? -1, args.limit ?? 100, args.snapshotRevision ?? 0), observing: false, upstreamStatus: 'not_requested' };
      let status;
      try {
        const cap = await this.capabilities(c, signal); this.require(cap, 'run_status');
        status = await this.status(c, args.runId, signal);
        if (cap.features.run_events_sse === true) this.collect(c, args.runId);
        else if (name === 'run.events') this.require(cap, 'run_events_sse');
      } catch (error) {
        if (name !== 'run.events' || !this.store.run(c.scope, args.runId)) throw error;
        this.jobs.get(`${c.scope}:${args.runId}`)?.controller.abort();
        this.store.updateRun(c.scope, args.runId, { state: 'paused', error: error instanceof Fault ? error.code : 'gateway_unavailable' });
      }
      if (name === 'run.status') return status;
      const page = this.store.page(c.scope, args.runId, args.afterSeq ?? -1, args.limit ?? 100, args.snapshotRevision ?? 0);
      return { ...page, observing: this.jobs.has(`${c.scope}:${args.runId}`), collectorError: this.errors.get(`${c.scope}:${args.runId}`), upstreamStatus: status?.status ?? 'unavailable' };
    }
    if (name === 'run.stop') return this.mutation(c, name, args, {}, `/v1/runs/${encodeURIComponent(args.runId)}/stop`, async () => { this.require(await this.capabilities(c, signal), 'run_stop'); }, signal, { runId: args.runId });
    if (name === 'run.approval' || name === 'run.clarify') {
      const kind = name === 'run.approval' ? 'approval' : 'clarify';
      const body = { request_id: args.requestId, ...(args.executionGeneration !== undefined ? { execution_generation: args.executionGeneration } : {}), [kind === 'approval' ? 'choice' : 'answer']: kind === 'approval' ? args.choice : args.answer };
      return this.mutation(c, name, args, body, `/v1/runs/${encodeURIComponent(args.runId)}/${kind}`, async () => {
        const cap = await this.capabilities(c, signal);
        this.require(cap, kind === 'approval' ? 'run_approval_response' : 'run_unknown_resolution');
        const current = await this.status(c, args.runId, signal);
        if (terminal.has(current.status)) throw new Fault('prompt_not_pending', 409);
        const authority = Array.isArray(current.pending_controls) && Number.isSafeInteger(current.execution_generation);
        if (kind === 'clarify' && !authority) throw new Fault('unsupported_clarify_runtime', 409);
        const prompt = authority ? current.pending_controls.find(p => p.kind === kind && p.prompt_id === args.requestId) : current.approval;
        if (!prompt || (prompt.prompt_id ?? prompt.request_id) !== args.requestId) throw new Fault('prompt_not_pending', 409);
        if (authority && args.executionGeneration !== current.execution_generation || !authority && args.executionGeneration !== undefined) throw new Fault('stale_execution_generation', 409);
        if (authority && prompt.execution_generation !== undefined && prompt.execution_generation !== args.executionGeneration) throw new Fault('stale_execution_generation', 409);
        // The durable reserve transaction claims this prompt before the POST, including
        // concurrent answers with distinct operation IDs. Uncertain claims are never released.
      }, signal, { runId: args.runId, controlTarget: fingerprint({ run: args.runId, prompt: args.requestId, generation: args.executionGeneration, kind }) });
    }
    throw new Fault('unknown_operation', 404);
  }
  collect(c, runId) {
    const key = `${c.scope}:${runId}`;
    if (this.closed || this.jobs.has(key) || this.store.retired(c.scope, runId) || this.store.run(c.scope, runId)?.state === 'settled') return;
    const controller = new AbortController();
    this.errors.delete(key);
    const job = { controller, connectionId: c.connectionId };
    job.promise = this.collectLoop(c, runId, controller.signal).catch(() => {
      if (!this.closed) {
        this.errors.set(key, 'collector_or_storage_failed');
        try { this.store.updateRun(c.scope, runId, { state: 'paused', error: 'collector_failed' }); } catch { /* Storage may be full; never crash the host or resume writes. */ }
      }
    }).finally(() => { if (this.jobs.get(key) === job) this.jobs.delete(key); });
    this.jobs.set(key, job);
  }
  async collectLoop(c, runId, signal) {
    let failures = 0;
    while (!signal.aborted && !this.closed) {
      const row = this.store.run(c.scope, runId);
      if (row.state === 'settled') return;
      this.store.updateRun(c.scope, runId, { state: 'observing', error: null });
      try {
        const response = await this.fetch(`${c.baseUrl}/v1/runs/${encodeURIComponent(runId)}/events`, {
          headers: { authorization: `Bearer ${c.token}`, accept: 'text/event-stream', ...(row.highSeq >= 0 ? { 'Last-Event-ID': String(row.highSeq) } : {}) },
          redirect: 'error', signal: AbortSignal.any([signal, AbortSignal.timeout(this.streamMs)]),
        });
        if (!response.ok) {
          await response.body?.cancel().catch(() => {});
          if ([401, 403, 404].includes(response.status)) { this.store.updateRun(c.scope, runId, { state: 'paused', error: [401, 403].includes(response.status) ? 'gateway_auth_required' : 'upstream_replay_unavailable' }); return; }
          throw new Fault('event_transport_error', 502);
        }
        if (!response.headers.get('content-type')?.startsWith('text/event-stream')) throw new Fault('invalid_event_content_type', 502);
        for await (const frame of sse(response.body, signal)) {
          const event = scrub(frame.data, c.secrets);
          if (event.run_id !== runId || typeof event.event !== 'string') throw new Fault('invalid_event_run_identity', 502);
          if (event.event === 'replay.truncated') { this.store.gap(c.scope, runId, event.oldest_retained_seq); continue; }
          if (frame.id !== String(event.seq)) throw new Fault('event_cursor_mismatch', 502);
          if (this.store.append(c.scope, runId, event)) failures = 0;
          if (event.event.startsWith('run.') && terminal.has(event.event.slice(4))) {
            this.store.updateRun(c.scope, runId, { state: 'settled', status: event.event.slice(4), terminalEventObserved: true }); return;
          }
        }
        // EOF does not prove completion. Check native run status; never start a new run.
        const current = await this.status(c, runId, signal);
        if (terminal.has(current.status)) { this.store.updateRun(c.scope, runId, { state: 'settled', terminalEventObserved: this.store.run(c.scope, runId).terminalEventObserved ?? false }); return; }
        throw new Fault('event_transport_error', 502); // Repeated early EOFs must back off and eventually pause.
      } catch (error) {
        if (signal.aborted || this.closed) return;
        failures++;
        const code = error instanceof Fault ? error.code : 'event_transport_error';
        this.store.updateRun(c.scope, runId, { state: 'paused', error: code });
        if (error instanceof Fault && !['gateway_transport_error', 'event_transport_error'].includes(code) || failures >= 5) return;
      }
      await delay(this.reconnectMs * Math.min(8, 2 ** failures), undefined, { signal, ref: false }).catch(() => {});
    }
  }
  revoke(connectionId) { for (const job of this.jobs.values()) if (job.connectionId === connectionId) job.controller.abort(); this.identities.delete(connectionId); }
  async close() { this.closed = true; for (const job of this.jobs.values()) job.controller.abort(); await Promise.allSettled([...this.jobs.values()].map(j => j.promise)); this.store.close(); }
}
