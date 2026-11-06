import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Engine } from '../lib/engine.js';

export const secret = 'mock-gateway-secret-only-for-tests';
export const connection = (id = 'account-a', rows = [{ id: 'default', baseUrl: 'http://127.0.0.1:8642' }]) => ({ id, config: { profiles: rows }, credentials: { tokens: Object.fromEntries(rows.map(r => [r.id, secret])) } });
export const caps = () => ({ object: 'hermes.api_server.capabilities', platform: 'hermes-agent', auth: { type: 'bearer', required: true },
  features: { session_resources: true, run_submission: true, run_status: true, run_events_sse: true, run_stop: true, run_approval_response: true, run_unknown_resolution: true, runs_idempotency: { supported: true, durable: true, retention_seconds: 86400 } }, endpoints: {} });
const json = (v, status = 200) => new Response(JSON.stringify(v), { status, headers: { 'content-type': 'application/json' } });
export function event(runId, seq, name = 'message.delta', fields = {}) { return { event: name, run_id: runId, timestamp: 123, seq, ...fields }; }
export function frames(events) { return events.map(e => `${e.seq !== undefined ? `id: ${e.seq}\n` : ''}data: ${JSON.stringify(e)}\n\n`).join(''); }
export function stream(text, { split = 7 } = {}) {
  const b = Buffer.from(text);
  return new Response(new ReadableStream({ start(c) { for (let i = 0; i < b.length; i += split) c.enqueue(b.subarray(i, i + split)); c.close(); } }), { headers: { 'content-type': 'text/event-stream' } });
}
export class Gateway {
  calls = []; sessions = new Map(); runs = new Map(); cap = caps(); online = true; auth = true; failWrite = false; streamCalls = 0;
  onStream = (runId) => stream(frames([event(runId, 0, 'message.delta', { delta: 'hello' }), event(runId, 1, 'run.completed', { output: 'hello' })]));
  fetch = async (url, init = {}) => {
    this.calls.push({ url, method: init.method ?? 'GET', headers: { ...init.headers }, body: init.body ? JSON.parse(init.body) : undefined });
    if (!this.online) throw new Error(`never expose ${secret}`);
    if (!this.auth || init.headers.authorization !== `Bearer ${secret}`) return json({ error: secret }, 401);
    const u = new URL(url), profile = u.pathname.match(/^\/p\/([^/]+)/)?.[1] ?? 'default';
    const route = u.pathname.replace(/^\/p\/[^/]+/, ''), key = id => `${profile}:${id}`;
    const body = init.body ? JSON.parse(init.body) : {};
    if (route === '/v1/capabilities') return json(this.cap);
    if (route === '/api/sessions' && init.method === 'GET') return json({ object: 'list', data: [...this.sessions.entries()].filter(([k]) => k.startsWith(profile + ':')).map(([, s]) => s), limit: Number(u.searchParams.get('limit')), offset: Number(u.searchParams.get('offset')), has_more: false });
    if (route === '/api/sessions' && init.method === 'POST') {
      if (this.sessions.has(key(body.id))) return json({}, 409);
      this.sessions.set(key(body.id), { id: body.id, source: body.source, title: body.title });
      if (this.failWrite) throw new Error('accepted but response lost');
      return json({ object: 'hermes.session', session: this.sessions.get(key(body.id)) }, 201);
    }
    const session = route.match(/^\/api\/sessions\/([^/]+)(\/messages)?$/);
    if (session) {
      if (!this.sessions.has(key(decodeURIComponent(session[1])))) return json({}, 404);
      return json(session[2] ? { object: 'list', session_id: session[1] + '_tip', data: [{ id: 'msg1', role: 'tool', content: 'result', tool_call_id: 'call1' }], pagination: Object.fromEntries(u.searchParams) } : { object: 'hermes.session', session: this.sessions.get(key(session[1])) });
    }
    if (route === '/v1/runs' && init.method === 'POST') {
      // Match the native gateway: arrays are messages, not bare content parts.
      if (Array.isArray(body.input) && !body.input.at(-1)?.content) return json({ error: 'No user message found in input' }, 400);
      // Hermes persists native `text` parts and image_url markers; input_text would be lost from history.
      if (Array.isArray(body.input) && body.input.at(-1).content.some(p => !['text', 'image_url'].includes(p.type))) return json({ error: 'Non-native content type' }, 400);
      const id = 'run_' + init.headers['Idempotency-Key'].replaceAll('/', '_');
      this.runs.set(key(id), { run_id: id, session_id: body.session_id, status: 'running' });
      if (this.failWrite) throw new Error('accepted but response lost');
      return json({ run_id: id, status: 'started' }, 202);
    }
    const run = route.match(/^\/v1\/runs\/([^/]+)(?:\/(events|stop|approval|clarify))?$/);
    if (run) {
      const id = decodeURIComponent(run[1]), row = this.runs.get(key(id)); if (!row) return json({}, 404);
      if (run[2] === 'events') { this.streamCalls++; return this.onStream(id, init, profile); }
      if (run[2]) { if (this.failWrite) throw new Error('answer accepted but reply lost'); return json({ run_id: id, accepted: true, ...body }); }
      return json(row);
    }
    return json({}, 404);
  };
  writes() { return this.calls.filter(c => c.method === 'POST'); }
}
export function fixture(t, options = {}) {
  const dir = fs.mkdtempSync(path.join(path.dirname(fileURLToPath(import.meta.url)), '.fixture-'));
  const gateway = new Gateway(), engine = new Engine(dir, { fetch: gateway.fetch, reconnectMs: 1, ...options });
  t.after(async () => { if (!engine.closed) await engine.close(); fs.rmSync(dir, { recursive: true, force: true }); });
  return { dir, gateway, engine, conn: connection(), call: (name, a = {}) => engine.call(connection(), `hermes.${name}`, { profile: 'default', ...a }) };
}
export async function collected(engine) { await Promise.all([...engine.jobs.values()].map(j => j.promise)); }
