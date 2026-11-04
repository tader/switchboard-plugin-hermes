import http from 'node:http';
import crypto from 'node:crypto';
import { Engine, profiles, tokens } from './lib/engine.js';
import { Fault, operations, openapi } from './lib/operations.js';

/** SDK field compatibility is documented in types/README.md against immutable releases.
 * The SDK has no arbitrary HTTP handler registry. A private loopback bridge + ServiceDefinition
 * is the existing shell-command plugin pattern; peers exchange finite JSON, never upstream SSE.
 */
export class Bridge {
  constructor(engine) { this.engine = engine; this.pending = new Map(); }
  authorize(req, connection, origin) {
    if (req.url.origin !== origin || req.method !== 'POST' || req.url.search || !operations.some(o => req.url.pathname === `/operations/${o.id}`)) throw new Fault('bridge_route_not_allowed', 403);
    this.engine.observe(connection);
    for (const [k, v] of this.pending) if (v.expires < Date.now()) this.pending.delete(k);
    if (this.pending.size >= 128) throw new Fault('bridge_busy', 429);
    const nonce = crypto.randomBytes(32).toString('base64url');
    this.pending.set(nonce, { connection: structuredClone(connection), path: req.url.pathname, expires: Date.now() + 10000 });
    req.headers.set('authorization', `Bearer ${nonce}`);
  }
  async dispatch(method, pathname, authorization, body, signal) {
    const token = String(authorization ?? '').replace(/^Bearer /, '');
    const invocation = this.pending.get(token); this.pending.delete(token);
    if (!invocation || invocation.expires < Date.now() || invocation.path !== pathname || method !== 'POST') throw new Fault('bridge_auth_required', 401);
    return this.engine.call(invocation.connection, pathname.slice('/operations/'.length), body, signal);
  }
  revoke(id) { for (const [k, v] of this.pending) if (v.connection.id === id) this.pending.delete(k); this.engine.revoke(id); }
  async handle(request, response) {
    const controller = new AbortController();
    const aborted = () => controller.abort();
    request.on('aborted', aborted); response.on('close', aborted);
    try {
      const u = new URL(request.url, 'http://127.0.0.1'); if (u.search) throw new Fault('bridge_route_not_allowed', 403);
      let size = 0; const chunks = [];
      for await (const chunk of request) { size += chunk.length; if (size > 262144) throw new Fault('request_too_large', 413); chunks.push(chunk); }
      let body; try { body = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { throw new Fault('invalid_json'); }
      const result = await this.dispatch(request.method, u.pathname, request.headers.authorization, body, controller.signal);
      response.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }); response.end(JSON.stringify(result));
    } catch (error) {
      response.writeHead(error instanceof Fault ? error.status : 500, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      response.end(JSON.stringify({ error: error instanceof Fault ? error.code : 'plugin_internal_error' }));
    } finally { request.off('aborted', aborted); response.off('close', aborted); }
  }
}

/** @param {import('./types/api.d.ts').SetupContext} ctx */
export default async function setup(ctx) {
  const engine = new Engine(ctx.dataDir), bridge = new Bridge(engine);
  const server = http.createServer((req, res) => { void bridge.handle(req, res); });
  server.requestTimeout = 20000; server.headersTimeout = 10000; server.maxConnections = 64;
  try { await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); }); }
  catch (error) { await engine.close(); throw error; }
  const address = server.address();
  const baseUrl = `http://127.0.0.1:${address.port}`;
  return {
    services: [{
      id: 'hermes-gateway', name: 'Hermes Agent gateway', baseUrl, allowedHosts: [new URL(baseUrl).host],
      description: 'Provider-neutral native sessions, runs and durable JSON event batches on the gateway-owning Switchboard.',
      openapi: openapi(baseUrl),
      authMethods: [{
        id: 'gateway', name: 'Authorized gateway profiles',
        fields: [
          { key: 'profiles', label: 'Authorized native profile endpoints (JSON)', type: 'textarea', required: true,
            default: '[{"id":"default","baseUrl":"http://127.0.0.1:8642"}]',
            description: 'Only actual authorized profiles. Named profiles require their verified /p/<name> URL. No profile names are fabricated or scanned.' },
          { key: 'tokens', label: 'Gateway-only bearer keys by profile (JSON)', type: 'secret', required: true,
            description: 'A JSON object mapping each authorized profile ID to its API_SERVER_KEY. Provider OAuth/model credentials remain at Hermes.' },
        ],
        connect({ config }) {
          const rows = profiles(config), keys = tokens(config.tokens, rows);
          return { config: { profiles: rows }, credentials: { tokens: keys }, account: { id: crypto.createHash('sha256').update(JSON.stringify(rows)).digest('hex'), label: `Hermes (${rows.map(r => r.id).join(', ')})` } };
        },
        authorize: (req, conn) => bridge.authorize(req, conn, baseUrl),
        revoke: conn => bridge.revoke(conn.id),
      }],
    }],
    async dispose() {
      bridge.pending.clear(); server.closeAllConnections();
      await new Promise(resolve => server.close(resolve)); await engine.close();
    },
  };
}
