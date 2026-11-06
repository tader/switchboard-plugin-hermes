import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { Gateway, secret, event, frames } from '../mock.js';
import { operations } from '../../lib/operations.js';

const pluginRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const releases = [
  { tag: 'v0.9.0', commit: 'af10250cab41991ae5f5214de1222bab3fd7de71', role: 'primary_deployed' },
  { tag: 'v0.8.3', commit: 'ec6fbc36abc25e087f44191d90c4572790acf152', role: 'minimum_supported' },
];
const coreRepo = process.env.SWITCHBOARD_CORE_REPO;
const modules = process.env.SWITCHBOARD_CORE_NODE_MODULES ?? (coreRepo ? path.join(coreRepo, 'node_modules') : undefined);
async function waitFor(fn, label, ms = 15000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { const value = await fn(); if (value) return value; await delay(50); }
  throw new Error(`fixture_timeout_${label}`);
}
function listen(server) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve(server.address().port));
  });
}
async function availablePort() {
  const s = http.createServer(); const port = await listen(s); await new Promise(r => s.close(r)); return port;
}

for (const release of releases) test(`Switchboard ${release.tag} (${release.role}): real loader/OpenAPI/REST/two-core peer`, { timeout: 90000 }, async t => {
  const receiptFile = path.join(pluginRoot, `test/.integration-${release.tag}-result.json`), milestones = [];
  const scratch = fs.mkdtempSync(path.join(pluginRoot, 'test/.integration-'));
  const children = [], servers = [];
  let result = { status: 'running', core: release, milestones };
  t.after(async () => {
    for (const child of children) if (child.exitCode === null) child.kill('SIGTERM');
    for (const child of children) {
      await Promise.race([new Promise(r => child.exitCode !== null ? r() : child.once('exit', r)), delay(4000)]);
      if (child.exitCode === null) { child.kill('SIGKILL'); await new Promise(r => child.once('exit', r)); }
    }
    for (const s of servers) { s.closeAllConnections(); await new Promise(r => s.close(r)); }
    fs.rmSync(scratch, { recursive: true, force: true });
    fs.writeFileSync(receiptFile, JSON.stringify({ ...result, finishedAt: new Date().toISOString() }, null, 2) + '\n');
  });
  try {
    assert.ok(coreRepo, 'Set SWITCHBOARD_CORE_REPO to a local core repository containing both immutable release commits; no personal path default or automatic fetch is used.');
    // Export the tag into fixture-owned storage: no checkout, branch, fetch or core edit.
    const commit = execFileSync('git', ['-C', coreRepo, 'rev-parse', `${release.commit}^{commit}`], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
    assert.equal(commit, release.commit, 'fixture must export inspected immutable release commit');
    const core = path.join(scratch, 'core'); fs.mkdirSync(core);
    const archive = execFileSync('git', ['-C', coreRepo, 'archive', '--format=tar', release.commit], { maxBuffer: 64 * 1024 * 1024 });
    execFileSync('tar', ['-xf', '-', '-C', core], { input: archive });
    assert.equal(JSON.parse(fs.readFileSync(path.join(core, 'package.json'))).version, release.tag.slice(1));
    // Reuse read-only installed modules only if every direct runtime version matches the release lock.
    const lock = JSON.parse(fs.readFileSync(path.join(core, 'package-lock.json')));
    for (const name of Object.keys(JSON.parse(fs.readFileSync(path.join(core, 'package.json'))).dependencies)) {
      const installed = JSON.parse(fs.readFileSync(path.join(modules, name, 'package.json'))).version;
      assert.equal(installed, lock.packages[`node_modules/${name}`].version, `release dependency mismatch: ${name}`);
    }
    fs.symlinkSync(path.resolve(modules), path.join(core, 'node_modules'), 'dir');
    milestones.push('immutable_released_core_and_locked_direct_dependencies_verified');

    const gateway = new Gateway(); let held;
    gateway.onStream = id => new Response(new ReadableStream({ start(controller) {
      held = { id, controller }; controller.enqueue(Buffer.from(frames([event(id, 0, 'message.delta', { delta: 'peer-live-before-eof' })])));
    } }), { headers: { 'content-type': 'text/event-stream' } });
    const upstream = http.createServer(async (request, response) => {
      try {
        let body = ''; for await (const chunk of request) body += chunk;
        const headers = Object.fromEntries(Object.entries(request.headers).map(([k, v]) => [k === 'idempotency-key' ? 'Idempotency-Key' : k, v]));
        const res = await gateway.fetch(`http://127.0.0.1${request.url}`, { method: request.method, headers, body: body || undefined });
        response.writeHead(res.status, Object.fromEntries(res.headers)); response.flushHeaders();
        const reader = res.body?.getReader(); if (!reader) { response.end(); return; }
        response.once('close', () => { void reader.cancel().catch(() => {}); });
        try { for (;;) { const r = await reader.read(); if (r.done) break; response.write(r.value); } }
        finally { reader.releaseLock(); }
        response.end();
      } catch { if (!response.headersSent) response.writeHead(500); response.end(); }
    });
    servers.push(upstream);
    const gatewayPort = await listen(upstream); milestones.push('real_mock_gateway_socket');

    async function launch(role, extra = {}) {
      const data = path.join(scratch, role), home = path.join(scratch, `${role}-home`);
      fs.mkdirSync(home, { recursive: true });
      if (role === 'owner') {
        const installed = path.join(data, 'plugins/hermes'); fs.mkdirSync(installed, { recursive: true });
        // Never recursively copy the fixture's own core/data back into itself.
        for (const file of ['plugin.json', 'package.json', 'index.js', 'lib', 'types']) fs.cpSync(path.join(pluginRoot, file), path.join(installed, file), { recursive: true });
      }
      const port = await availablePort(); let output = '';
      const child = spawn(process.execPath, ['--import', path.join(pluginRoot, 'test/integration/network-guard.mjs'), 'server/main.ts'], {
        cwd: core,
        env: { PATH: process.env.PATH ?? '/usr/bin:/bin', HOME: home, TMPDIR: scratch,
          SWITCHBOARD_DATA_DIR: data, SWITCHBOARD_PORT: String(port), SWITCHBOARD_HOST: '127.0.0.1', SWITCHBOARD_PUBLIC_URL: `http://127.0.0.1:${port}`,
          SWITCHBOARD_WATCH_PLUGINS: 'false', SWITCHBOARD_GITHUB_TOKEN: '', ...extra },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      children.push(child); child.stdout.on('data', b => output += b); child.stderr.on('data', b => output += b);
      await waitFor(() => { if (child.exitCode !== null) throw new Error(`fixture_core_${role}_exited`); return output.includes('Switchboard listening'); }, `${role}_startup`);
      const origin = `http://127.0.0.1:${port}`; let cookie = '';
      const call = async (method, route, body) => {
        const res = await fetch(origin + route, { method, headers: { cookie, 'content-type': 'application/json', 'sec-fetch-site': 'same-origin' }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(10000) });
        const set = res.headers.get('set-cookie'); if (set) cookie = set.split(';')[0];
        const text = await res.text(); return { status: res.status, data: text ? JSON.parse(text) : undefined };
      };
      const invitation = output.match(/\/invite#(\S+)/)?.[1]; assert.ok(invitation, 'isolated core should offer first-admin invitation');
      assert.equal((await call('POST', '/api/auth/invite', { token: invitation, password: 'fixture-only-local-password' })).status, 200);
      return { call, origin, child };
    }
    const central = await launch('central');
    const me = (await central.call('GET', '/api/me')).data;
    const enrollment = await central.call('POST', '/api/admin/peers', { name: 'Hermes fixture owner', ownerUserId: me.id });
    assert.equal(enrollment.status, 201);
    const owner = await launch('owner', { SWITCHBOARD_PEER_URL: central.origin, SWITCHBOARD_PEER_TOKEN: enrollment.data.token });
    const plugin = await owner.call('GET', '/api/admin/plugins/hermes');
    assert.equal(plugin.data.status, 'active', 'actual released core loader must accept setup and validate service');
    assert.ok((await owner.call('GET', '/api/services')).data.some(s => s.id === 'hermes-gateway'));
    milestones.push('actual_plugin_loader_and_service_validation');
    const connected = await owner.call('POST', '/api/connections', { service: 'hermes-gateway', method: 'gateway', name: 'hermes-fixture', config: {
      profiles: JSON.stringify([{ id: 'default', baseUrl: `http://127.0.0.1:${gatewayPort}` }]), tokens: JSON.stringify({ default: secret }),
    } });
    assert.equal(connected.status, 200); assert.equal(connected.data.status, 'connected');
    const connectionId = connected.data.connection.id;
    assert.ok(!JSON.stringify(connected.data.connection.config).includes(secret));
    const localSpec = await owner.call('GET', `/api/connections/${connectionId}/openapi`);
    assert.equal(localSpec.status, 200);
    assert.deepEqual(localSpec.data.operations.map(o => o.id).sort(), operations.map(o => o.id).sort());
    const direct = await owner.call('POST', `/proxy/${connectionId}/operations/hermes.profiles.list`, {});
    assert.equal(direct.status, 200); assert.equal(direct.data.profiles[0].available, true);
    assert.ok(!JSON.stringify(direct.data).includes(secret));
    milestones.push('actual_openapi_discovery_and_authorized_rest_bridge');

    const peerId = await waitFor(async () => (await owner.call('GET', '/api/admin/peers')).data.find(p => p.state === 'online')?.id, 'peer_online');
    assert.equal((await owner.call('PUT', `/api/connections/${connectionId}/shares`, { peerIds: [peerId] })).status, 200);
    const remote = await waitFor(async () => (await central.call('GET', '/api/connections')).data.find(c => c.name === 'hermes-fixture'), 'shared_connection');
    assert.deepEqual(remote.config, {});
    const remoteSpec = await central.call('GET', `/api/connections/${remote.id}/openapi`);
    assert.deepEqual(remoteSpec.data.operations.map(o => o.id).sort(), operations.map(o => o.id).sort());
    const remoteCall = async (operation, args = {}) => {
      const res = await central.call('POST', '/api/call', { connection: remote.id, method: 'POST', url: `/operations/${operation}`, body: args });
      assert.equal(res.status, 200); assert.equal(res.data.status, 200);
      return JSON.parse(res.data.body);
    };
    assert.equal((await remoteCall('hermes.profiles.list')).profiles[0].available, true);
    milestones.push('actual_two_core_peer_openapi_and_json_calls');
    const session = await remoteCall('hermes.session.create', { profile: 'default', operationId: 'peer-session' });
    assert.equal(session.status, 'accepted');
    const input = [{ type: 'input_text', text: 'inert mock only' }, { type: 'input_image', image_url: `data:image/png;base64,${Buffer.alloc(900 * 1024, 7).toString('base64')}` }];
    const runArgs = { profile: 'default', operationId: 'peer-run/key:exact', sessionId: session.sessionId, input };
    const admitted = await remoteCall('hermes.run.create', runArgs); assert.equal(admitted.status, 'accepted');
    const forwardedInput = gateway.writes().find(w => new URL(w.url).pathname === '/v1/runs')?.body.input;
    assert.deepEqual(forwardedInput, [{ role: 'user', content: [{ type: 'text', text: input[0].text }, { type: 'image_url', image_url: { url: input[1].image_url } }] }], 'peer forwards text and the full image in native message parts');
    await waitFor(() => held, 'sse_attached');
    const live = await waitFor(async () => {
      const p = await remoteCall('hermes.run.events', { profile: 'default', runId: admitted.result.run_id }); return p.events.length ? p : undefined;
    }, 'peer_live_batch_without_eof');
    assert.equal(live.events[0].delta, 'peer-live-before-eof'); assert.equal(live.observing, true);
    milestones.push('peer_event_batch_delivered_while_native_sse_remains_open');
    gateway.runs.get(`default:${held.id}`).status = 'completed';
    held.controller.enqueue(Buffer.from(frames([event(held.id, 1, 'run.completed')]))); held.controller.close();
    await waitFor(async () => (await remoteCall('hermes.run.events', { profile: 'default', runId: admitted.result.run_id })).state === 'settled', 'terminal_commit');
    const writesBeforeReload = gateway.writes().length;
    assert.equal((await owner.call('POST', '/api/admin/plugins/hermes/reload', {})).data.status, 'active');
    assert.deepEqual(await remoteCall('hermes.run.create', runArgs), admitted);
    assert.equal(gateway.writes().length, writesBeforeReload);
    const retired = await remoteCall('hermes.run.retire', { profile: 'default', runId: admitted.result.run_id }); assert.equal(retired.retired, true);
    const retiredPage = await remoteCall('hermes.run.events', { profile: 'default', runId: admitted.result.run_id }); assert.equal(retiredPage.coverage.reason, 'terminal_snapshot_retired');
    assert.deepEqual(await remoteCall('hermes.run.create', runArgs), admitted); assert.equal(gateway.writes().length, writesBeforeReload);
    milestones.push('loader_reload_durable_replay_and_scoped_retirement_over_peer');

    const apiToken = (await central.call('POST', '/api/tokens', { name: 'Hermes fixture operation discovery' })).data.secret;
    const tool = async (name, args) => {
      const r = await fetch(central.origin + '/mcp', { method: 'POST', headers: { authorization: `Bearer ${apiToken}`, 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) });
      const v = await r.json(); assert.equal(v.error, undefined); assert.notEqual(v.result.isError, true); return v.result;
    };
    const described = await tool('get_operation', { connection: remote.id, operationId: 'hermes.retention.status' });
    assert.equal(described.structuredContent.id, 'hermes.retention.status');
    const invoked = await tool('call_operation', { connection: remote.id, operationId: 'hermes.retention.status', body: { profile: 'default' } });
    assert.match(invoked.content[0].text, /HTTP 200/); assert.match(invoked.content[0].text, /immutableReceiptsPreserved/);
    milestones.push('actual_mcp_get_operation_and_call_operation_via_peer');
    result = { status: 'passed', core: release, milestones, providerWrites: 'mock_gateway_only', realModelRuns: 0 };
  } catch (error) {
    result = { status: ['EPERM', 'EACCES'].includes(error.code) ? 'blocked_socket_permission' : 'failed', core: release, milestones, errorCode: error.code ?? 'assertion_or_fixture_failure' };
    if (result.status === 'blocked_socket_permission') throw new Error('Integration fixture requires loopback sockets: sandbox denied them. Root can run npm run test:integration on the same frozen source; no escalation attempted.');
    throw error;
  }
});
