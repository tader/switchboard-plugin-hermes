const string = { type: 'string', minLength: 1, maxLength: 256 };
const id = { ...string, maxLength: 255, pattern: '^(?!\\.{1,2}$)[^/\\\\]+$' };
const operationKey = { ...string, maxLength: 255 }; // Native Idempotency-Key may contain '/' or ':'.
const text = { type: 'string', minLength: 1, maxLength: 65536 };
const integer = { type: 'integer', minimum: 0, maximum: 1000000 };
const profile = { ...string, pattern: '^[A-Za-z0-9_-]+$' };
const object = (properties, required = []) => ({ type: 'object', properties, required, additionalProperties: false });
const spec = (name, description, properties, required, readOnly = true) => ({
  id: `hermes.${name}`, description, input: object(properties, required), readOnly,
});
export const operations = [
  spec('profiles.list', 'Probe only configured authorized native gateway profiles; returns verified capabilities or safe unavailable states.', {}, []),
  spec('capabilities.get', 'Read actual native capability response for an authorized profile.', { profile }, ['profile']),
  spec('sessions.list', 'List native sessions without title lookup or resurrection.', { profile, limit: { ...integer, maximum: 200 }, offset: integer }, ['profile']),
  spec('session.get', 'Read a native session.', { profile, sessionId: id }, ['profile', 'sessionId']),
  spec('session.history', 'Read paginated native history, preserving native message identities and compression tip.', { profile, sessionId: id, limit: { ...integer, maximum: 500 }, offset: integer, order: { enum: ['oldest', 'latest'] } }, ['profile', 'sessionId']),
  spec('session.create', 'Create one operation-derived remote session. Replays return the durable receipt; uncertain actions never resubmit.', { profile, operationId: operationKey, title: { ...text, maxLength: 256 } }, ['profile', 'operationId'], false),
  spec('run.create', 'Admit one native run with the exact caller Idempotency-Key. Replays never resubmit uncertain actions.', { profile, operationId: operationKey, sessionId: id, input: text }, ['profile', 'operationId', 'sessionId', 'input'], false),
  spec('run.status', 'Read native run status; resume a known nonterminal event collector.', { profile, runId: id }, ['profile', 'runId']),
  spec('run.stop', 'Request native stop once; status must confirm settlement.', { profile, operationId: operationKey, runId: id }, ['profile', 'operationId', 'runId'], false),
  spec('run.events', 'Read durable ordered JSON event pages; explicitly reports missing and evicted sequence ranges. May resume SSE observation only.', { profile, runId: id, afterSeq: { type: 'integer', minimum: -1, maximum: Number.MAX_SAFE_INTEGER }, snapshotRevision: { type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER }, limit: { ...integer, minimum: 1, maximum: 100 } }, ['profile', 'runId']),
  spec('run.approval', 'Answer one exact observed pending approval, gated by native capabilities and observed generation identity.', { profile, operationId: operationKey, runId: id, requestId: id, executionGeneration: integer, choice: { enum: ['once', 'deny'] } }, ['profile', 'operationId', 'runId', 'requestId', 'choice'], false),
  spec('run.clarify', 'Answer one exact observed generation-fenced clarify prompt; requires native authority evidence.', { profile, operationId: operationKey, runId: id, requestId: id, executionGeneration: integer, answer: text }, ['profile', 'operationId', 'runId', 'requestId', 'executionGeneration', 'answer'], false),
  spec('action.get', 'Read durable action receipt without repeating any upstream write.', { profile, operationId: operationKey }, ['profile', 'operationId']),
  spec('retention.status', 'Read scoped retention/receipt counters and hot snapshot capacity; never calls the gateway.', { profile }, ['profile']),
  spec('run.retire', 'Idempotently retire only this scope\'s settled terminal snapshot; preserve every receipt/tombstone and all uncertain/active jobs. No gateway call.', { profile, runId: id }, ['profile', 'runId'], false),
];

export class Fault extends Error {
  constructor(code, status = 400) { super(code); this.code = code; this.status = status; }
}
export function validate(schema, value) {
  if (schema.enum && !schema.enum.includes(value)) throw new Fault('invalid_arguments');
  if (schema.type === 'object') {
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Fault('invalid_arguments');
    if (Object.keys(value).some(k => !Object.hasOwn(schema.properties, k))) throw new Fault('unknown_argument');
    for (const k of schema.required) if (!Object.hasOwn(value, k)) throw new Fault('missing_argument');
    for (const [k, v] of Object.entries(value)) validate(schema.properties[k], v);
  }
  if (schema.type === 'string') {
    if (typeof value !== 'string' || value.length < (schema.minLength ?? 0) || value.length > schema.maxLength) throw new Fault('invalid_arguments');
    if (schema.maxLength <= 256 && /[\x00-\x1f\x7f]/.test(value) || schema.pattern && !new RegExp(schema.pattern).test(value)) throw new Fault('invalid_arguments');
  }
  if (schema.type === 'integer' && (!Number.isSafeInteger(value) || value < schema.minimum || value > schema.maximum)) throw new Fault('invalid_arguments');
}
export function openapi(baseUrl) {
  return { openapi: '3.0.3', info: { title: 'Hermes gateway adapter', version: '0.1.0' }, servers: [{ url: baseUrl }],
    paths: Object.fromEntries(operations.map(op => [`/operations/${op.id}`, { post: {
      operationId: op.id, summary: op.description, 'x-readOnlyHint': op.readOnly,
      requestBody: { required: true, content: { 'application/json': { schema: op.input } } },
      responses: { 200: { description: 'Result or durable action receipt', content: { 'application/json': { schema: { type: 'object' } } } }, 400: { description: 'Invalid input' }, 401: { description: 'Authentication required' }, 409: { description: 'Identity conflict or unsupported/stale control' }, 502: { description: 'Upstream failure' } },
    } }])) };
}
