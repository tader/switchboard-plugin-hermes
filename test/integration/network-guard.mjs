// Loaded only in isolated integration-fixture core children, never in installed plugin code.
// No inherited production credentials or outbound HTTP destinations are permitted.
const original = globalThis.fetch;
globalThis.fetch = (input, init) => {
  const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url);
  if (!['127.0.0.1', '[::1]', 'localhost'].includes(url.hostname)) throw new Error('fixture_non_loopback_network_blocked');
  return original(input, init);
};
