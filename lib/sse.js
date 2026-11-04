import { Fault } from './operations.js';

// WHATWG SSE framing: UTF-8 chunk boundaries, CR/LF/CRLF, comments, multiline data.
export async function* sse(body, signal) {
  if (!body) throw new Fault('missing_event_body', 502);
  const reader = body.getReader(); const decoder = new TextDecoder('utf-8', { fatal: true });
  let buffer = '', data = [], eventId, frameBytes = 0;
  const abort = () => { void reader.cancel().catch(() => {}); };
  signal?.addEventListener('abort', abort, { once: true });
  try {
    for (;;) {
      signal?.throwIfAborted();
      const chunk = await reader.read();
      buffer += decoder.decode(chunk.value, { stream: !chunk.done });
      if (Buffer.byteLength(buffer) > 262144) throw new Fault('event_frame_too_large', 502);
      let match;
      while ((match = /\r\n|\r|\n/.exec(buffer))) {
        // A CR at the boundary may be the first half of CRLF.
        if (!chunk.done && match[0] === '\r' && match.index === buffer.length - 1) break;
        const line = buffer.slice(0, match.index); buffer = buffer.slice(match.index + match[0].length);
        frameBytes += Buffer.byteLength(line) + 1;
        if (frameBytes > 262144) throw new Fault('event_frame_too_large', 502);
        if (!line) {
          if (data.length) yield { id: eventId, data: JSON.parse(data.join('\n')) };
          data = []; eventId = undefined; frameBytes = 0;
        } else if (!line.startsWith(':')) {
          const i = line.indexOf(':'); const field = i < 0 ? line : line.slice(0, i);
          let v = i < 0 ? '' : line.slice(i + 1); if (v.startsWith(' ')) v = v.slice(1);
          if (field === 'data') data.push(v);
          if (field === 'id') eventId = v;
        }
      }
      if (chunk.done) break; // An unframed tail is not a committed SSE event.
    }
  } finally { signal?.removeEventListener('abort', abort); await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
