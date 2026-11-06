# Hermes gateway Switchboard plugin

No model/provider code lives here. Requires Node 24+ with `node:sqlite`. There are no third-party runtime dependencies.

The plugin consumes **Hermes Agent API-server REST/SSE**, not the separate `nesquena/hermes-webui` API. Gateway credentials and all model execution remain on the owning host. Clients access the gateway through Switchboard.

## Transport and SDK

The actual Switchboard SDK provides `ServiceDefinition`, optional `openapi`, auth hooks, `PluginContext.dataDir` and `dispose`. It does not provide an arbitrary HTTP-handler registry or a native SSE source adapter. This plugin uses the same **private loopback HTTP bridge** pattern as Switchboard's existing shell-command plugin. On installation only, `setup()` binds a random `127.0.0.1` port; gateway work is not started by setup. Each Switchboard-authorized request receives a one-use, route-bound, ten-second bridge nonce. Raw gateway URLs/credentials are never arguments to operations. The bridge runs inside the plugin host; it is not a separately deployed service.

OpenAPI declares actual custom REST operations at `/operations/<operationId>`. Call them through Switchboard's authenticated `/proxy/<connection>/operations/<operationId>` using POST JSON, or its normal discovered OpenAPI call tooling. These are **not** `websocket/call` operations. No synthetic WebSocket or gateway ticket is used. Peer HTTP transports only finite JSON results; upstream SSE stays at the gateway-owning plugin process.

SDK compatibility is documented against immutable [Switchboard 0.9.0](https://github.com/tader/switchboard/blob/af10250cab41991ae5f5214de1222bab3fd7de71/server/plugins/api.ts) (primary deployed target) and [0.8.3](https://github.com/tader/switchboard/blob/ec6fbc36abc25e087f44191d90c4572790acf152/server/plugins/api.ts) (minimum version). `types/api.d.ts` is an independently authored, MIT-licensed minimal structural shim for the one context field used, **not a vendored SDK**. See [types/README.md](types/README.md) for field/provenance details. The bridge follows the established shell-command plugin extension pattern; no core change is required.

## Connection configuration

Service ID `hermes-gateway`, auth method `gateway`, plugin ID `hermes`.

`profiles` is a nonsecret JSON list of authorized native endpoints:

```json
[{"id":"default","baseUrl":"http://127.0.0.1:8642"}]
```

The default example is not an assertion about profiles on an installation. Named profiles are supported when actually authorized/served: their configured native base URL must include `/p/<id>`. Discovery probes only configured endpoints, verifies the native capability object and returns availability. It neither guesses names nor advertises configured-but-unreachable profiles as available. It explicitly reports `exhaustive:false`: native Hermes has no verified public exhaustive profile-list API. Administrators must supply the real authorized candidate inventory. An unprefixed endpoint may represent default only. Non-loopback URLs require HTTPS, and URLs cannot contain credentials, query strings or arbitrary paths.

`tokens` is a **secret** JSON object mapping each configured profile ID to that profile's gateway-only `API_SERVER_KEY`. Values are entered through Switchboard's secret field; never place keys in source, examples, operation arguments, command lines or model-provider credentials. The plugin returns those values as `Connected.credentials`, not `Connected.config`; normal Switchboard credential storage applies. No raw token-export hook is provided.

Receipts/events are scoped by connection ID, profile, canonical configured endpoint and a hash of the gateway principal credential. Changing endpoint/key starts a new isolated namespace: reconcile outstanding actions under the old identity before changing credentials. Credentials are not persisted in the plugin database. Key rotation is not assumed to prove continuity of account identity.

## Actual custom operations

Every operation except `hermes.profiles.list` takes `profile`. Unknown arguments are rejected. Native identifiers are retained; path identifiers reject path traversal/separators. Operation keys remain exact visible ASCII, 1–255 characters, including `/` and `:`.

| Operation ID | Additional input / behavior |
| --- | --- |
| `hermes.profiles.list` | No arguments. Live native capability verification of configured authorized inventory. |
| `hermes.capabilities.get` | Native `/v1/capabilities` response. |
| `hermes.sessions.list` | `limit?` (max 200), `offset?`; no title lookup/resurrection. |
| `hermes.session.get` | `sessionId`. Native session response. |
| `hermes.session.history` | `sessionId`, `limit?` (max 500), `offset?`, `order?` (`latest`/`oldest`). Native message/tool IDs and compression-tip session ID preserved. |
| `hermes.session.create` | `operationId`, `title?`. Derives one stable native session ID from namespace/action, sends `POST /api/sessions` once. Returns its remote session ID and durable receipt. |
| `hermes.run.create` | `operationId`, `sessionId`, `input`. Input accepts text or bounded text/image content blocks with inline PNG, JPEG or WebP images. Uses **exact `operationId` as `Idempotency-Key`**, requires live durable native idempotency, returns a native run ID and starts event observation. No model/provider override. |
| `hermes.run.status` | `runId`. Fresh native status; eligible event observation may resume. |
| `hermes.run.stop` | `operationId`, `runId`. One stop request; acceptance does not imply settlement. |
| `hermes.run.events` | `runId`, `afterSeq?` (default -1), `limit?` (1–100), `snapshotRevision?`. Durable finite JSON event page; can resume read-only observation. |
| `hermes.run.approval` | `operationId`, `runId`, `requestId`, `choice` (`once`/`deny`), `executionGeneration` for authority mode. No implicit session-wide/always approval. |
| `hermes.run.clarify` | `operationId`, `runId`, `requestId`, `executionGeneration`, `answer`. Requires actual authority capability evidence and an exact current pending clarify prompt. |
| `hermes.action.get` | `operationId`. Stored receipt only, no upstream action. |
| `hermes.retention.status` | Local scoped snapshot/receipt/uncertainty counters and disk usage; no gateway request. |
| `hermes.run.retire` | `runId`. Idempotently retire only a settled terminal snapshot in this scope; preserve action receipts and uncertain/pending work. |

Image blocks are limited to 4 MiB decoded in total. Peer connections have a smaller 2 MiB message limit, including base64 and the request envelope; callers must size images accordingly.

Authority support is established from native `run_unknown_resolution:true` plus fresh run status containing `pending_controls` and integer `execution_generation`. This is necessary because the inspected native capability table does not advertise a dedicated clarify flag/endpoint. Native clarification still uses the real `/v1/runs/{id}/clarify` route and strict `{request_id,execution_generation,answer}` body. Without that evidence clarification is rejected; no fallback through chat send. Authority approvals use the strict generation body; legacy approvals use exact `{request_id,choice}` after checking fresh pending status.

## Durable writes and uncertain outcomes

SQLite reserves the action identity and immutable request fingerprint **before** an upstream POST. Reusing an identity for different input fails. Accepted/rejected/uncertain receipts replay locally, including after restart. A reservation left by a crash is treated as uncertain. Session IDs are derived before sending; run keys are forwarded unchanged. Creates/answers are **never automatically resubmitted**, even if upstream idempotency exists. A different operation ID also cannot repeat an accepted/uncertain answer for the same native prompt/generation; the prompt claim is atomic. Definitively rejected actions require an explicit new operation ID if a later deliberate retry is appropriate.

For an ambiguous session create, `action.get` returns the reserved session ID; `session.get` can inspect existence. The plugin does not treat mere existence as proof of ownership/acceptance and does not POST again. For a lost run-create reply with no run ID, native API has no inspected GET-by-idempotency-key recovery route: the uncertain receipt stays unresolved for operator reconciliation. No hidden retry operation is supplied. Lost answers similarly remain uncertain and may be reconciled with fresh native run/prompt state without resending.

## Event page contract

The gateway stream's numeric SSE `id` must match payload `seq` and its `run_id` must match the scoped run. The plugin preserves native event names and bodies (with known credentials/credential fields redacted), sorts by sequence, deduplicates exact repeats and pauses on conflicting sequence reuse. Text/tool/question events are not flattened or renamed. A tool ID may repeat across occurrences; use `(runId,seq)` as occurrence identity.

Pages contain `events`, `nextSeq`, `hasMore`, `snapshotRevision`, `highSeq`, `droppedThrough`, `gaps`, `coverage`, `state`, `error`, `observing`, `upstreamStatus` and terminal-observation metadata. `coverage.scope` is `retained_observed_sequences`: `complete` means no detected hole in that requested retained range, **not** a guarantee that all historical upstream activity is available. `terminalEventObserved:false` means terminal status was recovered without its terminal stream event.

Persist incoming events and page cursor atomically. Keep draining while `hasMore`. If a late out-of-order event repairs an already-passed sequence, the page sets `resyncRequired:true` and `rewindAfterSeq`; refetch from that cursor, deduplicate native sequences and acknowledge the new `snapshotRevision`. Missing/evicted ranges require native status/history reconciliation; a page cursor must not imply a hole was recovered. An ahead-of-snapshot cursor/revision fails explicitly.

The collector reconnects with native `Last-Event-ID` using the last committed high sequence. It has a 90-second stream window, read-only status checks and bounded exponential backoff; five consecutive failures pause it. Auth errors, missing replay, malformed/conflicting events and oversized events pause immediately where classified. Explicit later status/event reads can restart eligible observation; no collector retry starts a run or answers a prompt. Restart never starts a background gateway call using persisted credentials: the next authorized read rehydrates a connection and resumes its known run. Therefore host downtime can exceed native upstream replay retention; gaps cannot be reconstructed by this plugin.

Cached events remain readable during upstream outage with explicit error/status metadata. After native terminal status, retained SSE is drained when necessary rather than skipping terminal-run backfill. Connection deletion revokes pending nonces and collectors; changed connection credentials stop old collectors when observed on the next authorized request. The SDK provides no general credential-change callback to this plugin: Switchboard access still gates delivery, but immediate collector cancellation on every configuration change is a host lifecycle integration gap.

## Bounds and lifecycle

The hot snapshot capacity is **64 run slots** including ambiguous admissions, **not 64 lifetime turns**. On capacity pressure, the admission transaction retires its scope's oldest eligible settled terminal snapshot. Active collectors, nonterminal/pending/unknown work and uncertain controls/admissions are protected. If all slots are protected or owned by another scope, fresh admission fails before native POST. `hermes.run.retire` provides explicit scoped retirement; `hermes.retention.status` reports capacity and ledger usage. No operation deletes remote sessions or transcripts.

A retired snapshot becomes a permanent tombstone and releases its hot slot/event rows. Old action replay still returns the exact stored receipt without reallocation, gateway POST or collector restart. Event reads return an explicit retired/loss marker, never a silently empty complete transcript. See [RETENTION-SPEC.md](RETENTION-SPEC.md) for the additive retention contract.

Snapshot content remains bounded: **1,000 events / 512 KiB per hot run**, **100 events / 512 KiB per page**, 64 KiB per event, 128 KiB per receipt, 512 KiB per native JSON response and 256 KiB per SSE frame. FULL SQLite synchronization/transactions and private permissions remain. Event eviction and upstream truncation are explicit loss boundaries.

The safety ledger retains **all** immutable action receipts, prompt claims and compact retired tombstones. There is no action-count lifetime quota and the old 128 MiB total-file limit is removed on upgrade. This metadata necessarily grows with lifetime use; monitor `ledgerBytes`, back up the complete database and provision disk space. Filesystem/SQLite exhaustion fails closed; never delete the database or expire old action protection to recover capacity. Sustained normal chat reuses hot slots while preserving the ledger. More than 64 genuinely active/uncertain reservations still requires resolving that work or deliberately increasing provisioned capacity. A failure while recording a post-response receipt leaves the precommitted action uncertain. Event/receipt data is private plaintext local application data and should follow host storage/backup protections.

`dispose()` aborts readers and closes the bridge/database. Starting the plugin at installation opens only its loopback bridge; it does not change Hermes configuration, start Hermes, create sessions or invoke runs.

## Validation and remaining integration

```sh
npm test
npm run check
# Separate real released-core socket fixture, with local dependencies already installed:
SWITCHBOARD_CORE_REPO=/path/to/switchboard npm run test:integration
```

Unit tests use an in-memory mock gateway and temporary SQLite fixtures under `test/`, removed afterward. They do not bind ports or invoke setup. The separate integration fixture runs **two immutable release targets**: deployed **0.9.0** (`af10250cab41991ae5f5214de1222bab3fd7de71`) first, then minimum **0.8.3** (`ec6fbc36abc25e087f44191d90c4572790acf152`). For each, it exports the unmodified commit into temporary plugin-owned storage, verifies installed direct dependency versions against its lock, and starts an isolated mock HTTP/SSE gateway plus two real core processes. It verifies loader/service validation, OpenAPI, REST, peer JSON while SSE remains open, reload/replay/retirement and actual MCP get/call-operation. Children have isolated data/HOME/env and reject non-loopback fetch destinations. Only mock sessions/runs are used; no production configuration or model provider is contacted.

The integration command fails explicitly on denied sockets. Set `SWITCHBOARD_CORE_REPO` to a repository containing both pinned release commits. It leaves ignored per-release `test/.integration-v*-result.json` evidence and removes temporary fixture state. Optional `SWITCHBOARD_CORE_NODE_MODULES` selects existing read-only dependencies. The fixture performs no automatic dependency install or fetch. Both release targets pass locally; CI repeats the fixture on Node 24.
