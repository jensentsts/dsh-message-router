# Message Routing 0.0.2 / 消息路由

## 从 GitHub 直接安装

在 DSH 的「插件 → 安装第三方插件」粘贴以下地址并安装：

```text
https://github.com/jensentsts/dsh-message-router/archive/refs/heads/main.tar.gz
```


> 插件集导航与市场索引：https://github.com/jensentsts/dsh-independent-plugins


An independent Host plugin for DSH **0.2.2-rc.1** that connects native sessions and registered channels. Existing adapters use the `ctx.superAgentIo` service. QQ and remote adapters are optional consumers. The SVG icon is original artwork, licensed with this package.

## Safety and status

Both `enabled` and `sendEnabled` default to **false**. An outbound adapter binding separately requires `enabled` and `sendEnabled`. Receive requires enabled IO and an enabled binding. Preview is read-only. Host plugins authenticate and authorize their own external requests. The model can explicitly send during an owner-initiated DSH turn, subject to the gates below.

For a binding that only accepts input, set `receiveOnly: true`. Its committed input and local model replies stay in the canonical Session. IO holds no outbound delivery for that binding and forces its send switch off. The ordinary send switch still pauses queued deliveries on two-way bindings.

A trusted adapter can send a committed external reply with a bounded `replySource`: actor, provider, model, origin session/message IDs and sequence/turn/step. IO records those facts beside the source, leaving the message text intact. Presentation consumers can render the producer's identity and reply tag from that record.

Verification includes dependency-free regressions, exact-release Cordis/Session/storageDomain tests, official Loader+Include composition, real AgentLoop requests against a local scripted LLM adapter, strict Gateway authority tests and server-rendered browser-artifact tests. Storage media and external transports are mocked; no real model/network is called. App installation, visually rendered Settings in the existing GUI, QQ/peer delivery and restart/fork against a real profile require separate verification. Do not infer those from offline composition or server-rendered markup.

## Adapter service

The adapter service key is `superAgentIo`, version `1`; the optional owner-only read service is `superAgentIoMonitor`. package identity is `dsh-super-agent-io`, Loader entry `independent-super-agent-io`, domain `super_agent_io` version `1`. Exact signatures are in `index.d.ts`; dependency-free values and validators are in `contract.mjs`.

```js
const io = ctx.get('superAgentIo')
if (!io || io.contractVersion !== 1) throw new Error('Compatible IO service unavailable')
const remove = io.registerTransport({
  id: 'my-transport',
  async deliver(delivery, { signal, idempotencyKey }) {
    // Authenticate sender/target and preserve signal/idempotencyKey in your adapter.
    // One delivery is one real committed assistant message, NOT a sentence fragment.
    return { state: 'delivered', externalMessageId: 'confirmed-platform-message-id' }
  },
})
ctx.effect(() => remove)
await io.putBinding({
  id: 'my-binding', transport: 'my-transport', endpoint: 'private:123',
  sessionId: 'existing-session-full-id', enabled: true, sendEnabled: false,
  mode: 'auto', preload: true,
})
await io.preload('my-binding') // official resolveAgent, never create-on-error
const receipt = await io.receive({
  bindingId: 'my-binding', externalMessageId: 'platform-input-id', senderId: '123',
  content: [{ type: 'text', text: 'Hello' }], replyTo: 'quoted-platform-input-id',
})
```

`registerTransport` accepts arbitrary nonempty IDs up to 128 characters, rejects duplicates, and returns an idempotent disposer. Call it from the adapter's own effect. Disposing does not dispose borrowed Agents; in-flight transport results become unknown when interrupted. `putBinding` persists sidecar state, not profile configuration. Reusing an already-used binding ID for another endpoint/session is refused: use a new ID. Store secrets in the adapter's credentials service, never in endpoint IDs or content.

`receive` acknowledges bounded native inbox admission, not model completion. Its `state:'accepted'` means the native method returned; `messageCommitted` is initially false, and `acceptedSeq` is absent. Only the **exact committed `user/message` with matching Session, message ID, trace ID and binding** sets `messageCommitted:true` and `acceptedSeq`. `durable:true` is possible only after a persistence barrier started after that matching event. Re-query `receipt(traceId)` for updated state. A timeout, unload or crash around a non-retryable native send produces `unknown`, not false success or automatic resend. The immutable external input identity is binding + transport + endpoint + externalMessageId; concurrent and replayed duplicates return the same trace/message IDs. Capacity fails closed rather than deleting dedupe records.

Inbound content is public `text`, or `image`/`file` with already-admitted DSH durable references; never URLs, raw paths, arbitrary bytes, reasoning, tools or externally selected roles. Adapters own file size/MIME/path policy and reference admission. The Host verifies stored images and checks selected-model image modality; durable files must be produced by the trusted adapter through official attachments. Incoming role is `user` but source is **`kind:'super-agent-io'`**, with producer IDs. It is not a fake local user or `rpcId`. Quote text supplied by an adapter is external, untrusted input.

### Session actions and scheduling

- `preload(bindingId)` resumes a bound ordinary Session using official `sessionController.resolveAgent` and singleflights concurrent calls; disabled input/binding refuses preload. Sending can remain off. It never silently creates on not-found, corrupt, writer-held, archived or subagent errors.
- `createSession({cwd?,agentPreset?})` is a separate explicit trusted-Host action. Reference, preview and admission never call it as fallback.
- `preview(bindingId,{limit?,beforeSeq?})` uses cold-safe Session observations and excludes hidden context, reasoning and tools. It does not resume an Agent or make a model request.
- queue uses `followup`, steering uses `steer` at the next admitted step; steering does **not** cancel an already prepared request. Idle steering is delivered as queue. Auto uses steering while running, subject to a consecutive-steer budget; existing queued turns force new input to queue. The receipt reports actual mode.
- Session admission uses bounded concurrent workers and round-robin binding buckets. It never awaits `whenIdle`, a model turn, or approval. Slow cold resolution/attachment validation times out and releases the worker; late resolution cannot send. Native inbox ownership stays with DSH.
- `cancel(bindingId,{keepInbox?})` is trusted-Host only, bypasses admission, stops a live Agent, fences pending admission and cancels pending deliveries for that Session. A cold Session is not activated just to cancel it. Different bindings of one Session share its cancellation scope.

## Committed output and traceability

The Host consumes committed Session events, never transient streams or assistant attempts. Every public committed `assistant/message` is a separate ordered reply; public text, image and file-only messages work. Reasoning and tool blocks are omitted; canceled partial messages and replacement copies are not exported. Source preserves real model metadata except private adapter `replayState`; missing effort is not invented. No assistant is manually appended and no tool result is relabeled as a reply. This plugin does not split sentences or parse `[QQ]` tags.

Routing reconstructs inbox claims and turn ownership from canonical events. Only a turn attributable exclusively to one known binding may leave that binding. Mixed bindings, local input, unknown external producers or missing sidecar traces suppress external delivery with an audited reason. Ordinary runtime-context is not treated as a second human sender and is never exposed in history. A shared Session is not a promise of separate privacy contexts; use separate Sessions for distinct conversations. There is no last-sender/ten-minute heuristic.

The sidecar stores `bindings`, `traces`, `deliveries`, `cursors` and `audit`. Session logs remain the only model-history truth. Input identity, external replyTo, endpoint and delivery receipts do not use adapter replayState. Delivery is persisted before the cursor advances, keyed by Session/seq/message/binding. Replayed events cannot resend an acknowledged delivery. Cursor gaps require canonical observation replay before any output routing.

Delivery values contain `deliveryId`, `bindingId`, `endpoint`, `reply`, `traceIds` and optional `replyTo`. The reply contains canonical Session/seq/message IDs, turn/step, actual model source, timestamp, public content and available usage. A single triggering input supplies the outbound native quote target; with multiple input traces there is no guessed replyTo.

Adapters return one of:

- `{state:'delivered',externalMessageId}`: confirmed external acceptance.
- `{state:'failed',reason}`: definite no-send; retained for explicit operator handling, not automatically retried.
- `{state:'unknown',reason}`: external outcome cannot be established.

Throw, invalid result, timeout, unload, or restart with a `sending` record is **unknown**, never automatic retry. Per-binding delivery chains preserve order. Registration and `drain()` resume only untouched pending work. Unknown/failed work stays visible for reconciliation; no unsafe retry UI is supplied.

## Explicit destinations

A turn initiated from the owner composer or an owner-confirmed Workspace Reference route can use `io_destinations({query?,channel?})`, `io_probe({destination})` and `io_send({destination,text})`. Discovery returns exact binding IDs for opted-in adapters and `dsh:<sessionId>` for existing ordinary Sessions. It uses the native Session catalog and cached titles. A DSH probe checks catalog eligibility and an adapter probe checks transport availability. Both are read-only. Each explicit send stores one receipt and passes through the same per-binding delivery queue. The adapter sees `intent:'explicit'`, the originating assistant's metadata and the authored message. Native DSH delivery acknowledges target inbox acceptance and records the same source metadata on the admitted message. The reserved `dsh` channel is managed by Message Routing.

Adapters opt in with `explicitSend: true` on `registerTransport`. They can provide a `channel` name and `probe(binding,{signal})` returning `{available:boolean}`. Existing adapters may keep their current registration. External input and mixed turns keep their automatic binding routing and lack explicit-send authority.

## Opt-in direct-reply continuation

The global `continuationEnabled` switch defaults to false. A binding must additionally set an explicit policy:

```js
continuation: { enabled: true, maxAdditionalReplies: 2, maxOutputTokens: 2048, maxDurationMs: 10000 }
```

`maxAdditionalReplies` (1..8) is a hard ceiling on additional model requests initiated by this plugin. `maxOutputTokens` (1..32768, includes the initial reply) and `maxDurationMs` (100..120000, since turn start) are **admission thresholds for the next continuation**, not hard caps on an already-prepared/in-flight request. A request may exceed the remaining token/time threshold; it is not forcibly canceled, replayed or reported as free. Missing actual usage disables further continuation. All requests, including an exact stop response, retain native usage and cost records.

At the official serial `agent/turn-stopping` boundary, an enabled exclusive IO turn may receive one logged `super-agent-io-control` producer message requesting an optional useful plain-text follow-up. The message contains a fresh random nonce stop token. The model can immediately return that exact token if nothing more is needed; there is no required second public reply. Every useful follow-up is a new real model request and committed assistant message, never sentence splitting.

Only a complete, single text block **exactly** equal to the nonce token from the exact sidecar-prepared control message is control-only: it stays in native Session history and is recorded as a stopped continuation audit, but is not externally sent. Substrings, quotations, multiple blocks, whitespace/newline variants or forged control identities are not silently swallowed. This is an explicit control protocol, not a fictional final/channel field. The `io_history` audit view exposes prepared/requested/stopped diagnostic facts without hiding records from official native history.

Any observed tool/attempt/media, manual or other producer input, mixed binding, queued/next-step work, canceled signal, token/time limit, disabled binding or plugin disposal prevents another continuation request. Tools are not globally restricted: a model may ignore the plain-text instruction and use a tool; detection stops this plugin from adding more requests, while native tool execution and permissions remain unchanged. Existing and newly created Agents own scoped listeners; both Agent disposal and plugin unload remove them. No change to Session format or assistant publication is required.

## History and settings

The independent **IO monitor** Settings section (order 53) is a read-only non-Chat view. It lists current gates, paged bindings with transport endpoint ↔ native Session direction, input/output state counts and paged public input/delivery/audit receipts. Canonical message seq, external IDs, reply target, actual model, excerpt/media names, message commit and durability are separate facts. No button admits input, starts a model, cancels, retries, drains pending deliveries or changes configuration.

Its optional Host RPC exposes **only** `superAgentIoMonitor/query`, with strict `bindings`, `history` and `receipt` variants. An invocation must carry an owner principal; missing principal and non-owner peers are denied even if another policy allows the global endpoint. Trusted in-process queries have no invocation. Complete results, including all labels and wrappers, are limited to 64 KiB with a typed oversize error, not silently clipped JSON. No raw hidden context, tool output, credentials or adapter replayState is returned. Endpoint IDs remain owner-visible transport metadata.

The Client mounts exact strict Typert descriptors, registers an owned resource protocol and renders through the standard `useResource` seat in `settings.section`; manual refresh changes its resource key. It does not fetch in effects, create a private subscription, inspect DOM, or append assistant/chat messages. Locale follows global English/Chinese. Client Slot metadata was supplied by lead's live Inspect; resource/RPC contracts were read from exact source because this agent's Client Inspect timed out.

`io_history` returns receipt pages with IDs, text excerpts, media handles, omission flags and a query-bound continuation cursor. `io_history_read` retrieves public neighboring user/assistant messages by canonical seq. Both require the calling Agent's Session to own the binding; no cross-Session ID guessing, receive/send or approval action is exposed. Full emitted wrappers are byte-bounded, not only the text. The model can discover these two tools only when enabled bindings belong to its Session; no long per-turn prompt is injected.

The independent automatic Settings form uses a root volatile Config: input gate, external-delivery gate, queue/concurrency/steering bounds, text/block bounds, retained-record cap, history item/byte limits and admission/delivery timeouts. English and Chinese locale resources follow global language; there is no independent language switch. It has no custom DOM, chat renderer replacement or hidden settings file.

## Offline verification

Run from this package using the supported task-local Node executable:

```powershell
& 'node' --test tests/*.test.mjs
```

Pure tests inject a fake Host and `MemoryStore`; production never falls back to memory when storageDomain fails. Tests cover source identity, concurrent/restart dedupe, delayed native commit vs admission/durability, queue bounds, cold errors, ordered multiple replies and media-only replies, interruption/private-data filtering, mixed-turn suppression, timeout/unknown receipts, cancellation, pagination/byte limits and teardown.

Exact-release source integration is a separate command; its test-only loader uses the exact checkout's TypeScript paths and already-present packages without generating artifacts or installing anything:

```powershell
$env:DSH_SOURCE_ROOT='<DSH_SOURCE_ROOT>'
& 'node' --import ./tests/runtime-loader.mjs --test tests/runtime.integration.mjs
```

This verifies real Cordis registration/disposal, Session v4 message derivation, storageDomain generation/schema validation and the exact tool-schema compiler. Additional official Loader+Include and real AgentLoop+mock LLM tests run together through the repository-owned exact-source loader:

```powershell
& 'node' --expose-internals --import ../../tools/exact-source-loader.mjs --test tests/runtime.integration.mjs tests/loader.integration.mjs tests/loop.integration.mjs tests/monitor.integration.mjs tests/client.integration.mjs
```

The Loader fixture reads its test composition, persists only to its own in-memory storage backend, removes its entry through the official group API and checks effects after teardown. The loop fixture makes genuine separate LLM-runtime requests against a local scripted adapter, preserving native usage and stop-control history. No profile is installed or changed, no external process/app is launched, and these tests do not establish rendered Settings or real external delivery.

## Model Experience

### What the model sees

Normal native input with the external producer's source, and two conditional read-only history tools. Actual model output is unchanged. No separate send tool is required to export committed direct replies. Request/header and native usage/cost events are not rewritten.

### Token effect

History tool schemas and explicitly requested history results add tokens. By default no continuation request or automatic full-log dump is made. Explicitly enabled continuation adds bounded extra real model calls, including possible stop-only calls, with their actual native usage/cost. Output sidecar persistence itself does not cost model tokens.

### KV Cache effect

The ordinary Session's context/cache rules remain native. The history tool set changes only when bindings become available/unavailable. No system-prompt replacement is installed.

## Known Limitations and Deferred Work

- Real Cordis/native Session/storageDomain, official Loader+Include composition and real AgentLoop+scripted local LLM tests passed against exact 0.2.2-rc.1 source. App/process launch, profile installation, HMR and rendered display resources remain unverified. No installation or profile change is part of this package's current task.
- The independent non-Chat Client surface is a read-only IO monitor, not a replacement native conversation renderer or a compose/send UI. Native Session input/output remains visible through official source-aware conversation rendering; no renderer is hijacked to hide it. Optional bubbles/picker/reference plugins integrate separately through their documented services.
- The transport-neutral IO package itself does not implement per-binding model/effort selection or quota fallback. The optional QQ bridge implements its own native-selection and next-distinct-input fallback policy; those are adapter features, not required IO dependencies. Model selection remains native, and neither layer should emulate an immediate same-step route switch or replay partial/tool effects.
- Continuation uses an explicit optional protocol and real extra requests; it does not guarantee the model will produce another useful reply, obey the no-tools instruction, or remain below token/time thresholds within one already-admitted request. Only the additional-request count is a hard plugin ceiling.
- Permission-response transport, ask/approval cards, deadlines and external user-authenticated approval submission remain in adapters/official interaction APIs. This package preserves native permissions and does not solve missing approval UX by auto-granting authority.
- Retention requires explicit future archive/reconcile tooling; record capacity currently refuses new work rather than evicting safety-critical dedupe facts. No transactional cross-table backend is assumed; interrupted admission/delivery intents are conservatively unknown.
- Read-only public history intentionally excludes arbitrary trigger/context source kinds and tool output. Native non-IO history remains available through official Session facilities; raw log dumping is not exposed here.
