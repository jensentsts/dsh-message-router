import test from 'node:test'
import assert from 'node:assert/strict'
import { IoCore } from '../core.mjs'
import { MemoryStore } from '../store.mjs'
import { publicReply, normalizeIngress, normalizeBinding, jsonCopy } from '../contract.mjs'
import { createNativeHost } from '../native-host.mjs'
import { boundedEventPage } from '../history.mjs'

const defer = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b }); return { promise, resolve, reject } }
const text = value => [{ type: 'text', text: value }]
async function fixture(config = {}, seed) {
  const store = new MemoryStore(seed); const logs = new Map(); const agents = new Map(); const sends = []; const delivered = []; const errors = []
  let seqId = 0; let resolutions = 0; let core
  const append = (sessionId, type, data, surfaceOp) => {
    const events = logs.get(sessionId) ?? []; logs.set(sessionId, events)
    const event = { seq: events.length, time: events.length + 1, type, data, ...(surfaceOp ? { surfaceOp } : {}) }; events.push(event)
    const agent = agents.get(sessionId); if (agent) agent.session.seq = events.length
    core?.observe(sessionId, event).catch(() => {})
    return event
  }
  const get = sessionId => {
    if (agents.has(sessionId)) return agents.get(sessionId)
    const inbox = { nextTurn: [], nextStep: [] }; const session = { id: sessionId, seq: (logs.get(sessionId) ?? []).length }
    const send = (mode, message) => { sends.push({ mode, message }); const target = mode === 'steer' ? 'next-step' : 'next-turn'; const q = mode === 'steer' ? inbox.nextStep : inbox.nextTurn; append(sessionId, 'agent/inbox/spliced', { target, start: q.length, inserted: [message] }); q.push(message) }
    const agent = { id: sessionId, status: 'idle', session, inbox, followup: m => send('queue', m), steer: m => send('steer', m), cancel() { agent.cancelled = true } }
    agents.set(sessionId, agent); return agent
  }
  const host = { resolveAgent: async id => { resolutions++; return get(id) }, liveAgent: id => agents.get(id), flush: async () => {}, eventsThrough: async (id, a, b) => (logs.get(id) ?? []).slice(a, b + 1), preview: async (id, b) => boundedEventPage(id, logs.get(id) ?? [], b), readEvent: async (id, seq, b) => boundedEventPage(id, (logs.get(id) ?? []).slice(Math.max(0, seq - b.before), seq + b.after + 1), { maxBytes: b.maxBytes, limit: 11, targetSeq: seq }), createSession: async () => ({ sessionId: 'new-explicit' }) }
  core = new IoCore({ store, host, config: typeof config === 'function' ? () => ({ enabled: true, sendEnabled: true, ...config() }) : { enabled: true, sendEnabled: true, ...config }, uuid: () => `id-${++seqId}`, now: () => 100, report: e => errors.push(e) })
  await core.start()
  const bind = async (id = 'b', sessionId = 's', extra = {}) => core.putBinding({ id, sessionId, transport: 'mock', endpoint: `chat:${id}`, enabled: true, sendEnabled: true, mode: 'auto', ...extra })
  const receive = (id = 'ext', bindingId = 'b', extra = {}) => core.receive({ bindingId, externalMessageId: id, content: text(id), ...extra })
  const start = (sessionId = 's') => {
    const a = get(sessionId); a.status = 'running'; append(sessionId, 'turn/start', { turn: 1 })
    for (const [target, q] of [['next-step', a.inbox.nextStep], ['next-turn', a.inbox.nextTurn]]) {
      const n = target === 'next-step' ? q.length : Math.min(1, q.length)
      if (!n) continue
      const claimed = q.splice(0, n); append(sessionId, 'agent/inbox/spliced', { target, start: 0, removedCount: n, inserted: [] })
      for (const m of claimed) append(sessionId, 'user/message', m, 'append')
    }
  }
  const reply = (value, sessionId = 's', extra = {}) => append(sessionId, 'assistant/message', { turn: 1, step: 1, stream: [], message: { id: `assistant-${++seqId}`, role: 'assistant', source: { kind: 'model', provider: 'actual', model: 'mock', replayState: { secret: true } }, content: Array.isArray(value) ? value : text(value) }, ...extra }, 'append')
  const register = (deliver = async value => { delivered.push(value); return { state: 'delivered', externalMessageId: `out-${delivered.length}` } }) => core.registerTransport({ id: 'mock', deliver })
  return { core, store, host, logs, agents, get, sends, delivered, append, bind, receive, start, reply, register, errors, resolutions: () => resolutions }
}

test('ingress uses durable producer source, dedupes concurrent and restart replay', async () => {
  const f = await fixture(); await f.bind(); const [a, b] = await Promise.all([f.receive('same'), f.receive('same')]); await f.core.drain()
  assert.equal(f.sends.length, 1); assert.equal(a.traceId, b.traceId); assert.equal(b.duplicate, true)
  const m = f.sends[0].message; assert.equal(m.role, 'user'); assert.equal(m.source.kind, 'super-agent-io'); assert.equal(m.source.externalMessageId, 'same'); assert.equal(m.source.inputMode, 'queue'); assert.equal(m.source.rpcId, undefined)
  assert.equal(f.core.receipt(a.traceId).durable, false); assert.equal(f.core.receipt(a.traceId).acceptedSeq, undefined)
  f.start(); await f.core.drain(); assert.equal(f.core.receipt(a.traceId).messageCommitted, true); assert.equal(f.core.receipt(a.traceId).durable, true)
  const again = await fixture({}, f.store.snapshot()); const duplicate = await again.receive('same'); assert.equal(duplicate.duplicate, true); assert.equal(again.sends.length, 0)
})
test('default gates refuse ingress and external deliveries', async () => {
  const f = await fixture({ enabled: false, sendEnabled: false }); await f.bind()
  assert.throws(() => f.receive(), { code: 'DISABLED' }); assert.equal(f.sends.length, 0)
})
test('running auto is bounded steering and respects an existing queued turn', async () => {
  const f = await fixture({ maxConsecutiveSteers: 2 }); await f.bind(); f.get('s').status = 'running'
  const result = []
  for (let i = 0; i < 4; i++) result.push((await f.receive(String(i))).mode)
  assert.deepEqual(result, ['steer', 'steer', 'queue', 'queue']); assert.equal(f.sends.length, 4)
  assert.deepEqual(f.sends.map(({ message }) => message.source.inputMode), result)
})
test('native admission does not wait for model, approval, or durability', async () => {
  const f = await fixture(); await f.bind(); const barrier = defer(); f.host.flush = () => barrier.promise
  const receipt = await f.receive(); assert.equal(receipt.state, 'accepted'); assert.equal(receipt.durable, false)
  f.start()
  const second = await f.receive('second'); assert.equal(second.state, 'accepted'); barrier.resolve(); await f.core.drain()
})
test('cold reference singleflights and a resume error never creates a Session', async () => {
  const f = await fixture(); await f.bind(); const barrier = defer(); const original = f.host.resolveAgent; let count = 0
  f.host.resolveAgent = async id => { count++; await barrier.promise; return original(id) }
  const a = f.core.preload('b'); const b = f.core.preload('b'); barrier.resolve(); await Promise.all([a, b]); assert.equal(count, 1)
  f.host.resolveAgent = async () => { throw Object.assign(new Error('corrupt'), { code: 'CORRUPT' }) }
  assert.equal((await f.receive()).reason, 'CORRUPT'); assert.equal(f.sends.length, 0)
})
test('canonical committed messages preserve multiple replies and file-only output without tools', async () => {
  const f = await fixture(); await f.bind(); f.register(); await f.receive(); f.start()
  f.reply('one'); f.reply([{ type: 'reasoning', text: 'secret' }, { type: 'text', text: 'two' }, { type: 'tool-call', id: 'tool', name: 'x', arguments: '{}' }]); f.reply([{ type: 'file', attachment: { attachmentId: 'f', bytes: 1, name: 'a.txt' } }]); await f.core.drain()
  assert.equal(f.delivered.length, 3); assert.deepEqual(f.delivered[0].reply.content, text('one')); assert.deepEqual(f.delivered[1].reply.content, text('two')); assert.equal(f.delivered[2].reply.content[0].type, 'file')
  assert.equal(f.delivered[0].reply.source.replayState, undefined); assert.equal(f.delivered[0].replyTo, 'ext')
  await f.core.observe('s', f.logs.get('s').at(-1)); await f.core.drain(); assert.equal(f.delivered.length, 3)
  assert.equal(f.store.entries('deliveries').every(([, d]) => d.state === 'delivered'), true)
})
test('receive-only binding commits input and keeps native replies out of the delivery queue', async () => {
  const f = await fixture(); const binding = await f.bind('b', 's', { receiveOnly: true }); f.register()
  const replySource = { actor: 'peer', provider: 'provider', model: 'model', sessionId: 'remote-s', messageId: 'remote-m', seq: 42, turn: 3, step: 1 }
  const receipt = await f.receive('original remote text', 'b', { replySource }); f.start(); f.reply('one'); f.reply('two'); await f.core.drain()
  assert.equal(binding.sendEnabled, false)
  assert.equal(f.core.receipt(receipt.traceId).messageCommitted, true)
  const committed = f.logs.get('s').find(event => event.type === 'user/message').data
  assert.deepEqual(committed.source.replySource, replySource)
  assert.deepEqual(committed.content, text('original remote text'))
  assert.equal(committed.role, 'user')
  assert.equal(f.logs.get('s').filter(event => event.type === 'assistant/message').length, 2)
  assert.equal(f.store.entries('deliveries').length, 0)
  assert.equal(f.delivered.length, 0)
})

test('reasoning attempts, interrupted partials and replacement copies are not exported', () => {
  const base = { type: 'assistant/message', seq: 0, time: 1, surfaceOp: 'append', data: { turn: 1, step: 1, message: { id: 'm', role: 'assistant', source: { kind: 'model', provider: 'p', model: 'm' }, content: [{ type: 'reasoning', text: 'private' }] } } }
  assert.equal(publicReply('s', base), null); assert.equal(publicReply('s', { ...base, type: 'assistant/attempt' }), null)
  base.data.message.content = text('partial'); assert.equal(publicReply('s', { ...base, data: { ...base.data, interrupted: true } }), null)
  assert.equal(publicReply('s', { ...base, surfaceOp: { op: 'replace', startSeq: 0, endSeq: 0 } }), null)
})
test('mixed binding steering and local composer messages fail closed instead of choosing last sender', async () => {
  const f = await fixture(); await f.bind(); await f.bind('b2'); f.register(); await f.receive(); f.start(); await f.receive('other', 'b2', { mode: 'steer' })
  const a = f.get('s'); const claimed = a.inbox.nextStep.splice(0); f.append('s', 'agent/inbox/spliced', { target: 'next-step', start: 0, removedCount: claimed.length, inserted: [] }); for (const m of claimed) f.append('s', 'user/message', m, 'append')
  f.reply('not safe'); await f.core.drain(); assert.equal(f.delivered.length, 0); assert.equal(f.store.entries('deliveries')[0][1].state, 'suppressed')
  const g = await fixture(); await g.bind(); g.register(); await g.receive(); g.start(); g.append('s', 'user/message', { id: 'local', role: 'user', source: { kind: 'user' }, content: text('local') }, 'append'); g.reply('private'); await g.core.drain(); assert.equal(g.delivered.length, 0)
})
test('catch-up commits ingress receipts and only exports the live assistant event', async () => {
  const f = await fixture(); await f.bind()
  const receipt = await f.receive(); await f.core.drain()
  const message = f.sends[0].message
  const events = f.logs.get('s')
  const record = (type, data, surfaceOp) => {
    const seq = events.length
    const event = { type, data, seq, time: seq + 1, surfaceOp }
    events.push(event)
    return event
  }
  const reply = (id, turn, step) => record('assistant/message', {
    turn, step, message: { id, role: 'assistant', source: { kind: 'model', provider: 'actual', model: 'mock' }, content: text(id) },
  }, 'append')
  record('turn/start', { turn: 1 })
  record('user/message', { id: 'legacy', role: 'user', source: { kind: 'qq' }, content: text('legacy') }, 'append')
  reply('legacy-reply', 1, 1)
  record('turn/end', { turn: 1 })
  record('turn/start', { turn: 2 })
  record('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] })
  const committed = record('user/message', message, 'append')
  reply('old-routable-reply', 2, 1)
  const live = reply('live-reply', 2, 2)
  f.register()
  await f.core.observe('s', live); await f.core.drain()

  const accepted = f.core.receipt(receipt.traceId)
  assert.equal(accepted.acceptedSeq, committed.seq)
  assert.equal(accepted.durable, true)
  assert.equal(f.store.get('cursors', 's').lastSeq, live.seq)
  assert.deepEqual(f.store.entries('deliveries').map(([, delivery]) => delivery.reply.messageId), ['live-reply'])
  assert.deepEqual(f.delivered.map(delivery => delivery.reply.messageId), ['live-reply'])
})
test('live unbound replies advance the cursor without a binding audit', async () => {
  const f = await fixture(); await f.bind(); f.register()
  f.start()
  const reply = f.reply('local'); await f.core.drain()
  assert.equal(f.store.get('cursors', 's').lastSeq, reply.seq)
  assert.equal(f.store.entries('deliveries')[0][1].state, 'suppressed')
  assert.equal(f.store.entries('audit').some(([, row]) => row.kind === 'delivery'), false)
  assert.equal(f.delivered.length, 0)
})
test('restart never duplicates deliveries', async () => {
  const f = await fixture(); await f.bind(); await f.receive(); f.start(); f.reply('replay'); await f.core.drain()
  const restart = await fixture({}, f.store.snapshot()); restart.logs.set('s', f.logs.get('s')); restart.register(); await restart.core.observe('s', f.logs.get('s').at(-1)); await restart.core.drain(); assert.equal(restart.delivered.length, 1)
  await restart.core.observe('s', f.logs.get('s').at(-1)); await restart.core.drain(); assert.equal(restart.delivered.length, 1)
})
test('timeout is unknown and is not retried on transport replacement', async () => {
  const f = await fixture({ deliveryTimeoutMs: 5 }); await f.bind(); const remove = f.register(() => new Promise(() => {})); await f.receive(); f.start(); f.reply('one'); await f.core.drain(); remove()
  assert.equal(f.store.entries('deliveries')[0][1].state, 'unknown'); f.register(); await f.core.drain(); assert.equal(f.delivered.length, 0)
})
test('pending sending receipt becomes unknown after restart', async () => {
  const f = await fixture(); await f.bind(); await f.receive(); f.start(); f.reply('one'); await f.core.drain()
  const [key, value] = f.store.entries('deliveries')[0]; await f.store.put('deliveries', key, { ...value, state: 'sending' })
  const restart = await fixture({}, f.store.snapshot()); restart.register(); await restart.core.drain(); assert.equal(restart.store.get('deliveries', key).state, 'unknown'); assert.equal(restart.delivered.length, 0)
})
test('bounded per-binding history is read-only and cross-session calls are denied', async () => {
  const f = await fixture({ historyMaxBytes: 1500, historyLimit: 2 }); await f.bind(); await f.receive('first', 'b', { content: text('中文🙂'.repeat(500)) }); await f.receive('second')
  await assert.rejects(f.core.history({ bindingId: 'b' }, { sessionId: 'elsewhere' }), { code: 'HISTORY_FORBIDDEN' })
  const page = await f.core.history({ bindingId: 'b', kind: 'audit' }, { sessionId: 's' }); assert.ok(Buffer.byteLength(JSON.stringify(page)) <= 1500); assert.ok(page.items.length <= 2); assert.equal(page.truncated, true)
  const next = await f.core.history({ bindingId: 'b', kind: 'audit', cursor: page.nextCursor }, { sessionId: 's' }); assert.notDeepEqual(page.items, next.items)
  await assert.rejects(f.core.history({ bindingId: 'b', kind: 'ingress', cursor: page.nextCursor }, { sessionId: 's' }), { code: 'INVALID_CURSOR' })
})
test('native preview is cold, bounded and excludes hidden channels', async () => {
  const f = await fixture(); await f.bind(); f.append('s', 'system/message', { message: { source: { kind: 'system-prompt' }, content: text('secret') } }, 'append'); f.append('s', 'user/message', { id: 'u', role: 'user', source: { kind: 'user' }, content: text('visible') }, 'append')
  const before = f.resolutions(); const page = await f.core.preview('b'); assert.equal(f.resolutions(), before); assert.equal(page.events.length, 1); assert.equal(page.events[0].text, 'visible')
})
test('admission timeout frees unrelated Session and cancellation prevents late native send', async () => {
  const f = await fixture({ admissionTimeoutMs: 10, maxConcurrentSessions: 1 }); await f.bind(); await f.bind('b2', 's2'); const original = f.host.resolveAgent; const gate = defer()
  f.host.resolveAgent = id => id === 's' ? gate.promise : original(id)
  const a = f.receive('stuck'); const b = f.receive('safe', 'b2'); assert.equal((await a).state, 'failed'); assert.equal((await b).state, 'accepted'); gate.resolve(f.get('s')); await f.core.drain(); assert.equal(f.sends.length, 1)
})
test('cancel bypasses a pending resume and does not wake cold Session', async () => {
  const f = await fixture({ admissionTimeoutMs: 500 }); await f.bind(); const gate = defer(); f.host.resolveAgent = () => gate.promise
  const pending = f.receive(); await Promise.resolve(); await Promise.resolve(); await f.core.cancel('b'); assert.equal((await pending).state, 'cancelled'); gate.resolve(f.get('s')); await f.core.drain(); assert.equal(f.sends.length, 0)
})
test('concurrent backpressure reservations bound work before slow storage completes', async () => {
  const f = await fixture({ maxPendingPerBinding: 1 }); await f.bind(); const put = f.store.put.bind(f.store); const gate = defer()
  f.store.put = async (table, key, value) => { if (table === 'traces' && value.state === 'pending') await gate.promise; return put(table, key, value) }
  const pending = f.receive('first'); await assert.rejects(f.receive('second'), { code: 'BACKPRESSURE' }); gate.resolve(); assert.equal((await pending).state, 'accepted')
})
test('transport registration cleanup and borrowed Agent ownership are explicit', async () => {
  const f = await fixture(); await f.bind(); let disposed = 0; f.get('s').dispose = () => { disposed++ }; const remove = f.register(); assert.throws(() => f.register(), { code: 'DUPLICATE_TRANSPORT' }); remove(); remove(); f.register(); await f.core.close(); assert.equal(disposed, 0); assert.throws(() => f.register(), { code: 'DISPOSED' })
})
test('JSON ingress rejects reasoning, sparse arrays and values that cannot roundtrip', () => {
  assert.throws(() => normalizeIngress({ bindingId: 'b', externalMessageId: 'e', content: [{ type: 'reasoning', text: 'x' }] }), { code: 'INVALID_CONTENT' }); assert.throws(() => jsonCopy({ x: undefined }), { code: 'INVALID_JSON' }); assert.throws(() => jsonCopy(new Array(2)), { code: 'INVALID_JSON' })
})
test('native Host resolver preserves public structured error and never invokes create on it', async () => {
  let creates = 0; const error = Object.assign(new Error('writer held'), { code: 'session/agent-busy' }); const ctx = { sessionController: { resolveAgent: async () => ({ error }), create: () => { creates++ } } }
  await assert.rejects(createNativeHost(ctx).resolveAgent('s'), error); assert.equal(creates, 0)
})
test('delayed native log cannot borrow another event seq or an earlier flush', async () => {
  const f = await fixture(); await f.bind(); let flushes = 0; const barrier = defer(); f.host.flush = () => { flushes++; return barrier.promise }
  const agent = f.get('s'); let input
  agent.followup = message => { input = message; f.append('s', 'request/context', { provider: 'x', model: 'y' }) }
  const receipt = await f.receive(); await f.core.drain(); assert.equal(receipt.state, 'accepted'); assert.equal(receipt.acceptedSeq, undefined); assert.equal(receipt.messageCommitted, false); assert.equal(flushes, 0)
  f.append('s', 'user/message', { ...input, id: 'wrong-id' }, 'append'); await f.core.drain(); assert.equal(f.core.receipt(receipt.traceId).acceptedSeq, undefined); assert.equal(flushes, 0)
  const native = f.append('s', 'user/message', input, 'append')
  // Await only committed event processing; the intentional pending durability barrier is not part of admission.
  await f.core.eventChains.get('s'); assert.equal(f.core.receipt(receipt.traceId).acceptedSeq, native.seq); assert.equal(f.core.receipt(receipt.traceId).messageCommitted, true); assert.equal(f.core.receipt(receipt.traceId).durable, false); assert.equal(flushes, 1)
  barrier.resolve(); await f.core.drain(); assert.equal(f.core.receipt(receipt.traceId).durable, true)
})
test('commit observed before native admission acknowledgement retains the exact anchor', async () => {
  const f = await fixture(); await f.bind(); const agent = f.get('s'); agent.followup = message => { f.append('s', 'user/message', message, 'append') }
  const receipt = await f.receive(); await f.core.drain(); const current = f.core.receipt(receipt.traceId)
  assert.equal(current.acceptedSeq, 0); assert.equal(current.messageCommitted, true); assert.equal(current.durable, true)
})
test('turn context hidden under role user is not exposed by preview', async () => {
  const f = await fixture(); await f.bind(); f.append('s', 'user/message', { id: 'context', role: 'user', source: { kind: 'runtime-context' }, content: text('SECRET RUNTIME TEXT') }, 'append')
  assert.equal((await f.core.preview('b')).events.length, 0)
})
test('live send gates hold pending replies and drain releases only after explicit enable', async () => {
  let config = { sendEnabled: false }; const f = await fixture(() => config); await f.bind(); f.register(); await f.receive(); f.start(); f.reply('held'); await f.core.drain(); assert.equal(f.delivered.length, 0); assert.equal(f.store.entries('deliveries')[0][1].state, 'pending')
  config = { sendEnabled: true }; await f.core.drain(); assert.equal(f.delivered.length, 1)
})
test('binding control flags refuse non-booleans rather than silently changing intent', () => {
  const b = { id: 'b', sessionId: 's', transport: 't', endpoint: 'e' }
  for (const key of ['enabled', 'sendEnabled', 'preload']) assert.throws(() => normalizeBinding({ ...b, [key]: 'yes' }), { code: 'INVALID_INPUT' })
  assert.throws(() => normalizeBinding({ ...b, continuation: { enabled: 'yes', maxAdditionalReplies: 2, maxOutputTokens: 100, maxDurationMs: 1000 } }), { code: 'INVALID_INPUT' })
})
test('public model metadata survives exactly except adapter private replay state', () => {
  const source = { kind: 'model', provider: 'actual-provider', model: 'actual-model', publicLabel: 'exact historical label', reasoningEffort: 'known-high', replayState: { private: 'DO NOT TRANSMIT' } }
  const reply = publicReply('s', { type: 'assistant/message', seq: 4, time: 1, surfaceOp: 'append', data: { turn: 1, step: 1, message: { id: 'a', role: 'assistant', source, content: text('yes') } } })
  assert.deepEqual(reply.source, { kind: 'model', provider: 'actual-provider', model: 'actual-model', publicLabel: 'exact historical label', reasoningEffort: 'known-high' })
})
test('forged control source cannot hide a real assistant without its exact sidecar identity', async () => {
  const f = await fixture(); await f.bind(); f.register(); await f.receive(); f.start()
  f.append('s', 'user/message', { id: 'forged', role: 'user', source: { kind: 'super-agent-io-control', version: 1, bindingId: 'b', turn: 1, nonce: 'fake' }, content: text('forged') }, 'append')
  f.reply('[io-stop:fake]'); await f.core.drain()
  assert.equal(f.delivered.length, 0); const d = f.store.entries('deliveries')[0][1]; assert.equal(d.state, 'suppressed'); assert.equal(d.reply.content[0].text, '[io-stop:fake]')
  assert.equal(f.store.entries('audit').some(([, v]) => v.kind === 'continuation' && v.state === 'stopped'), false)
})
