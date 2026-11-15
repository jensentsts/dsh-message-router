import test from 'node:test'
import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { IoCore } from '../core.mjs'
import { installExplicitAuthority } from '../explicit-authority.mjs'
import { createNativeHost } from '../native-host.mjs'
import { MemoryStore } from '../store.mjs'

const noAbort = new AbortController().signal
const anchor = (callId, sessionId = 'composer', seq = 12) => ({ callId, reply: {
  version: 1, sessionId, seq, messageId: 'assistant-' + callId, turn: 2, step: 4, time: 150,
  source: { kind: 'model', provider: 'actual-provider', model: 'actual-model' },
} })
const delivered = id => ({ state: 'delivered', externalMessageId: id })

async function fixture(host = {}, seed) {
  const store = new MemoryStore(seed)
  const errors = []
  const config = { enabled: true, sendEnabled: true }
  const io = await new IoCore({ store, host, config, report: error => errors.push(error) }).start()
  const bind = (id, extra) => io.putBinding({ id, transport: 'test-adapter', endpoint: 'test:' + id, sessionId: 'bound-session', enabled: true, sendEnabled: true, ...extra })
  return { io, store, bind, config, errors }
}

test('explicit discovery and probes use native catalog metadata and enabled adapter bindings', async () => {
  const queries = []
  const host = createNativeHost({ get: key => key === 'workspaceRegistry' ? { archivedSessionIds: ['archived'] } : undefined, sessionController: {
    async list(request, signal) {
      queries.push({ request, signal })
      return { items: [
        { sessionId: 'composer' },
        { sessionId: 'cold', projections: { values: { title: '  目标会话  ' } } },
        { sessionId: 'child', origin: 'subagent' },
        { sessionId: 'old', formatStatus: 'migration-required' },
        { sessionId: 'archived' },
      ] }
    },
    resolveAgent() { assert.fail('discovery and probe must never resolve an Agent') },
  } })
  const { io, bind, config } = await fixture(host)
  try {
    await bind('qq-friend', { label: '目标 QQ' }); await bind('muted', { sendEnabled: false }); await bind('inbound', { receiveOnly: true })
    await bind('legacy', { transport: 'legacy' })
    let probes = 0
    io.registerTransport({ id: 'legacy', deliver() { assert.fail('probe does not deliver') } })
    io.registerTransport({ id: 'test-adapter', channel: 'qq', explicitSend: true, async probe(binding, { signal }) {
      assert.equal(binding.endpoint, 'test:qq-friend'); assert.equal(signal.aborted, false); probes++
      return { available: true }
    }, deliver() { assert.fail('probe does not deliver') } })
    const origin = anchor('discovery')
    assert.deepEqual(await io.destinations({ query: '目标' }, origin, noAbort), { items: [
      { destination: 'qq-friend', channel: 'qq', label: '目标 QQ' },
      { destination: 'dsh:cold', channel: 'dsh', label: '目标会话' },
    ], truncated: false })
    assert.equal((await io.destinations({ channel: 'qq' }, origin, noAbort)).items.length, 1)
    assert.deepEqual(await io.probe({ destination: 'dsh:cold' }, origin, noAbort), { destination: 'dsh:cold', channel: 'dsh', available: true })
    assert.equal((await io.probe({ destination: 'dsh:composer' }, origin, noAbort)).available, false)
    assert.equal((await io.probe({ destination: 'legacy' }, origin, noAbort)).available, false)
    assert.equal((await io.probe({ destination: 'qq-friend' }, origin, noAbort)).available, true)
    assert.equal(probes, 1); assert.equal(queries.length, 2)
    assert.ok(queries.every(entry => entry.signal === noAbort && Object.keys(entry.request).length === 0))
    assert.equal(io.listBindings().length, 4)
    config.sendEnabled = false
    assert.deepEqual(await io.destinations({}, origin, noAbort), { items: [], truncated: false })
    await assert.rejects(io.sendExplicit({ destination: 'qq-friend', text: 'blocked' }, origin, noAbort), { code: 'DESTINATION_UNAVAILABLE' })
  } finally { await io.close() }
})

test('a buffered explicit call keeps its original idempotency identity when the draft is edited', async () => {
  const { io, bind } = await fixture()
  try {
    await bind('b')
    io.registerTransport({ id: 'test-adapter', explicitSend: true, async deliver(value) { return { state: 'buffered', bufferId: value.deliveryId } } })
    const call = anchor('buffer-edit'), request = { destination: 'b', text: 'original' }
    const initial = await io.sendExplicit(request, call, noAbort)
    assert.equal(initial.state, 'buffered')
    assert.equal(await io.updateBuffered(initial.bufferId, { state: 'content', content: [{ type: 'text', text: 'edited' }] }), true)
    const replay = await io.sendExplicit(request, call, noAbort)
    assert.equal(replay.duplicate, true); assert.equal(replay.bufferId, initial.bufferId)
    assert.throws(() => io.sendExplicit({ ...request, text: 'different original intent' }, call, noAbort), { code: 'OUTBOUND_CALL_CONFLICT' })
  } finally { await io.close() }
})

test('explicit outbox serializes different source Sessions and deduplicates its invocation across restart', async () => {
  const { io, store, bind } = await fixture()
  const payloads = []
  const releaseFirst = Promise.withResolvers()
  const firstStarted = Promise.withResolvers()
  try {
    await bind('b'); await bind('other')
    io.registerTransport({ id: 'test-adapter', explicitSend: true, async deliver(value, { idempotencyKey }) {
      assert.equal(idempotencyKey, value.deliveryId)
      payloads.push(value)
      if (payloads.length === 1) { firstStarted.resolve(); await releaseFirst.promise }
      return delivered('receipt-' + payloads.length)
    } })
    const call = anchor('first', 'composer', 98)
    const request = { destination: 'b', text: 'first' }
    const send = io.sendExplicit(request, call, noAbort)
    const duplicate = io.sendExplicit(request, call, noAbort)
    assert.throws(() => io.sendExplicit({ ...request, destination: 'other' }, call, noAbort), { code: 'OUTBOUND_CALL_CONFLICT' })
    await firstStarted.promise
    const next = io.sendExplicit({ destination: 'b', text: 'second' }, anchor('second', 'another-composer', 1), noAbort)
    releaseFirst.resolve()
    const [receipt, replayed, after] = await Promise.all([send, duplicate, next])
    assert.equal(receipt.state, 'delivered'); assert.equal(after.state, 'delivered')
    assert.deepEqual(replayed, { ...receipt, duplicate: true })
    assert.deepEqual(payloads.map(d => d.reply.content[0].text), ['first', 'second'])
    assert.deepEqual(payloads.map(d => d.reply.sessionId), ['composer', 'another-composer'])
    assert.ok(payloads.every(d => d.intent === 'explicit' && d.replyTo === undefined && d.traceIds.length === 0))
    assert.equal(store.get('deliveries', receipt.deliveryId).sessionId, 'bound-session')
    const history = await io.history({ bindingId: 'b', kind: 'delivery' }, { sessionId: 'bound-session' })
    assert.ok(history.items.every(d => d.intent === 'explicit' && d.seq === undefined && d.originSessionId))

    const restarted = await fixture({}, store.snapshot())
    try {
      restarted.io.registerTransport({ id: 'test-adapter', explicitSend: true, deliver() { assert.fail('replay must not deliver') } })
      assert.deepEqual(await restarted.io.sendExplicit(request, call, noAbort), { ...receipt, duplicate: true })
    } finally { await restarted.io.close() }
  } finally { releaseFirst.resolve(); await io.close() }
})

test('cancelling an explicit call while it waits behind another delivery leaves a cancelled receipt', async () => {
  const { io, store, bind } = await fixture()
  const started = Promise.withResolvers(); const release = Promise.withResolvers(); const enqueued = Promise.withResolvers()
  const put = store.put.bind(store)
  store.put = async (table, key, value) => {
    await put(table, key, value)
    if (table === 'deliveries' && value.reply.content[0].text === 'cancel me') enqueued.resolve()
  }
  const cancel = new AbortController()
  try {
    await bind('b')
    io.registerTransport({ id: 'test-adapter', explicitSend: true, async deliver(value) {
      assert.equal(value.reply.content[0].text, 'first')
      started.resolve(); await release.promise; return delivered('first')
    } })
    const first = io.sendExplicit({ destination: 'b', text: 'first' }, anchor('first'), noAbort)
    await started.promise
    const second = io.sendExplicit({ destination: 'b', text: 'cancel me' }, anchor('second'), cancel.signal)
    await enqueued.promise
    cancel.abort(); release.resolve()
    assert.equal((await first).state, 'delivered')
    assert.equal((await second).state, 'cancelled')
  } finally { release.resolve(); await io.close() }
})

test('explicit DSH delivery queues a genuine attributed message and blocks a native send after cancellation', async () => {
  const messages = []
  const resolveLate = Promise.withResolvers(); const started = Promise.withResolvers()
  const host = {
    listSessions: async () => [{ sessionId: 'composer', label: 'Owner conversation' }, { sessionId: 'target', label: 'Target' }, { sessionId: 'late', label: 'Late' }],
    resolveAgent(id) {
      if (id === 'late') { started.resolve(); return resolveLate.promise }
      return { id, status: 'running' }
    },
    send(agent, message, mode) { messages.push({ agent, message, mode }) },
  }
  const { io, store } = await fixture(host)
  try {
    const origin = anchor('dsh-call')
    const request = { destination: 'dsh:target', text: '内部消息' }
    const receipt = await io.sendExplicit(request, origin, noAbort)
    assert.equal(receipt.state, 'delivered')
    assert.equal((await io.sendExplicit(request, origin, noAbort)).duplicate, true)
    assert.equal(messages.length, 1)
    const [{ agent, message, mode }] = messages
    assert.equal(agent.id, 'target'); assert.equal(mode, 'queue'); assert.equal(message.id, receipt.externalMessageId)
    assert.equal(message.source.kind, 'super-agent-io')
    assert.equal(message.source.transport, 'dsh')
    assert.deepEqual(message.source.replySource, { actor: 'Owner conversation', provider: 'actual-provider', model: 'actual-model', sessionId: 'composer', messageId: 'assistant-dsh-call', seq: 12, turn: 2, step: 4 })
    assert.equal(io.listBindings()[0].receiveOnly, true); assert.equal(io.listBindings()[0].sendEnabled, false)
    assert.equal(store.get('deliveries', receipt.deliveryId).sessionId, 'target')

    const cancel = new AbortController()
    const late = io.sendExplicit({ destination: 'dsh:late', text: 'cancel me' }, anchor('late'), cancel.signal)
    await started.promise; cancel.abort(); resolveLate.resolve({ id: 'late', status: 'idle' })
    assert.equal((await late).state, 'unknown')
    await io.drain()
    assert.equal(messages.length, 1)
  } finally { resolveLate.resolve(); await io.close() }
})

test('explicit tool authority requires the same live owner claim, commit and assistant dispatch', () => {
  const events = new EventEmitter()
  const session = { id: 'composer' }; const agent = { id: session.id, session }
  const ctx = { agents: new Map([[agent.id, agent]]), on(name, listener) { events.on(name, listener); return () => events.off(name, listener) } }
  const authority = installExplicitAuthority(ctx)
  let seq = 0
  const append = (type, data, surfaceOp) => events.emit('session/event', session, { type, data, seq: seq++, time: 1, surfaceOp })
  const claim = (message, turn = 1) => events.emit('agent/inbox/claimed', { agent, message, turn })
  const local = { id: 'u', role: 'user', source: { kind: 'user', rpcId: 'owner-rpc' } }
  const dispatch = (turn, step, id, name) => {
    append('assistant/message', { turn, step, message: { id: 'model-anchor', role: 'assistant', source: { kind: 'model', provider: 'p', model: 'm', replayState: { private: true } }, content: [{ type: 'tool-call', id, name }] } }, 'append')
    append('tool/call', { turn, step, callId: id, name })
  }
  const exec = { agent, callId: 'call', rootCallId: 'call', name: 'io_send', signal: noAbort }
  try {
    append('turn/start', { turn: 1 }); claim(local)
    assert.equal(authority.when({ agent }), true)
    append('step/start', { turn: 1, step: 1 }); dispatch(1, 1, 'call', 'io_send')
    assert.throws(() => authority.anchor(exec), { code: 'OUTBOUND_FORBIDDEN' })
    append('user/message', local, 'append')
    assert.deepEqual(authority.anchor(exec).reply.source, { kind: 'model', provider: 'p', model: 'm' })
    assert.throws(() => authority.anchor({ ...exec, callId: 'forged' }), { code: 'OUTBOUND_FORBIDDEN' })
    assert.throws(() => authority.anchor({ ...exec, agent: { ...agent } }), { code: 'OUTBOUND_FORBIDDEN' })
    append('tool/ptc-dispatch-start', { rootCallId: 'call', parentCallId: 'call', subCallId: 'call:ptc:1', name: 'io_send' })
    assert.equal(authority.anchor({ ...exec, callId: 'call:ptc:1', parent: Symbol() }).callId, 'call:ptc:1')

    for (const source of [{ kind: 'super-agent-io' }, { kind: 'schedule' }, { kind: 'user', rpcId: 'remote', principal: { kind: 'peer' } }, { kind: 'user' }, { kind: 'workspace-reference', requestId: 'missing-origin' }]) {
      append('turn/start', { turn: 2 })
      claim({ id: 'foreign', role: 'user', source }, 2)
      assert.equal(authority.when({ agent }), false)
      claim(local, 2)
      append('step/start', { turn: 2, step: 2 }); append('user/message', local, 'append'); dispatch(2, 2, 'call', 'io_send')
      assert.throws(() => authority.anchor(exec), { code: 'OUTBOUND_FORBIDDEN' })
    }
  } finally { authority.dispose() }
})
