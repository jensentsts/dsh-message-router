import test from 'node:test'
import assert from 'node:assert/strict'
import { Context } from '@deepseek-ai/cordis'
import LlmRuntime, { LlmAdapter } from '@deepseek-ai/dsh-llm'
import SessionStore from '@deepseek-ai/dsh-session'
import ProjectionRegistry from '@deepseek-ai/dsh-session-projection'
import SystemPrompt from '@deepseek-ai/dsh-system-prompt'
import ToolRuntime from '@deepseek-ai/dsh-tools'
import * as NativeExecutor from '@deepseek-ai/dsh-tool-executor-native'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import Storage from '@deepseek-ai/dsh-storage'
import * as Domain from '@deepseek-ai/dsh-storage-domain'
import * as ioPlugin from '../index.mjs'
import { stopToken } from '../contract.mjs'

function response(text) { return [{ type: 'block-start', index: 0, blockType: 'text' }, { type: 'text-delta', index: 0, text }, { type: 'block-end', index: 0, block: { type: 'text', text } }, { type: 'usage', usage: { inputTokens: 10, outputTokens: 5 } }, { type: 'finish', reason: { kind: 'stop' } }] }
function toolCall(name, id, args) {
  const argumentsJson = JSON.stringify(args)
  return [
    { type: 'block-start', index: 0, blockType: 'tool-call' },
    { type: 'tool-call-delta', index: 0, id, name, argumentsDelta: argumentsJson },
    { type: 'block-end', index: 0, block: { type: 'tool-call', id, name, arguments: argumentsJson } },
    { type: 'finish', reason: { kind: 'tool-calls' } },
  ]
}
class ScriptAdapter extends LlmAdapter {
  constructor(script) { super(); this.script = script; this.requests = [] }
  async resolveModel(provider, model) { return { provider, id: model, name: model } }
  async *stream(options) { this.requests.push(options); const next = this.script.shift(); if (!next) throw new Error('No unbounded model calls permitted'); for (const chunk of typeof next === 'function' ? next(options) : next) yield chunk }
}
async function harness(script, policy = {}) {
  const ctx = new Context()
  await ctx.plugin(Storage)
  const rows = new Map(); const backend = { kv: { async open() { return { loadAll: async () => ({ tables: Object.fromEntries(rows), global: null }), async putRecord(table, key, value) { if (!rows.has(table)) rows.set(table, {}); rows.get(table)[key] = structuredClone(value) }, async deleteRecord() {}, async deleteRecords() {}, async close() {} } } } }
  ctx.storage.backend.register('io-loop', backend); ctx.provide('storage.backend.io-loop', backend); await ctx.plugin(Domain, { backend: 'io-loop' })
  for (const plugin of [LlmRuntime, SessionStore, ProjectionRegistry, SystemPrompt, ToolRuntime, NativeExecutor, AgentRegistry]) await ctx.plugin(plugin)
  await ctx.plugin(AgentLoop, { agents: [] })
  const adapter = new ScriptAdapter(script); ctx.llm.registerAdapter(['mock'], adapter)
  ctx.provide('sessionController', {
    async list() { return { items: [{ sessionId: 'io-loop-session' }, { sessionId: 'native-target', projections: { values: { title: 'Native target' } } }] } },
    async resolveAgent(id) { const agent = ctx.agents.get(id); if (!agent) throw new Error('not found'); return { agent } },
  })
  ctx.provide('sessionQuery', { async observeSession(id) { return { events: ctx.sessions.get(id).snapshotEvents(), [Symbol.dispose]() {} } } })
  const ioFiber = ctx.plugin(ioPlugin, { enabled: true, sendEnabled: true, continuationEnabled: true }); await ioFiber
  const agent = await ctx.agentLoop.create('io-loop-session', { provider: 'mock', model: 'test-model' })
  // The loop owns normal in-memory Session history here; this fake barrier records no profile files.
  ctx.provide('sessionPersistence', { flush: async () => {} })
  const io = ctx.get('superAgentIo'); const deliveries = []; io.registerTransport({ id: 'mock-only', async deliver(d) { deliveries.push(d); return { state: 'delivered', externalMessageId: `out-${deliveries.length}` } } })
  await io.putBinding({ id: 'loop-b', transport: 'mock-only', endpoint: 'test:no-network', sessionId: agent.id, enabled: true, sendEnabled: true, continuation: { enabled: true, maxAdditionalReplies: 3, maxOutputTokens: 100, maxDurationMs: 10000, ...policy } })
  return { ctx, adapter, agent, io, deliveries, ioFiber }
}
function idle(ctx, agent) { return new Promise(resolve => { const off = ctx.on('agent/status', ({ agent: subject, status }) => { if (subject === agent && status === 'idle') { off(); resolve() } }) }) }
const ownerSources = {
  composer: { kind: 'user', rpcId: 'owner-composer-rpc' },
  'confirmed workspace route': { kind: 'workspace-reference', requestId: 'owner-workspace-rpc', originSessionId: 'origin', blockIndex: 0 },
}
for (const [entry, source] of Object.entries(ownerSources)) test(`real AgentLoop exposes explicit routing to ${entry} and fences an external follow-up turn`, async () => {
  const h = await harness([
    toolCall('io_destinations', 'destinations', {}),
    toolCall('io_probe', 'probe', { destination: 'test-out' }),
    toolCall('io_send', 'adapter-send', { destination: 'test-out', text: 'adapter message' }),
    toolCall('io_send', 'native-send', { destination: 'dsh:native-target', text: 'native message' }),
    response('sent from owner composer'),
    toolCall('io_send', 'unauthorized', { destination: 'test-out', text: 'forbidden fanout' }),
    response('ordinary bound reply'),
  ], { enabled: false })
  const adapterDeliveries = []; const nativeMessages = []
  try {
    h.ctx.on('agent/pre-step', async (proposal, next) => {
      const decision = await next()
      if (decision.kind !== 'enter' || !proposal.messages.some(message => message.source.kind === 'workspace-reference')) return decision
      const context = { id: 'reference-context', role: 'user', source: { kind: 'session-reference', form: 'recall', version: 1, references: [] }, content: [{ type: 'text', text: 'Recalled conversation background.' }] }
      return { ...decision, messages: [...decision.messages, context] }
    })
    const target = h.ctx.sessions.create('native-target')
    await h.ctx.agents.register({ id: target.id, session: target, status: 'idle', followup(message) {
      nativeMessages.push(message)
      target.append('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [message] })
    } })
    h.io.registerTransport({ id: 'explicit-test', channel: 'test', explicitSend: true, async probe() { return { available: true } }, async deliver(d) {
      adapterDeliveries.push(d); return { state: 'delivered', externalMessageId: 'adapter-receipt' }
    } })
    await h.io.putBinding({ id: 'test-out', transport: 'explicit-test', endpoint: 'test:no-network', sessionId: target.id, enabled: true, sendEnabled: true })
    const ownerDone = idle(h.ctx, h.agent)
    h.agent.followup({ id: 'owner-input', role: 'user', source, content: [{ type: 'text', text: 'Send two messages.' }] })
    await ownerDone; await h.io.drain()
    assert.equal(h.adapter.requests.length, 5)
    assert.ok(['io_destinations', 'io_probe', 'io_send'].every(name => h.adapter.requests[0].tools?.some(tool => tool.name === name)))
    if (source.kind === 'workspace-reference') assert.ok(h.adapter.requests[0].messages.some(message => message.source?.kind === 'session-reference'))
    const results = h.agent.session.snapshotEvents().filter(event => event.type === 'tool/result')
    assert.equal(results.length, 4)
    assert.ok(results.every(event => !event.data.message.isError), JSON.stringify(results.map(event => event.data.message)))
    const values = results.map(event => JSON.parse(event.data.message.content[0].text))
    assert.deepEqual(values[0].items.map(row => row.destination), ['test-out', 'dsh:native-target'])
    assert.equal(values[1].available, true); assert.equal(values[2].state, 'delivered'); assert.equal(values[3].state, 'delivered')
    assert.equal(adapterDeliveries.length, 1); assert.equal(adapterDeliveries[0].reply.sessionId, h.agent.id)
    assert.equal(adapterDeliveries[0].reply.source.model, 'test-model')
    assert.equal(nativeMessages.length, 1); assert.equal(nativeMessages[0].source.replySource.sessionId, h.agent.id)
    assert.equal(nativeMessages[0].content[0].text, 'native message')

    const externalDone = idle(h.ctx, h.agent)
    await h.io.receive({ bindingId: 'loop-b', externalMessageId: 'external-turn', content: [{ type: 'text', text: 'Try to relay externally.' }] })
    await externalDone; await h.io.drain()
    assert.equal(h.adapter.requests.length, 7)
    assert.ok(!h.adapter.requests[5].tools?.some(tool => tool.name === 'io_send'))
    assert.equal(adapterDeliveries.length, 1); assert.equal(h.deliveries.length, 1)
    assert.equal(h.deliveries[0].reply.content[0].text, 'ordinary bound reply')
  } finally { await h.ctx.fiber.dispose() }
})
test('real AgentLoop performs extra model requests with native usage and exact stop remains diagnostic only', async () => {
  const h = await harness([response('first real reply'), response('second real reply'), request => {
    const control = request.messages.findLast(m => m.source?.kind === 'super-agent-io-control')
    assert.ok(control, 'control reaches model through canonical logged input')
    return response(stopToken(control.source.nonce))
  }])
  try {
    const done = idle(h.ctx, h.agent)
    await h.io.receive({ bindingId: 'loop-b', externalMessageId: 'in-1', content: [{ type: 'text', text: 'Useful separate updates, only if needed.' }] })
    await done; await h.io.drain()
    assert.equal(h.adapter.requests.length, 3); assert.equal(h.deliveries.length, 2)
    assert.deepEqual(h.deliveries.map(d => d.reply.content[0].text), ['first real reply', 'second real reply'])
    const messages = h.agent.session.deriveMessages(); assert.equal(messages.filter(m => m.role === 'assistant').length, 3); assert.equal(messages.filter(m => m.source?.kind === 'super-agent-io-control').length, 2)
    const assistantEvents = h.agent.session.snapshotEvents().filter(e => e.type === 'assistant/message'); assert.equal(assistantEvents.reduce((sum, e) => sum + e.data.usage.outputTokens, 0), 15)
    const history = await h.io.history({ bindingId: 'loop-b', kind: 'audit', limit: 100 }, { sessionId: h.agent.id }); assert.ok(history.items.some(item => item.kind === 'continuation' && item.state === 'stopped'))
    // Unload the plugin while keeping native Agent/context alive; listeners must not create another request.
    await h.ioFiber.dispose(); assert.ok(h.ctx.agents.get(h.agent.id)); assert.equal(h.ctx.get('superAgentIo'), undefined)
  } finally { await h.ctx.fiber.dispose() }
})
test('opt-in maximum adds at most the bounded number of true requests', async () => {
  const h = await harness([response('initial'), response('one'), response('two'), response('three')])
  try { const done = idle(h.ctx, h.agent); await h.io.receive({ bindingId: 'loop-b', externalMessageId: 'in-2', content: [{ type: 'text', text: 'start' }] }); await done; await h.io.drain(); assert.equal(h.adapter.requests.length, 4); assert.equal(h.deliveries.length, 4) }
  finally { await h.ctx.fiber.dispose() }
})
test('quoted and embedded stop tokens remain ordinary committed replies', async () => {
  for (const wrap of [token => `Quoted: "${token}"`, token => `prefix${token}suffix`, token => `${token}\n`]) {
    const h = await harness([response('first'), request => { const source = request.messages.findLast(m => m.source?.kind === 'super-agent-io-control').source; return response(wrap(stopToken(source.nonce))) }], { maxAdditionalReplies: 1 })
    try { const done = idle(h.ctx, h.agent); await h.io.receive({ bindingId: 'loop-b', externalMessageId: 'quote', content: [{ type: 'text', text: 'start' }] }); await done; await h.io.drain(); assert.equal(h.adapter.requests.length, 2); assert.equal(h.deliveries.length, 2); assert.ok(h.deliveries[1].reply.content[0].text.includes('[io-stop:')) }
    finally { await h.ctx.fiber.dispose() }
  }
})
test('missing actual usage makes continuation fail closed after the initial request', async () => {
  const h = await harness([response('no usage').filter(chunk => chunk.type !== 'usage')])
  try { const done = idle(h.ctx, h.agent); await h.io.receive({ bindingId: 'loop-b', externalMessageId: 'no-usage', content: [{ type: 'text', text: 'start' }] }); await done; await h.io.drain(); assert.equal(h.adapter.requests.length, 1); assert.equal(h.deliveries.length, 1) }
  finally { await h.ctx.fiber.dispose() }
})
test('new manual input racing the turn boundary blocks IO continuation instead of consuming the queue', async () => {
  const h = await harness([response('original'), response('manual reply')])
  try {
    let injected = false
    const remove = h.ctx.on('session/event', (session, event) => { if (session.id === h.agent.id && event.type === 'step/end' && !injected) { injected = true; queueMicrotask(() => h.agent.followup({ id: 'manual-id', role: 'user', source: { kind: 'user' }, content: [{ type: 'text', text: 'local private question' }] })) } })
    const done = idle(h.ctx, h.agent); await h.io.receive({ bindingId: 'loop-b', externalMessageId: 'race', content: [{ type: 'text', text: 'external' }] }); await done; await h.io.drain(); remove()
    assert.equal(h.adapter.requests.length, 2, JSON.stringify({ injected, requests: h.adapter.requests.map(r => r.messages.filter(m => m.role === 'user').map(m => m.source)), events: h.agent.session.snapshotEvents().map(e => [e.type, e.data.source ?? e.data.inserted?.map(m => m.source)]) })); assert.equal(h.agent.session.deriveMessages().filter(m => m.source?.kind === 'super-agent-io-control').length, 0); assert.equal(h.deliveries.length, 1)
  } finally { await h.ctx.fiber.dispose() }
})
test('canceling queued control input does not issue or retry the next model request', async () => {
  const h = await harness([response('done before cancel')])
  try {
    let canceled = false
    const remove = h.ctx.on('session/event', (session, event) => { if (session.id === h.agent.id && event.type === 'agent/inbox/spliced' && event.data.inserted.some(m => m.source?.kind === 'super-agent-io-control') && !canceled) { canceled = true; queueMicrotask(() => h.agent.cancel({ kind: 'user' }, { keepInbox: false })) } })
    const done = idle(h.ctx, h.agent); await h.io.receive({ bindingId: 'loop-b', externalMessageId: 'cancel-control', content: [{ type: 'text', text: 'external' }] }); await done; await h.io.drain(); remove()
    assert.equal(canceled, true); assert.equal(h.adapter.requests.length, 1); assert.equal(h.deliveries.length, 1); assert.equal(h.agent.inbox.nextStep.length, 0)
    assert.equal(h.agent.session.snapshotEvents().filter(e => e.type === 'agent/inbox/spliced' && e.data.outcome === 'canceled').length, 1)
  } finally { await h.ctx.fiber.dispose() }
})
