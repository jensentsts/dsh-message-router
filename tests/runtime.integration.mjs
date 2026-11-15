import test from 'node:test'
import assert from 'node:assert/strict'
import { Context } from '@deepseek-ai/cordis'
import * as domainPlugin from '@deepseek-ai/dsh-storage-domain'
import storagePlugin from '@deepseek-ai/dsh-storage'
import sessionPlugin from '@deepseek-ai/dsh-session'
import projectionPlugin from '@deepseek-ai/dsh-session-projection'
import agentPlugin from '@deepseek-ai/dsh-agent'
import { createUserMessage, createAssistantMessage } from '@deepseek-ai/dsh-llm'
import { defineTool } from '@deepseek-ai/dsh-tools'
import * as plugin from '../index.mjs'
import { domainSpec } from '../domain.mjs'
import { DomainStore } from '../store.mjs'

async function makeContext() {
  const ctx = new Context(); const snapshots = new Map(); const definitions = new Map(); let closes = 0
  await ctx.plugin(storagePlugin)
  ctx.storage.backend.register('io-test', { kv: { async open(descriptor) {
    const state = snapshots.get(descriptor.name) ?? { tables: {}, global: null }; snapshots.set(descriptor.name, state)
    return { loadAll: async () => structuredClone(state), async putRecord(table, key, value) { state.tables[table] ??= {}; state.tables[table][key] = structuredClone(value) }, async deleteRecord(table, key) { delete state.tables[table]?.[key] }, async deleteRecords(table, keys) { for (const key of keys) delete state.tables[table]?.[key] }, async setGlobal(value) { state.global = structuredClone(value) }, async close() { closes++ } }
  } } })
  ctx.provide('storage.backend.io-test', {})
  await ctx.plugin(domainPlugin, { backend: 'io-test' })
  await ctx.plugin(sessionPlugin)
  await ctx.plugin(projectionPlugin)
  await ctx.plugin(agentPlugin)
  ctx.provide('sessionController', { async resolveAgent(sessionId) { const agent = ctx.agents.get(sessionId); return agent ? { agent } : { error: Object.assign(new Error('no session'), { code: 'session/not-found' }) } } })
  ctx.provide('sessionQuery', { async observeSession(id) { const session = ctx.sessions.get(id); return { events: session.snapshotEvents(), [Symbol.dispose]() {} } }, async readEvent({ sessionId, seq, before, after }) { return { events: ctx.sessions.get(sessionId).snapshotEvents().slice(Math.max(0, seq - before), seq + after + 1) } } })
  ctx.provide('tools', { register(definition) { if (definitions.has(definition.name)) throw new Error('duplicate'); definitions.set(definition.name, definition); return () => definitions.delete(definition.name) } })
  ctx.provide('sessionPersistence', { flush: async () => {} })
  return { ctx, snapshots, definitions, closes: () => closes }
}
test('exact Cordis, native Session and storageDomain keep real IO data readable and teardown effects', async () => {
  const { ctx, snapshots, definitions, closes } = await makeContext()
  let fiber
  try {
    fiber = ctx.plugin(plugin, { enabled: true, sendEnabled: true }); await fiber
    const io = ctx.get('superAgentIo'); assert.equal(io.contractVersion, 1); assert.deepEqual([...definitions.keys()], ['io_history', 'io_history_read', 'io_destinations', 'io_probe', 'io_send'])
    const session = ctx.sessions.create('io-session'); const input = []
    const agent = { id: session.id, session, status: 'idle', followup(message) { input.push(message); session.append('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [message] }) }, steer(message) { input.push(message); session.append('agent/inbox/spliced', { target: 'next-step', start: 0, inserted: [message] }) }, cancel() {} }
    await ctx.agents.register(agent)
    await io.putBinding({ id: 'b', transport: 'mock-native', endpoint: 'p:1', sessionId: session.id, enabled: true, sendEnabled: true })
    const output = []; const unregister = io.registerTransport({ id: 'mock-native', deliver: async value => { output.push(value); return { state: 'delivered', externalMessageId: String(output.length) } } })
    const receipt = await io.receive({ bindingId: 'b', externalMessageId: 'external-1', content: [{ type: 'text', text: 'hello native' }] })
    assert.equal(receipt.acceptedSeq, undefined); assert.equal(receipt.durable, false); assert.equal(input[0].source.kind, 'super-agent-io')
    session.append('turn/start', { turn: 1 }); session.append('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] }); const committed = session.append('user/message', input[0], { surfaceOp: 'append' })
    for (const [i, text] of ['first reply', 'second reply'].entries()) {
      session.append('step/start', { turn: 1, step: i + 1 })
      session.append('assistant/message', { turn: 1, step: i + 1, stream: [], message: createAssistantMessage({ content: [{ type: 'text', text }], source: { provider: 'actual-provider', model: 'actual-model' } }) }, { surfaceOp: 'append' })
      session.append('step/end', { turn: 1, step: i + 1 })
    }
    session.append('turn/end', { turn: 1, reason: { kind: 'completed' } }); await io.drain()
    assert.equal(output.length, 2); assert.equal(io.receipt(receipt.traceId).acceptedSeq, committed.seq); assert.equal(io.receipt(receipt.traceId).durable, true)
    assert.equal(session.header.version, 4); assert.equal(session.deriveMessages().filter(m => m.role === 'assistant').length, 2)
    assert.equal(session.deriveMessages().filter(m => m.role === 'user')[0].source.kind, 'super-agent-io')
    const state = snapshots.get('super_agent_io'); for (const [table, rows] of Object.entries(state.tables)) for (const value of Object.values(rows)) domainSpec.tables[table].valueSchema.parse(value)
    unregister(); await fiber.dispose(); assert.equal(ctx.get('superAgentIo'), undefined); assert.equal(definitions.size, 0); assert.ok(closes() >= 1)
    // Unloading the plugin removes no canonical messages and disposes no borrowed Agent.
    assert.equal(ctx.agents.get(agent.id), agent); assert.equal(session.deriveMessages().filter(m => m.role === 'assistant').length, 2)
  } finally { await ctx.fiber.dispose() }
})
test('exact tool schema compiler accepts bounded read-only history definitions', async () => {
  const { historyTools } = await import('../tools.mjs')
  const definitions = historyTools({ history: async () => ({ items: [], truncated: false }), historyRead: async () => ({ events: [] }) }, defineTool)
  assert.equal(definitions.length, 2)
})
test('schema rejects invalid stored state before production adapter writes', async () => {
  const { ctx } = await makeContext()
  try {
    const domain = await ctx.storageDomain.open(domainSpec); const store = new DomainStore(domain, domainSpec)
    assert.throws(() => store.put('bindings', 'bad', { version: 999 }), /Invalid/)
    await store.close()
  } finally { await ctx.fiber.dispose() }
})
