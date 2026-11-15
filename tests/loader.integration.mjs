import test from 'node:test'
import assert from 'node:assert/strict'
import { Context } from '@deepseek-ai/cordis'
import Loader from '@deepseek-ai/cordis-plugin-loader'
import Include from '@deepseek-ai/cordis-plugin-include'
import { createAssistantMessage } from '@deepseek-ai/dsh-llm'
import { Peer } from '@deepseek-ai/dsh-client-connection'

test('official Loader composes native Session IO, denies history escalation, persists and removes effects', async () => {
  const ctx = new Context()
  try {
    await ctx.plugin(Loader); ctx.loader.builtins.include = Include
    await ctx.loader.create({ name: 'cordis:include', config: { path: new URL('./fixtures/cordis.yml', import.meta.url).href } }); await ctx.loader.await()
    const entries = [...ctx.loader.entries()]
    for (const id of ['typert', 'remote-access', 'gateway', 'storage', 'sessions', 'projections', 'agents', 'fixture-external', 'domain', 'super-agent-io']) {
      const entry = entries.find(e => e.options.id === id); assert.ok(entry, id); assert.ok(entry.fiber, `${id}: ${entry.error?.message}; ${JSON.stringify(entry.options)}; keys:${Object.keys(entry)}; state:${entry.state}`)
    }
    const io = ctx.get('superAgentIo'); const fixture = ctx.get('ioFixture'); assert.ok(io)
    const session = ctx.sessions.create('loader-io'); const nativeInputs = []
    const agent = { id: session.id, session, status: 'idle', inbox: { nextTurn: [], nextStep: [] }, followup(message) { nativeInputs.push(message); this.inbox.nextTurn.push(message); session.append('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [message] }) }, cancel() {} }
    await ctx.agents.register(agent)
    await io.putBinding({ id: 'binding', sessionId: session.id, transport: 'no-network', endpoint: 'mock:1', enabled: true, sendEnabled: true })
    const delivery = []; io.registerTransport({ id: 'no-network', deliver: async d => { delivery.push(d); return { state: 'delivered', externalMessageId: String(delivery.length) } } })
    const receipt = await io.receive({ bindingId: 'binding', externalMessageId: 'source-1', content: [{ type: 'text', text: 'original user' }] })
    assert.equal(receipt.acceptedSeq, undefined); assert.equal(fixture.flushes, 0)
    await assert.rejects(io.history({ bindingId: 'binding' }, { sessionId: 'unrelated' }), { code: 'HISTORY_FORBIDDEN' })
    session.append('turn/start', { turn: 1 }); session.append('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [], removedCount: 1 }); const committed = session.append('user/message', nativeInputs[0], { surfaceOp: 'append' })
    for (const [index, content] of [[{ type: 'text', text: 'first native reply' }], [{ type: 'file', attachment: { attachmentId: 'test-file', bytes: 1, name: 'a.txt' } }]].entries()) {
      session.append('step/start', { turn: 1, step: index + 1 })
      session.append('assistant/message', { turn: 1, step: index + 1, stream: [], message: createAssistantMessage({ content, source: { provider: 'fixture-provider', model: 'fixture-model' } }) }, { surfaceOp: 'append' })
      session.append('step/end', { turn: 1, step: index + 1 })
    }
    session.append('turn/end', { turn: 1, reason: { kind: 'completed' } }); await io.drain()
    assert.equal(delivery.length, 2); assert.equal(delivery[1].reply.content[0].type, 'file'); assert.equal(io.receipt(receipt.traceId).acceptedSeq, committed.seq); assert.equal(io.receipt(receipt.traceId).durable, true)
    assert.equal(session.header.version, 4); assert.equal(session.deriveMessages().filter(m => m.role === 'assistant').length, 2); assert.equal(fixture.creates, 0)
    const owner = new Peer(ctx, { principal: { kind: 'owner' } }); const guest = new Peer(ctx, { principal: { kind: 'io-loader-guest' } })
    ctx.remoteAccess.registerPolicy('io-loader-guest', { bindings: () => [{ resource: { kind: 'global' }, role: { name: 'query-fixture', resource: 'global', actions: ['superAgentIoMonitor/query'] } }] })
    const query = peer => ctx.typertGateway.invoke({ peer, namespace: 'superAgentIoMonitor', method: 'query', args: { request: { action: 'bindings' } } })
    assert.equal((await query(owner)).value.items[0].endpoint, 'mock:1'); assert.equal((await query(guest)).error.code, 'OWNER_REQUIRED')
    const state = fixture.snapshots.get('super_agent_io'); assert.equal(Object.values(state.tables.deliveries).filter(d => d.state === 'delivered').length, 2)
    const entry = entries.find(e => e.options.id === 'super-agent-io'); const stopping = entry.fiber; entry.parent.remove(entry.options.id); await stopping.inertia; assert.equal(ctx.get('superAgentIo'), undefined); assert.equal(fixture.tools.size, 0); assert.equal(fixture.closes, 1); assert.equal(ctx.get('superAgentIoMonitor'), undefined); assert.equal(ctx.typert.local.get('superAgentIoMonitor/query'), undefined)
    assert.equal(ctx.agents.get(agent.id), agent); assert.equal(session.deriveMessages().filter(m => m.role === 'assistant').length, 2)
  } finally { await ctx.fiber.dispose() }
})
