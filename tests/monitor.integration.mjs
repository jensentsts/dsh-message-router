import test from 'node:test'
import assert from 'node:assert/strict'
import { Context } from '@deepseek-ai/cordis'
import TypertRegistry from '@deepseek-ai/dsh-typert-registry'
import RemoteAccessService from '@deepseek-ai/dsh-api-remote-access'
import TypertGatewayService from '@deepseek-ai/dsh-api-gateway'
import { Peer } from '@deepseek-ai/dsh-client-connection'
import { IoCore } from '../core.mjs'
import { MemoryStore } from '../store.mjs'
import { installMonitor } from '../monitor-host.mjs'

test('strict Gateway cannot turn monitor query into input or bypass explicit owner-only data checks', async () => {
  const ctx = new Context(); let writes = 0
  try {
    await ctx.plugin(TypertRegistry); await ctx.plugin(RemoteAccessService); await ctx.plugin(TypertGatewayService)
    const io = new IoCore({ store: new MemoryStore(), config: { enabled: false, sendEnabled: false }, host: new Proxy({}, { get() { writes++; throw new Error('No native operations') } }) })
    await io.putBinding({ id: 'owner-binding', sessionId: 's', transport: 'qq', endpoint: 'group:private-id' })
    const fiber = ctx.plugin({ name: 'io-monitor-test-provider', inject: ['typert'], apply(inner) { inner.effect(() => installMonitor(inner, io)) } }); await fiber
    const owner = new Peer(ctx, { principal: { kind: 'owner' } }); const guest = new Peer(ctx, { principal: { kind: 'io-monitor-guest' } })
    ctx.remoteAccess.registerPolicy('io-monitor-guest', { bindings: () => [{ resource: { kind: 'global' }, role: { name: 'explicit-global-query-for-test', resource: 'global', actions: ['superAgentIoMonitor/query'] } }] })
    const query = (peer, request) => ctx.typertGateway.invoke({ peer, namespace: 'superAgentIoMonitor', method: 'query', args: { request } })
    const value = await query(owner, { action: 'bindings' }); assert.equal(value.ok, true); assert.equal(value.value.items[0].endpoint, 'group:private-id')
    assert.equal((await query(guest, { action: 'bindings' })).error.code, 'OWNER_REQUIRED')
    const missingPrincipal = await ctx.extend({ invocation: { peer: {} } }).get('superAgentIoMonitor').query({ action: 'bindings' }); assert.equal(missingPrincipal.error.code, 'OWNER_REQUIRED')
    await assert.rejects(query(owner, { action: 'bindings', sendEnabled: true })); await assert.rejects(query(owner, { action: 'receive', bindingId: 'owner-binding' })); await assert.rejects(ctx.typertGateway.invoke({ peer: owner, namespace: 'superAgentIoMonitor', method: 'receive', args: {} }))
    assert.equal(writes, 0); assert.equal(ctx.typert.local.get('superAgentIoMonitor/receive'), undefined)
    await fiber.dispose(); assert.equal(ctx.get('superAgentIoMonitor'), undefined); assert.equal(ctx.typert.local.get('superAgentIoMonitor/query'), undefined); await assert.rejects(query(owner, { action: 'bindings' }))
  } finally { await ctx.fiber.dispose() }
})
