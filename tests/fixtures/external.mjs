/** Fixture-only nondeterministic driver and storage medium; real Cordis, Session and domain providers are composed by Loader. */
import { storageBackendServiceKey } from '@deepseek-ai/dsh-storage'
export const name = 'io-fixture-external'
export const inject = ['storage', 'sessions', 'agents']
export function apply(ctx) {
  const snapshots = new Map(); const tools = new Map(); let closes = 0; let creates = 0; let flushes = 0
  const backend = { kv: { async open(descriptor) {
    const state = snapshots.get(descriptor.name) ?? { tables: {}, global: null }; snapshots.set(descriptor.name, state)
    return { loadAll: async () => structuredClone(state), async putRecord(table, key, value) { state.tables[table] ??= {}; state.tables[table][key] = structuredClone(value) }, async deleteRecord(table, key) { delete state.tables[table]?.[key] }, async deleteRecords(table, keys) { for (const key of keys) delete state.tables[table]?.[key] }, async setGlobal(value) { state.global = structuredClone(value) }, async close() { closes++ } }
  } } }
  ctx.effect(() => ctx.storage.backend.register('io-test', backend))
  ctx.provide(storageBackendServiceKey('io-test'), backend)
  ctx.provide('ioFixture', { snapshots, tools, get closes() { return closes }, get creates() { return creates }, get flushes() { return flushes } })
  ctx.provide('tools', { register(definition) { if (tools.has(definition.name)) throw new Error('duplicate tool'); tools.set(definition.name, definition); return () => tools.delete(definition.name) } })
  ctx.provide('sessionController', { async resolveAgent(id) { const agent = ctx.agents.get(id); return agent ? { agent } : { error: Object.assign(new Error('not found'), { code: 'session/not-found' }) } }, async create() { creates++; return { sessionId: 'new-explicit' } } })
  ctx.provide('sessionPersistence', { async flush() { flushes++ } })
  ctx.provide('sessionQuery', { async observeSession(id) { const session = ctx.sessions.get(id); if (!session) throw new Error('not found'); return { events: session.snapshotEvents(), [Symbol.dispose]() {} } }, async readEvent({ sessionId, seq, before, after }) { return { events: ctx.sessions.get(sessionId).snapshotEvents().slice(Math.max(0, seq - before), seq + after + 1) } } })
}
