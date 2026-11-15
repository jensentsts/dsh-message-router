import test from 'node:test'
import assert from 'node:assert/strict'
import { MemoryStore } from '../store.mjs'
import { IoCore } from '../core.mjs'
import { parseMonitorRequest, queryMonitor, monitorBound, MAX_MONITOR_BYTES } from '../monitor.mjs'

test('monitor admission whitelists only bounded read queries', () => {
  for (const value of [{ action: 'receive' }, { action: 'bindings', enabled: true }, { action: 'receipt', traceId: 'x', cancel: true }, { action: 'history', bindingId: 'b', limit: 101 }]) assert.throws(() => parseMonitorRequest(value), { code: 'INVALID_INPUT' })
  assert.deepEqual(parseMonitorRequest({ action: 'history', bindingId: 'b', kind: 'audit', limit: 2 }), { action: 'history', bindingId: 'b', kind: 'audit', limit: 2 })
})
test('monitor binding and history reads never drain, activate, admit or send work', async () => {
  const io = new IoCore({ store: new MemoryStore(), config: { enabled: false, sendEnabled: false }, host: new Proxy({}, { get() { throw new Error('No native operation is allowed while monitoring') } }) })
  await io.putBinding({ id: 'b', sessionId: 's', transport: 'qq', endpoint: 'group:123' }); await io.putBinding({ id: 'a', sessionId: 's2', transport: 'remote', endpoint: 'peer:s2' })
  const page = await queryMonitor(io, { action: 'bindings', limit: 1 }); assert.equal(page.ok, true); assert.equal(page.value.items[0].id, 'a'); assert.equal(page.value.gates.enabled, false); assert.equal(page.value.truncated, true)
  const next = await queryMonitor(io, { action: 'bindings', cursor: page.value.nextCursor, limit: 1 }); assert.equal(next.value.items[0].id, 'b')
  const history = await queryMonitor(io, { action: 'history', bindingId: 'b', kind: 'audit' }); assert.equal(history.ok, true); assert.equal(history.value.items[0].kind, 'binding')
  const receipt = await queryMonitor(io, { action: 'receipt', traceId: 'missing' }); assert.equal(receipt.value.receipt, null)
})
test('complete oversized wrapper is a typed failure, not silently truncated JSON', () => {
  const result = monitorBound({ ok: true, value: { labels: '中'.repeat(MAX_MONITOR_BYTES) } }); assert.equal(result.ok, false); assert.equal(result.error.code, 'RESULT_TOO_LARGE'); assert.ok(Buffer.byteLength(JSON.stringify(result)) < MAX_MONITOR_BYTES)
})
