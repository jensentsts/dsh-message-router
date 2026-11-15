import test from 'node:test'
import assert from 'node:assert/strict'
import { MemoryStore, DomainStore } from '../store.mjs'
import { IoCore } from '../core.mjs'
import { queryMonitor, MAX_MONITOR_BYTES, monitorBytes } from '../monitor.mjs'

for (const kind of ['memory', 'domain']) test(`${kind} store detaches writes, reads, entries and test snapshots`, async () => {
  let store
  if (kind === 'memory') store = new MemoryStore()
  else {
    const tables = new Map([['traces', new Map()]])
    store = new DomainStore({ table(name) { const table = tables.get(name); return { get: key => table.get(key), entries: () => table.entries(), async put(key, value) { table.set(key, value) } } }, async close() {} })
  }
  const input = { id: 'trace', content: [{ text: 'original' }], nested: { target: 'qq:123' } }
  await store.put('traces', 'trace', input)
  input.content[0].text = 'caller mutation'; input.nested.target = 'qq:other'
  assert.equal(store.get('traces', 'trace').content[0].text, 'original'); assert.equal(store.get('traces', 'trace').nested.target, 'qq:123')
  const read = store.get('traces', 'trace'); read.content[0].text = 'read mutation'
  const entries = store.entries('traces'); entries[0][1].nested.target = 'entry mutation'
  assert.equal(store.get('traces', 'trace').content[0].text, 'original'); assert.equal(store.get('traces', 'trace').nested.target, 'qq:123')
  if (kind === 'memory') {
    const snapshot = store.snapshot(); snapshot.traces.trace.content[0].text = 'snapshot mutation'
    assert.equal(store.get('traces', 'trace').content[0].text, 'original')
    const seeded = new MemoryStore(snapshot); snapshot.traces.trace.content[0].text = 'seed mutation'
    assert.equal(seeded.get('traces', 'trace').content[0].text, 'snapshot mutation')
  }
})
test('owner binding projection includes full Unicode label/IDs in byte budget and returns typed overflow', async () => {
  let nativeCalls = 0
  const io = new IoCore({ store: new MemoryStore(), config: {}, host: new Proxy({}, { get() { nativeCalls++; throw new Error('No native or credential access during monitor reads') } }) })
  const label = '研究🙂'.repeat(40); const endpoint = `group:${'界'.repeat(500)}`; const sessionId = '会'.repeat(500)
  for (let i = 0; i < 40; i++) await io.putBinding({ id: `b-${String(i).padStart(3, '0')}`, label, endpoint, sessionId, transport: 'qq', accessToken: 'SHOULD_NOT_PERSIST', credentials: { secret: 'SHOULD_NOT_PERSIST' } })
  const first = await queryMonitor(io, { action: 'bindings', limit: 1 })
  assert.equal(first.ok, true); assert.equal(first.value.items[0].label, label); assert.equal(first.value.items[0].endpoint, endpoint); assert.equal(first.value.items[0].sessionId, sessionId)
  assert.equal(first.value.items[0].accessToken, undefined); assert.equal(first.value.items[0].credentials, undefined); assert.equal(JSON.stringify(first).includes('SHOULD_NOT_PERSIST'), false)
  first.value.items[0].endpoint = 'mutated'; assert.equal(io.listBindings()[0].endpoint, endpoint)
  const oversized = await queryMonitor(io, { action: 'bindings', limit: 40 })
  assert.deepEqual(oversized, { ok: false, error: { code: 'RESULT_TOO_LARGE', message: 'The complete monitor result exceeds 64 KiB. Reduce the page size.' } }); assert.ok(monitorBytes(oversized) <= MAX_MONITOR_BYTES); assert.equal(nativeCalls, 0)
})
