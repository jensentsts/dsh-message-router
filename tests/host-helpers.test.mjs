import test from 'node:test'
import assert from 'node:assert/strict'
import { createNativeHost } from '../native-host.mjs'
import { historyTools } from '../tools.mjs'
import { DomainStore } from '../store.mjs'
import { boundedEventPage } from '../history.mjs'
import { normalizeBinding, normalizeIngress } from '../contract.mjs'

const user = (seq, kind = 'user', value = 'visible') => ({ seq, time: seq, type: 'user/message', surfaceOp: 'append', data: { id: `u${seq}`, role: 'user', source: { kind }, content: [{ type: 'text', text: value }] } })
test('cold read lease is released and no Agent is resolved during preview or history', async () => {
  let leases = 0; let resumes = 0; const events = [user(0, 'runtime-context', 'PRIVATE'), user(1)]
  const host = createNativeHost({ sessionController: { resolveAgent() { resumes++ } }, sessionQuery: { observeSession: async () => ({ events, [Symbol.dispose]() { leases++ } }), readEvent: async () => ({ events }) } })
  const page = await host.preview('s', { limit: 20, maxBytes: 4096 }); assert.equal(page.events.length, 1); assert.equal(resumes, 0); assert.equal(leases, 1)
  const detail = await host.readEvent('s', 1, { before: 2, after: 2, maxBytes: 4096 }); assert.equal(detail.targetIncluded, true); assert.equal(detail.events.length, 1); assert.equal(resumes, 0)
})
test('preview lease is released even when the complete bounded result cannot fit', async () => {
  let leases = 0; const host = createNativeHost({ sessionQuery: { observeSession: async () => ({ events: [user(0, 'user', '中'.repeat(3000))], [Symbol.dispose]() { leases++ } }) } })
  await assert.rejects(host.preview('s', { limit: 1, maxBytes: 256 }), { code: 'OUTPUT_TOO_LARGE' }); assert.equal(leases, 1)
})
test('history tools expose no mutation operation and require actual caller Session', async () => {
  const calls = []; const io = { history: async (args, scope) => { calls.push(scope); return { items: [], truncated: false } }, historyRead: async (args, scope) => { calls.push(scope); return { events: [] } } }
  const tools = historyTools(io, definition => definition); assert.deepEqual(tools.map(t => t.name), ['io_history', 'io_history_read'])
  assert.throws(() => tools[0].execute({ bindingId: 'x' }, {}), { code: 'HISTORY_FORBIDDEN' })
  await tools[0].execute({ bindingId: 'x' }, { agent: { id: 'canonical-session' } }); assert.deepEqual(calls[0], { sessionId: 'canonical-session' }); assert.equal(tools[0].parameters.sessionId, undefined)
  assert.equal(tools[0].output.render({}, { items: [], truncated: false })[0].type, 'text')
})
test('DomainStore does not acknowledge before backend write resolves or silently use memory', async () => {
  const records = new Map(); let release; const wait = new Promise(resolve => { release = resolve }); const domain = { table: () => ({ get: k => records.get(k), entries: () => records.entries(), put: async (key, value) => { await wait; records.set(key, value) } }), close: async () => {} }
  const store = new DomainStore(domain); let acknowledged = false; const p = store.put('traces', 'x', { a: 1 }).then(() => { acknowledged = true })
  await Promise.resolve(); assert.equal(acknowledged, false); assert.equal(store.get('traces', 'x'), undefined); release(); await p; assert.deepEqual(store.get('traces', 'x'), { a: 1 })
})
test('strict content refuses malformed image and externally chosen reasoning or byte inputs', () => {
  assert.throws(() => normalizeIngress({ bindingId: 'b', externalMessageId: 'e', content: [{ type: 'image', attachment: { attachmentId: 'x', bytes: 1, mediaType: 'image/jpeg', width: 0, height: 2 } }] }), { code: 'INVALID_CONTENT' })
  assert.throws(() => normalizeIngress({ bindingId: 'b', externalMessageId: 'e', content: [{ type: 'text', text: 'hello', data: 'not-allowed' }] }), { code: 'INVALID_CONTENT' })
  assert.equal(normalizeBinding({ id: 'b', sessionId: 's', transport: 'qq-bridge-001', endpoint: 'group:1' }).sendEnabled, false)
})
test('native send establishes initiating Agent and refuses stale Agent rather than using ambient identity', () => {
  const sent = []; const agent = { id: 's', followup: m => sent.push(m) }; let active = agent; let entered = 0
  const host = createNativeHost({ agents: { get: () => active, withInitiator: (subject, fn) => { assert.equal(subject, agent); entered++; return fn() } } })
  host.send(agent, { id: 'm' }, 'queue'); assert.equal(entered, 1); assert.equal(sent.length, 1); active = undefined; assert.throws(() => host.send(agent, {}, 'queue'), { code: 'STALE_AGENT' })
})
test('model image admission checks capability and backend reference integrity', async () => {
  const agent = { id: 's', model: { current: () => ({ provider: 'p', model: 'text-only' }) } }; const content = [{ type: 'image', attachment: { attachmentId: 'x', mediaType: 'image/png', bytes: 3, width: 1, height: 1 } }]
  let reads = 0; const ctx = { agents: { get: () => agent }, get: key => key === 'attachments' ? { imageLimits: { maxImagesPerMessage: 2, maxMessageImageBytes: 10 }, readImage: async () => { reads++ } } : key === 'llm' ? { resolveModelInfo: async () => ({ inputModalities: ['text'] }) } : undefined }
  await assert.rejects(createNativeHost(ctx).validateAdmission(agent, { content }), { code: 'MODEL_DOES_NOT_SUPPORT_IMAGES' }); assert.equal(reads, 0)
  ctx.get = key => key === 'attachments' ? { imageLimits: { maxImagesPerMessage: 2, maxMessageImageBytes: 10 }, readImage: async () => { reads++; throw Object.assign(new Error(), { code: 'CORRUPT_ATTACHMENT' }) } } : { resolveModelInfo: async () => ({ inputModalities: ['text', 'image'] }) }
  await assert.rejects(createNativeHost(ctx).validateAdmission(agent, { content }), { code: 'CORRUPT_ATTACHMENT' }); assert.equal(reads, 1)
})
test('full UTF-8 result including metadata obeys byte ceiling and marks omission', () => {
  const events = Array.from({ length: 20 }, (_, i) => user(i, 'user', '中🙂'.repeat(20)))
  const result = boundedEventPage('s', events, { maxBytes: 1400, limit: 20 }); assert.equal(result.truncated, true); assert.ok(Buffer.byteLength(JSON.stringify(result)) <= 1400)
})
