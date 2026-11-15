import test from 'node:test'
import assert from 'node:assert/strict'
import { IoCore } from '../core.mjs'
import { MemoryStore } from '../store.mjs'

test('a known pre-admission failure can be retried with the same identity; an uncertain send is not replayed', async () => {
  let failResolve = true, failSend = false
  const messages = [], agent = { id: 's', status: 'idle', inbox: { nextTurn: [] }, followup(message) { if (failSend) throw Error('uncertain'); messages.push(message) } }
  const io = new IoCore({ store: new MemoryStore(), config: { enabled: true }, host: { resolveAgent: async () => { if (failResolve) throw Object.assign(Error('temporary'), { code: 'OFFLINE' }); return agent } } })
  await io.start()
  await io.putBinding({ id: 'b', sessionId: 's', transport: 'qq', endpoint: 'group:1', enabled: true, mode: 'auto' })
  const input = { bindingId: 'b', externalMessageId: '11', content: [{ type: 'text', text: 'original' }] }
  const first = await io.receive(input)
  assert.equal(first.state, 'failed'); assert.equal(messages.length, 0)
  failResolve = false
  assert.equal((await io.receive(input)).state, 'failed')
  const retried = await io.receive({ ...input, retryFailed: true, content: [{ type: 'text', text: 'changed' }] })
  assert.equal(retried.state, 'accepted'); assert.equal(retried.traceId, first.traceId); assert.equal(retried.messageId, first.messageId)
  assert.equal(messages.length, 1); assert.equal(messages[0].content[0].text, 'original')
  assert.equal((await io.receive({ ...input, retryFailed: true })).duplicate, true); assert.equal(messages.length, 1)
  failSend = true
  const uncertain = await io.receive({ ...input, externalMessageId: '12', retryFailed: true })
  assert.equal(uncertain.state, 'unknown')
  failSend = false
  assert.equal((await io.receive({ ...input, externalMessageId: '12', retryFailed: true })).state, 'unknown')
  assert.equal(messages.length, 1)
  await io.close()
})
