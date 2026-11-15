import test from 'node:test'
import assert from 'node:assert/strict'
import { ReplyContinuation } from '../continuation.mjs'
import { initialCursor, foldRouting } from '../routing.mjs'
import { stopToken, isExactStop } from '../contract.mjs'
const text = value => [{ type: 'text', text: value }]
function fixture(policy = {}) {
  const sent = []; const prepared = []; const audit = []; let now = 100; let nonce = 0
  const binding = { id: 'b', sessionId: 's', enabled: true, continuation: { enabled: true, maxAdditionalReplies: 2, maxOutputTokens: 20, maxDurationMs: 1000, ...policy } }
  const controller = new ReplyContinuation({ binding: () => binding, enabled: () => true, prepare: async (id, proposal) => prepared.push(proposal), send: (agent, message) => sent.push(message), audit: async (...args) => audit.push(args), now: () => now, uuid: () => `nonce-${++nonce}` })
  const agent = { id: 's', inbox: { nextStep: [], nextTurn: [] } }; const abort = new AbortController(); let seq = 0
  const event = (type, data, surfaceOp) => ({ seq: seq++, time: now, type, data, ...(surfaceOp ? { surfaceOp } : {}) })
  const observe = (type, data, surfaceOp) => { const e = event(type, data, surfaceOp); controller.observe('s', e); return e }
  observe('turn/start', { turn: 1 }); observe('user/message', { id: 'input', role: 'user', source: { kind: 'super-agent-io', bindingId: 'b', version: 1 }, content: text('question') }, 'append')
  const reply = (value = 'one', usage = 2, extra = {}) => observe('assistant/message', { turn: 1, step: 1, stream: [], usage: { outputTokens: usage }, message: { id: `a-${seq}`, role: 'assistant', source: { kind: 'model', provider: 'p', model: 'm' }, content: Array.isArray(value) ? value : text(value) }, ...extra }, 'append')
  const stop = () => controller.stop({ agent, turn: 1, signal: abort.signal })
  return { controller, binding, agent, abort, sent, prepared, audit, observe, reply, stop, advance: value => { now += value } }
}
test('each admitted follow-up is a real logged producer message and not a sentence split', async () => {
  const f = fixture(); f.reply('original. Still one message.'); await f.stop(); assert.equal(f.sent.length, 1); assert.equal(f.prepared.length, 1)
  const message = f.sent[0]; assert.equal(message.source.kind, 'super-agent-io-control'); assert.equal(message.role, 'user'); assert.ok(message.content[0].text.includes(stopToken(message.source.nonce)))
  f.observe('user/message', message, 'append'); f.reply('new reply'); await f.stop(); assert.equal(f.sent.length, 2)
  f.observe('user/message', f.sent[1], 'append'); f.reply('third reply'); await f.stop(); assert.equal(f.sent.length, 2)
})
test('exact nonce-only output stops without requiring an additional public reply', async () => {
  const f = fixture(); f.reply(); await f.stop(); f.observe('user/message', f.sent[0], 'append'); f.reply(stopToken(f.sent[0].source.nonce)); await f.stop(); assert.equal(f.sent.length, 1)
  const nonce = f.sent[0].source.nonce; assert.equal(isExactStop({ content: text(`quote ${stopToken(nonce)}`) }, nonce), false); assert.equal(isExactStop({ content: text(`${stopToken(nonce)}\n`) }, nonce), false); assert.equal(isExactStop({ content: [{ type: 'text', text: stopToken(nonce) }, { type: 'text', text: '' }] }, nonce), false)
})
test('tool, media, manual input, unrelated source, pending work, cancellation and missing usage prevent new requests', async () => {
  for (const condition of ['tool', 'media', 'local', 'external', 'pending', 'cancel', 'usage', 'max-tokens']) {
    const f = fixture(); f.reply('reply', condition === 'usage' ? undefined : 2)
    if (condition === 'usage') f.controller.states.get('s').usageKnown = false
    if (condition === 'tool') f.observe('tool/call', { turn: 1, step: 1 })
    if (condition === 'media') f.reply([{ type: 'image', attachment: {} }])
    if (condition === 'local') f.observe('user/message', { source: { kind: 'user' } })
    if (condition === 'external') f.observe('agent/inbox/spliced', { inserted: [{ source: { kind: 'super-agent-io', bindingId: 'another' } }] })
    if (condition === 'pending') f.agent.inbox.nextTurn.push({})
    if (condition === 'cancel') f.abort.abort()
    if (condition === 'max-tokens') f.reply('limited', 2, { stream: [{ type: 'chunk', chunk: { type: 'finish', reason: { kind: 'max-tokens' } } }] })
    await f.stop(); assert.equal(f.sent.length, 0, condition)
  }
})
test('output usage and elapsed time are admission budgets, with no global tool restriction', async () => {
  const f = fixture({ maxOutputTokens: 2 }); f.reply('reply', 2); await f.stop(); assert.equal(f.sent.length, 0)
  const g = fixture(); g.reply(); g.advance(1001); await g.stop(); assert.equal(g.sent.length, 0)
  const h = fixture(); h.binding.continuation.enabled = false; h.reply(); await h.stop(); assert.equal(h.sent.length, 0)
})
test('racing external input or cancellation during durable preparation prevents late steering', async () => {
  const f = fixture(); f.reply(); const original = f.controller.prepare; f.controller.prepare = async (...args) => { await original(...args); f.abort.abort() }; await f.stop(); assert.equal(f.prepared.length, 1); assert.equal(f.sent.length, 0)
})
test('routing excludes only the exact control response while native history remains untouched', () => {
  let state = initialCursor('s'); let seq = 0
  const fold = (type, data, surfaceOp) => { const result = foldRouting(state, { type, data, seq: seq++, time: 1, ...(surfaceOp ? { surfaceOp } : {}) }); state = result.cursor; return result }
  fold('turn/start', { turn: 1 }); fold('user/message', { source: { kind: 'super-agent-io', version: 1, bindingId: 'b', traceId: 't' } }, 'append'); fold('user/message', { source: { kind: 'super-agent-io-control', version: 1, bindingId: 'b', nonce: 'random', turn: 1 } }, 'append')
  const make = value => ({ turn: 1, step: 2, message: { id: 'a', role: 'assistant', source: { kind: 'model', provider: 'p', model: 'm' }, content: text(value) } })
  const exact = fold('assistant/message', make(stopToken('random')), 'append'); assert.equal(exact.controlOnly, true); assert.equal(exact.reply, null)
  const quoted = fold('assistant/message', make(`Example ${stopToken('random')}`), 'append'); assert.equal(quoted.controlOnly, false); assert.equal(quoted.reply.content[0].text, `Example ${stopToken('random')}`)
})
