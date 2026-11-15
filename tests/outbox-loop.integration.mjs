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
import * as mergePlugin from '../../dsh-steer-merge/index.mjs'
import { QqBridge } from '../../dsh-qq-bridge/lib/bridge.mjs'
import { MemoryQqStore } from '../../dsh-qq-bridge/lib/store.mjs'
import { normalizeConfig } from '../../dsh-qq-bridge/lib/config.mjs'
import { featureTurnFold } from '../../dsh-qq-bridge/lib/native-features.mjs'

function response(text) { return [{ type: 'block-start', index: 0, blockType: 'text' }, { type: 'text-delta', index: 0, text }, { type: 'block-end', index: 0, block: { type: 'text', text } }, { type: 'finish', reason: { kind: 'stop' } }] }
function toolCall(name, id, args, prefix) {
  const json = JSON.stringify(args), index = prefix ? 1 : 0
  return [...(prefix ? response(prefix).slice(0, 3) : []),
    { type: 'block-start', index, blockType: 'tool-call' },
    { type: 'tool-call-delta', index, id, name, argumentsDelta: json },
    { type: 'block-end', index, block: { type: 'tool-call', id, name, arguments: json } },
    { type: 'finish', reason: { kind: 'tool-calls' } }]
}
class ScriptAdapter extends LlmAdapter {
  constructor(script) { super(); this.script = script; this.requests = [] }
  async resolveModel(provider, model) { return { provider, id: model, name: model } }
  async *stream(options) { this.requests.push(options); const next = this.script.shift(); if (!next) throw new Error('Unexpected model request'); for (const chunk of await (typeof next === 'function' ? next(options) : next)) yield chunk }
}
const idle = (ctx, agent) => new Promise(resolve => { const off = ctx.on('agent/status', ({ agent: subject, status }) => { if (subject === agent && status === 'idle') { off(); resolve() } }) })
function toolValues(agent) { return agent.session.snapshotEvents().filter(e => e.type === 'tool/result').map(e => JSON.parse(e.data.message.content[0].text)) }

test('typing clock wakes an idle real AgentLoop and waits for its decision before sending', { timeout: 10000 }, async () => {
  let h, id, jobId, enter, releaseTimer
  const entered = new Promise(resolve => { enter = resolve }), timer = new Promise(resolve => { releaseTimer = resolve })
  const messages = [{ messageId: '11', text: '原来的问题' }]
  const script = [toolCall('qq_buffer', 'clock-append', { action: 'append', text: '我准备好了' }), () => {
    id = toolValues(h.agent).at(-1).id
    return toolCall('qq_buffer', 'clock-preview', { action: 'send', id })
  }, () => {
    const review = toolValues(h.agent).at(-1)
    return toolCall('qq_buffer', 'clock-start', { action: 'send', id, reviewId: review.reviewId })
  }, () => {
    const started = toolValues(h.agent).at(-1)
    assert.equal(started.code, 'QQ_OUTBOX_TYPING_STARTED'); jobId = started.jobId
    return response('我稍后再决定。')
  }, () => {
    assert.ok(h.agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.source?.kind === 'schedule'))
    assert.equal(h.calls.length, 0)
    return toolCall('qq_buffer', 'clock-watch', { action: 'watch', jobId })
  }, () => {
    const clock = toolValues(h.agent).at(-1)
    assert.equal(clock.code, 'QQ_TYPING_CONFIRM')
    assert.equal(clock.timeline.at(-2).text, '后来有人补充了')
    return toolCall('qq_buffer', 'clock-confirm', { action: 'send', id, reviewId: clock.reviewId })
  }, () => {
    assert.equal(toolValues(h.agent).at(-1).receipts[0].state, 'delivered')
    return response('已经决定发送。')
  }]
  h = await harness(script)
  h.bridge.confirmTypingClock = true
  h.bridge.nativeFeatures.history = { read: async () => ({ ok: true, text: JSON.stringify({ messages }) }) }
  h.bridge.typingWait = async () => { enter(); await timer }
  try {
    const first = idle(h.ctx, h.agent)
    h.agent.followup({ id: 'initial-clock-input', role: 'user', source: { kind: 'user', rpcId: 'clock-rpc' }, content: [{ type: 'text', text: '准备发送消息' }] })
    await first; await entered
    assert.equal(h.calls.length, 0)
    messages.push({ messageId: '12', text: '后来有人补充了' })
    const second = idle(h.ctx, h.agent)
    releaseTimer()
    await second; await h.io.drain()
    assert.equal(h.calls.length, 1)
    assert.equal(h.calls[0].params.message[0].data.text, '我准备好了')
  } finally { releaseTimer(); h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('real AgentLoop sees chat changes during typing and can discard a redundant reply', async () => {
  let h, id, enteredResolve, releaseResolve
  const entered = new Promise(resolve => { enteredResolve = resolve }), release = new Promise(resolve => { releaseResolve = resolve })
  const messages = [{ messageId: '11', text: '群友提问' }]
  const script = [toolCall('qq_buffer', 'context-append', { action: 'append', text: '我来回答这个问题' }), () => {
    id = toolValues(h.agent).at(-1).id
    return toolCall('qq_buffer', 'context-preview', { action: 'send', id })
  }, () => {
    const preview = toolValues(h.agent).at(-1)
    assert.equal(preview.code, 'QQ_CONTEXT_REVIEW')
    return toolCall('qq_buffer', 'context-start', { action: 'send', id, reviewId: preview.reviewId, background: true })
  }, async () => {
    const started = toolValues(h.agent).at(-1)
    assert.equal(started.code, 'QQ_OUTBOX_TYPING_STARTED')
    await entered
    messages.push({ messageId: '12', text: '另一位群友已经回答了' })
    releaseResolve()
    return toolCall('qq_buffer', 'context-watch', { action: 'watch', jobId: started.jobId, waitMs: 3000 })
  }, () => {
    const changed = toolValues(h.agent).at(-1)
    assert.equal(changed.code, 'QQ_CONTEXT_CHANGED_DURING_TYPING')
    assert.equal(changed.context.recentMessages.find(row => row.messageId === changed.context.changedMessageIds[0]).text, '另一位群友已经回答了')
    return toolCall('qq_buffer', 'context-clear', { action: 'clear', id })
  }, () => {
    assert.equal(toolValues(h.agent).at(-1).code, 'QQ_OUTBOX_CLEARED')
    return response('已经有人回答，我先观察。')
  }]
  h = await harness(script)
  h.bridge.nativeFeatures.history = { read: async () => ({ ok: true, text: JSON.stringify({ messages }) }) }
  h.bridge.typingWait = async () => { enteredResolve(); await release }
  try {
    const done = idle(h.ctx, h.agent)
    h.agent.followup({ id: 'local-input', role: 'user', source: { kind: 'user', rpcId: 'local-rpc' }, content: [{ type: 'text', text: '准备在群里回答' }] })
    await done; await h.io.drain()
    assert.equal(h.calls.length, 0)
  } finally { releaseResolve(); h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('real AgentLoop observes and cancels its background typing, then sends its revision', async () => {
  let h, id, enteredResolve
  const entered = new Promise(resolve => { enteredResolve = resolve })
  const script = [toolCall('qq_buffer', 'typing-append', { action: 'append', text: '正在准备发送' }), () => {
    id = toolValues(h.agent).at(-1).id
    return toolCall('qq_buffer', 'typing-start', { action: 'send', id, background: true })
  }, () => {
    const review = toolValues(h.agent).at(-1)
    assert.equal(review.code, 'QQ_CONTEXT_REVIEW')
    return toolCall('qq_buffer', 'typing-confirm', { action: 'send', id, reviewId: review.reviewId, background: true })
  }, async () => {
    assert.equal(toolValues(h.agent).at(-1).code, 'QQ_OUTBOX_TYPING_STARTED')
    await entered
    return toolCall('qq_buffer', 'typing-read', { action: 'read', id })
  }, () => {
    const read = toolValues(h.agent).at(-1)
    assert.equal(read.activity.state, 'typing'); assert.equal(read.activity.id, id)
    assert.ok(read.activity.remainingMs >= 0)
    return toolCall('qq_buffer', 'typing-cancel', { action: 'cancel_typing', id })
  }, async () => {
    assert.equal(toolValues(h.agent).at(-1).code, 'QQ_TYPING_CANCEL_REQUESTED')
    await h.bridge.outbox.sendTails.get(h.binding.id)
    return toolCall('qq_buffer', 'typing-replace', { action: 'replace', id, text: '修改后发送', typing: { speed: 'instant' } })
  }, () => toolCall('qq_buffer', 'typing-review-revision', { action: 'send', id, finish: true }), () => {
    const review = toolValues(h.agent).at(-1)
    assert.equal(review.code, 'QQ_CONTEXT_REVIEW')
    return toolCall('qq_buffer', 'typing-send', { action: 'send', id, finish: true, reviewId: review.reviewId })
  }]
  h = await harness(script)
  h.bridge.nativeFeatures.history = { read: async () => ({ ok: true, text: JSON.stringify({ messages: [] }) }) }
  h.bridge.typingWait = async (_ms, signal) => {
    enteredResolve()
    await new Promise((resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }))
  }
  try {
    const done = idle(h.ctx, h.agent)
    h.agent.followup({ id: 'local-input', role: 'user', source: { kind: 'user', rpcId: 'local-rpc' }, content: [{ type: 'text', text: '发消息' }] })
    await done; await h.io.drain()
    assert.equal(toolValues(h.agent).at(-1).receipts[0].state, 'delivered')
    assert.equal(h.calls.length, 1)
    assert.equal(h.calls[0].params.message[0].data.text, '修改后发送')
  } finally { h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('qq_buffer works when the bound conversation is started from DSH itself', async () => {
  let h
  const script = [toolCall('qq_buffer', 'local-append', { action: 'append', text: '测试消息' }), () => {
    const draft = toolValues(h.agent).at(-1)
    assert.equal(draft.code, 'QQ_OUTBOX_APPENDED')
    return toolCall('qq_buffer', 'local-send', { action: 'send', id: draft.id, finish: true })
  }]
  h = await harness(script)
  try {
    const done = idle(h.ctx, h.agent)
    h.agent.followup({ id: 'local-input', role: 'user', source: { kind: 'user', rpcId: 'local-rpc' }, content: [{ type: 'text', text: '发个消息' }] })
    await done; await h.io.drain()
    assert.equal(toolValues(h.agent).at(-1).receipts[0].state, 'delivered')
    assert.equal(h.calls.length, 1); assert.equal(h.calls[0].params.message.find(part => part.type === 'text').data.text, '测试消息')
  } finally { h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

async function harness(script) {
  const ctx = new Context(), rows = new Map()
  await ctx.plugin(Storage)
  const backend = { kv: { async open() { return { loadAll: async () => ({ tables: Object.fromEntries(rows), global: null }), async putRecord(table, key, value) { if (!rows.has(table)) rows.set(table, {}); rows.get(table)[key] = structuredClone(value) }, async deleteRecord() {}, async deleteRecords() {}, async close() {} } } } }
  ctx.storage.backend.register('outbox-loop', backend); ctx.provide('storage.backend.outbox-loop', backend); await ctx.plugin(Domain, { backend: 'outbox-loop' })
  for (const plugin of [LlmRuntime, SessionStore, ProjectionRegistry, SystemPrompt, ToolRuntime, NativeExecutor, AgentRegistry]) await ctx.plugin(plugin)
  await ctx.plugin(AgentLoop, { agents: [] })
  const adapter = new ScriptAdapter(script); ctx.llm.registerAdapter(['mock'], adapter)
  ctx.provide('sessionController', { async list() { return { items: [{ sessionId: 'outbox-loop-session' }] } }, async resolveAgent(id) { const agent = ctx.agents.get(id); if (!agent) throw new Error('not found'); return { agent } } })
  ctx.provide('sessionQuery', { async observeSession(id) { return { events: ctx.sessions.get(id).snapshotEvents(), [Symbol.dispose]() {} } } })
  await ctx.plugin(ioPlugin, { enabled: true, sendEnabled: true, continuationEnabled: false, maxConsecutiveSteers: 1 })
  await ctx.plugin(mergePlugin, { enabled: true, windowMs: 100 })
  const agent = await ctx.agentLoop.create('outbox-loop-session', { provider: 'mock', model: 'test' })
  const other = await ctx.agentLoop.create('unbound-session', { provider: 'mock', model: 'test' })
  ctx.provide('sessionPersistence', { flush: async () => {} })
  const io = ctx.get('superAgentIo'), calls = []
  assert.equal(typeof io.flushOutbound, 'function'); assert.equal(typeof io.updateBuffered, 'function')
  const config = normalizeConfig({ enabled: true, sendEnabled: true, bindings: [{ endpoint: 'group:456', sessionId: agent.id, enabled: true, sendEnabled: true }] })
  const binding = config.bindings[0], projection = { stateOf: session => session.snapshotEvents().reduce(featureTurnFold.apply, featureTurnFold.init()) }
  const nativeFeatures = { projection, reconcile: async () => {}, prepare: async () => {}, dispose: async () => {}, allowDelivery: async () => true }
  const bridge = new QqBridge({ io, config, store: new MemoryQqStore(config), nativeFeatures, confirmTypingClock: false, onebot: { async call(action, params) { calls.push({ action, params }); return { message_id: calls.length } } } })
  await bridge.start()
  const registration = bridge.outbox.register(binding, agent, agent.ctx.get('tools'), () => true)
  assert.equal(registration.state, 'applied')
  const stopGlobalBuffer = io.registerChannelTool(bridge.outbox.definition({ name: 'qq_buffer' }))
  const stopGlobalOutbox = io.registerChannelTool(bridge.outbox.definition({ name: 'qq_outbox' }))
  const stopOwner = () => { stopGlobalOutbox(); stopGlobalBuffer() }
  assert.ok(ctx.tools.get('qq_buffer', agent)); assert.ok(ctx.tools.get('qq_outbox', agent))
  return { ctx, agent, other, adapter, io, calls, bridge, binding, registration, stopOwner }
}

test('real AgentLoop stages text before an immediately following tool, sends only by action, and stages its final text', async () => {
  let h
  const script = [toolCall('qq_buffer', 'read-1', { action: 'read' }, '第一条回复'), () => {
    const read = toolValues(h.agent).at(-1)
    assert.equal(h.calls.length, 0)
    assert.equal(read.code, 'QQ_OUTBOX_READ'); assert.equal(read.items.length, 1); assert.equal(read.items[0].content[0].text, '第一条回复')
    return toolCall('qq_buffer', 'send-1', { action: 'send', id: read.items[0].id })
  }, response('最后的说明')]
  h = await harness(script)
  try {
    const done = idle(h.ctx, h.agent)
    await h.io.receive({ bindingId: h.binding.id, externalMessageId: '123', content: [{ type: 'text', text: 'start' }] })
    await done; await h.io.drain()
    const values = toolValues(h.agent); assert.equal(values[1].ok, true); assert.equal(values[1].receipts[0].state, 'delivered')
    assert.equal(h.calls.length, 1); assert.equal(h.calls[0].params.message.find(p => p.type === 'text').data.text, '第一条回复')
    const box = await h.bridge.store.outbox(h.binding.id)
    assert.deepEqual(box.entries.map(row => [row.state, row.delivery.reply.content[0].text]), [['sent', '第一条回复'], ['draft', '最后的说明']])
    const history = await h.io.history({ bindingId: h.binding.id, kind: 'delivery', limit: 20 }, { sessionId: h.agent.id })
    assert.deepEqual(history.items.map(row => row.state).sort(), ['buffered', 'delivered'])
  } finally { h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('owner io_send reports buffered until the owner explicitly presses the QQ outbox send button', async () => {
  let h
  const script = [() => toolCall('io_send', 'send-io', { destination: h.binding.id, text: '主动发送' }), () => {
    const value = toolValues(h.agent).at(-1)
    assert.equal(value.state, 'buffered'); assert.equal(h.calls.length, 0)
    return toolCall('qq_outbox', 'send-qq', { action: 'send', destination: h.binding.id, id: value.bufferId })
  }, response('已完成')]
  h = await harness(script)
  try {
    const done = idle(h.ctx, h.agent)
    h.agent.followup({ id: 'owner-input', role: 'user', source: { kind: 'user', rpcId: 'owner-rpc' }, content: [{ type: 'text', text: '给群发消息' }] })
    await done; await h.io.drain()
    const values = toolValues(h.agent); assert.equal(values[1].receipts[0].state, 'delivered'); assert.equal(h.calls.length, 1)
    assert.equal(h.calls[0].params.message.find(p => p.type === 'text').data.text, '主动发送')
  } finally { h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('finish=true concludes an actual AgentLoop only after the QQ send succeeds', async () => {
  let h
  const script = [toolCall('qq_buffer', 'read-final', { action: 'read' }, '发出后结束'), () => {
    const read = toolValues(h.agent).at(-1)
    return toolCall('qq_buffer', 'send-final', { action: 'send', id: read.items[0].id, finish: true })
  }]
  h = await harness(script)
  try {
    const done = idle(h.ctx, h.agent)
    await h.io.receive({ bindingId: h.binding.id, externalMessageId: '124', content: [{ type: 'text', text: 'start' }] })
    await done; await h.io.drain()
    assert.equal(h.adapter.requests.length, 2); assert.equal(h.calls.length, 1)
    const box = await h.bridge.store.outbox(h.binding.id); assert.deepEqual(box.entries.map(row => row.state), ['sent'])
  } finally { h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('merged QQ steers reach one model step while its reply stays buffered until selected send', async () => {
  let h, release, started
  const blocker = new Promise(resolve => { release = resolve })
  const began = new Promise(resolve => { started = resolve })
  const script = [async () => { started(); await blocker; return response('工作中') }, request => {
    const texts = request.messages.flatMap(message => message.content ?? []).filter(block => block.type === 'text').map(block => block.text)
    for (const text of ['插话甲', '插话乙', '插话丙']) assert.ok(texts.includes(text), text)
    return toolCall('qq_buffer', 'read-merged', { action: 'read' }, '收到三条')
  }, () => {
    assert.equal(h.calls.length, 0)
    const read = toolValues(h.agent).at(-1)
    const item = read.items.find(row => row.content.some(block => block.text === '收到三条'))
    assert.ok(item)
    return toolCall('qq_buffer', 'send-merged', { action: 'send', id: item.id, finish: true })
  }]
  h = await harness(script)
  try {
    const done = idle(h.ctx, h.agent)
    const receive = (id, text) => h.io.receive({ bindingId: h.binding.id, externalMessageId: id, mode: 'steer', content: [{ type: 'text', text }] })
    await receive('201', '开始')
    await began
    const receipts = await Promise.all([receive('202', '插话甲'), receive('203', '插话乙'), receive('204', '插话丙')])
    assert.deepEqual(receipts.map(value => value.mode), ['steer', 'queue', 'queue'])
    release()
    await done; await h.io.drain()
    assert.equal(h.adapter.requests.length, 3)
    assert.equal(h.calls.length, 1)
    assert.equal(h.calls[0].params.message.find(part => part.type === 'text').data.text, '收到三条')
    const box = await h.bridge.store.outbox(h.binding.id)
    assert.deepEqual(box.entries.map(row => row.state), ['draft', 'sent'])
  } finally { release(); h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('a late QQ message reaches the real AgentLoop before one consolidated group reply', async () => {
  let h, draftId, delivered
  const script = [toolCall('qq_buffer', 'late-append', { action: 'append', text: '旧草稿' }), () => {
    draftId = toolValues(h.agent).at(-1).id
    setTimeout(() => { delivered = h.io.receive({ bindingId: h.binding.id, externalMessageId: 'late-2', mode: 'steer', content: [{ type: 'text', text: '迟到的补充' }] }) }, 40)
    return toolCall('qq_buffer', 'late-send-1', { action: 'send', id: draftId })
  }, request => {
    assert.equal(toolValues(h.agent).at(-1).code, 'QQ_INPUT_PENDING')
    assert.equal(h.calls.length, 0)
    const texts = request.messages.flatMap(message => message.content ?? []).filter(block => block.type === 'text').map(block => block.text)
    assert.ok(texts.some(text => text.includes('迟到的补充')))
    return toolCall('qq_buffer', 'late-replace', { action: 'replace', id: draftId, text: '一起回复两条消息' })
  }, () => toolCall('qq_buffer', 'late-send-2', { action: 'send', id: draftId, finish: true })]
  h = await harness(script)
  h.bridge.config.replyQuietMs = 300
  try {
    const done = idle(h.ctx, h.agent)
    await h.io.receive({ bindingId: h.binding.id, externalMessageId: 'late-1', content: [{ type: 'text', text: '第一条消息' }] })
    await done; await delivered; await h.io.drain()
    assert.equal(h.calls.length, 1)
    assert.equal(h.calls[0].params.message.find(part => part.type === 'text').data.text, '一起回复两条消息')
  } finally { h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('the real AgentLoop can choose to send before later QQ input and then processes that input', async () => {
  let h, draftId
  const script = [toolCall('qq_buffer', 'choose-append', { action: 'append', text: '先回应前面的话' }), async () => {
    draftId = toolValues(h.agent).at(-1).id
    await h.io.receive({ bindingId: h.binding.id, externalMessageId: 'choose-2', mode: 'steer', content: [{ type: 'text', text: '随后来的补充' }] })
    return toolCall('qq_buffer', 'choose-send', { action: 'send', id: draftId, sendNow: true, finish: true })
  }, request => {
    assert.equal(h.calls.length, 1)
    const texts = request.messages.flatMap(message => message.content ?? []).filter(block => block.type === 'text').map(block => block.text)
    assert.ok(texts.some(text => text.includes('随后来的补充')))
    return response('已处理后续消息')
  }]
  h = await harness(script)
  try {
    const done = idle(h.ctx, h.agent)
    await h.io.receive({ bindingId: h.binding.id, externalMessageId: 'choose-1', content: [{ type: 'text', text: '最初的问题' }] })
    await done; await h.io.drain()
    assert.equal(h.calls.length, 1)
    assert.equal(h.calls[0].params.message.find(part => part.type === 'text').data.text, '先回应前面的话')
    const received = h.agent.session.snapshotEvents().filter(event => event.type === 'user/message').map(event => event.data.source?.externalMessageId)
    assert.ok(received.includes('choose-2'))
  } finally { h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('the real AgentLoop sees the current chat and pending draft before deciding whether to send', async () => {
  let h, draftId
  const script = [toolCall('qq_buffer', 'review-append', { action: 'append', text: '让我翻一下记录' }), () => {
    draftId = toolValues(h.agent).at(-1).id
    return toolCall('qq_buffer', 'review-send', { action: 'send', id: draftId, finish: true })
  }, () => {
    const review = toolValues(h.agent).at(-1)
    assert.equal(review.code, 'QQ_CONTEXT_REVIEW'); assert.equal(review.finishRequested, true); assert.equal(review.agentStatus, 'running')
    assert.equal(review.timeline[0].text, '记录里已经有结果了')
    assert.equal(review.timeline.at(-1).content[0].text, '让我翻一下记录')
    assert.equal(h.calls.length, 0)
    return toolCall('qq_buffer', 'review-replace', { action: 'replace', id: draftId, text: '我查到了，结果在这里。' })
  }, () => toolCall('qq_buffer', 'review-again', { action: 'send', id: draftId, finish: true }), () => {
    const review = toolValues(h.agent).at(-1)
    assert.equal(review.code, 'QQ_CONTEXT_REVIEW'); assert.equal(review.timeline.at(-1).content[0].text, '我查到了，结果在这里。')
    return toolCall('qq_buffer', 'review-confirm', { action: 'send', id: draftId, finish: true, reviewId: review.reviewId })
  }]
  h = await harness(script)
  h.bridge.nativeFeatures.history = { async read(_args, allowed) {
    assert.ok(allowed())
    return { ok: true, text: JSON.stringify({ messages: [{ messageId: '3', senderName: '群友', text: '记录里已经有结果了', time: 10 }] }) }
  } }
  h.bridge.config.replyQuietMs = 0
  try {
    const done = idle(h.ctx, h.agent)
    await h.io.receive({ bindingId: h.binding.id, externalMessageId: 'review-1', content: [{ type: 'text', text: '请查看记录' }] })
    await done; await h.io.drain()
    assert.equal(h.calls.length, 1)
    assert.equal(h.calls[0].params.message.find(part => part.type === 'text').data.text, '我查到了，结果在这里。')
  } finally { h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('QQ send stays available when a steer reaches the inbox before the first input is committed', async () => {
  let h, inserted = false
  const script = [toolCall('qq_buffer', 'append-early', { action: 'append', text: '收到了' }), () => {
    const draft = toolValues(h.agent).at(-1)
    assert.equal(draft.code, 'QQ_OUTBOX_APPENDED')
    return toolCall('qq_buffer', 'send-early', { action: 'send', id: draft.id, finish: true })
  }]
  h = await harness(script)
  const off = h.ctx.on('agent/inbox/claimed', ({ agent, message }) => {
    if (agent !== h.agent || inserted || message.source?.externalMessageId !== '601') return
    inserted = true
    agent.steer({ id: 'early-steer', role: 'user', source: { ...message.source, inputMode: 'steer', externalMessageId: '602' }, content: [{ type: 'text', text: '紧随其后的插话' }] })
  })
  try {
    const done = idle(h.ctx, h.agent)
    await h.io.receive({ bindingId: h.binding.id, externalMessageId: '601', content: [{ type: 'text', text: '第一条消息' }] })
    await done; await h.io.drain()
    const events = h.agent.session.snapshotEvents()
    const before = events.find(event => event.type === 'agent/inbox/spliced' && event.data.inserted?.some(message => message.id === 'early-steer'))
    const first = events.find(event => event.type === 'user/message' && event.data.source?.externalMessageId === '601')
    assert.ok(before.seq < first.seq)
    assert.equal(h.calls.length, 1)
    assert.equal(h.calls[0].params.message.find(part => part.type === 'text').data.text, '收到了')
    assert.equal(toolValues(h.agent).at(-1).receipts[0].state, 'delivered')
  } finally { off(); h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

for (const tool of ['qq_buffer', 'qq_outbox']) test(`an unbound Agent with external input can use ${tool} to send to an unconfigured private QQ`, async () => {
  let h, other
  const script = [toolCall(tool, 'outside-append', { action: 'append', destination: 'private:789', text: '主动私聊' }), () => {
    const draft = toolValues(other).at(-1)
    assert.equal(draft.code, 'QQ_OUTBOX_APPENDED'); assert.equal(h.calls.length, 0)
    return toolCall(tool, 'outside-send', { action: 'send', destination: 'private:789', id: draft.id, finish: true })
  }]
  h = await harness(script)
  try {
    other = h.other
    assert.ok(h.ctx.tools.get('qq_buffer', other)); assert.ok(h.ctx.tools.get('qq_outbox', other))
    const done = idle(h.ctx, other)
    other.followup({ id: 'external-input', role: 'user', source: { kind: 'super-agent-io', version: 1, transport: 'remote', bindingId: 'other', endpoint: 'remote:1', traceId: 't', externalMessageId: '1', inputMode: 'queue' }, content: [{ type: 'text', text: '去 QQ 打个招呼' }] })
    await done; await h.io.drain()
    assert.equal(toolValues(other).at(-1).receipts[0].state, 'delivered')
    assert.equal(h.calls.length, 1); assert.equal(h.calls[0].action, 'send_private_msg'); assert.equal(String(h.calls[0].params.user_id), '789')
  } finally { h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('a QQ-triggered conversation can use the global outbox to send to a different QQ chat', async () => {
  let h
  const script = [toolCall('qq_outbox', 'cross-append', { action: 'append', destination: 'group:999', text: '跨群消息' }), () => {
    const draft = toolValues(h.agent).at(-1)
    assert.equal(draft.code, 'QQ_OUTBOX_APPENDED')
    return toolCall('qq_outbox', 'cross-send', { action: 'send', destination: 'group:999', id: draft.id, finish: true })
  }]
  h = await harness(script)
  try {
    const done = idle(h.ctx, h.agent)
    await h.io.receive({ bindingId: h.binding.id, externalMessageId: '701', content: [{ type: 'text', text: '开始' }] })
    await done; await h.io.drain()
    assert.equal(toolValues(h.agent).at(-1).receipts[0].state, 'delivered')
    assert.equal(h.calls.length, 1); assert.equal(String(h.calls[0].params.group_id), '999')
  } finally { h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

test('QQ sending continues when a DSH user interrupts the current QQ turn', async () => {
  let h, release, started
  const blocker = new Promise(resolve => { release = resolve })
  const began = new Promise(resolve => { started = resolve })
  const script = [async () => { started(); await blocker; return toolCall('qq_buffer', 'mixed-append', { action: 'append', text: '插话后继续发送' }) }, () => {
    const draft = toolValues(h.agent).at(-1)
    assert.equal(draft.code, 'QQ_OUTBOX_APPENDED')
    return toolCall('qq_buffer', 'mixed-send', { action: 'send', id: draft.id, finish: true })
  }]
  h = await harness(script)
  try {
    const done = idle(h.ctx, h.agent)
    await h.io.receive({ bindingId: h.binding.id, externalMessageId: '801', content: [{ type: 'text', text: 'QQ 消息' }] })
    await began
    h.agent.steer({ id: 'local-steer', role: 'user', source: { kind: 'user', rpcId: 'local-steer-rpc' }, content: [{ type: 'text', text: 'DSH 插话' }] })
    release(); await done; await h.io.drain()
    assert.equal(toolValues(h.agent).at(-1).receipts[0].state, 'delivered')
    assert.equal(h.calls.length, 1); assert.equal(h.calls[0].params.message.find(part => part.type === 'text').data.text, '插话后继续发送')
  } finally { release(); h.registration.dispose(); h.stopOwner(); await h.bridge.dispose(); await h.ctx.fiber.dispose() }
})

