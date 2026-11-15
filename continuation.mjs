import { randomUUID } from 'node:crypto'
import { SOURCE_KIND, CONTROL_SOURCE_KIND, isExactStop, stopToken } from './contract.mjs'
import { isNativeContext } from './routing.mjs'

/** Tool-free continuation admission controller. It never restricts tools, rewrites model output, or fabricates assistant events. */
export class ReplyContinuation {
  constructor({ binding, enabled, prepare, send, audit, now = Date.now, uuid = randomUUID }) {
    this.binding = binding; this.enabled = enabled; this.prepare = prepare; this.send = send; this.audit = audit; this.now = now; this.uuid = uuid; this.states = new Map(); this.closed = false
  }
  observe(sessionId, event) {
    if (this.closed) return
    if (event.type === 'turn/start') {
      this.states.set(sessionId, { turn: event.data.turn, startedAt: this.now(), bindingId: null, nonce: null, issued: 0, outputTokens: 0, usageKnown: true, stopped: false, lastPlain: false, blocked: false })
      return
    }
    const state = this.states.get(sessionId)
    if (!state) return
    if (event.type === 'turn/end') { this.states.delete(sessionId); return }
    if (event.type === 'tool/call' || event.type === 'tool/result' || event.type === 'assistant/attempt') { state.blocked = true; return }
    if (event.type === 'agent/inbox/spliced') {
      if (event.data.inserted.some(m => m.source?.kind !== CONTROL_SOURCE_KIND && !isNativeContext(m.source))) state.blocked = true
      return
    }
    if (event.type === 'user/message') {
      const source = event.data.source
      if (isNativeContext(source)) return
      if (event.data.content?.some(b => !['text', 'reasoning'].includes(b.type))) state.blocked = true
      if (source?.kind === CONTROL_SOURCE_KIND) {
        if (source.turn !== state.turn || source.nonce !== state.nonce || source.bindingId !== state.bindingId) state.blocked = true
      } else if (source?.kind === SOURCE_KIND) {
        if (state.bindingId !== null) state.blocked = true
        else state.bindingId = source.bindingId
      } else state.blocked = true
      return
    }
    if (event.type !== 'assistant/message') return
    const data = event.data
    if (data.turn !== state.turn || data.interrupted || event.surfaceOp !== 'append') { state.blocked = true; return }
    const blocks = data.message.content
    const pureText = blocks.length > 0 && blocks.every(b => b.type === 'text' || b.type === 'reasoning') && blocks.some(b => b.type === 'text' && b.text.trim())
    if (!pureText || blocks.some(b => !['text', 'reasoning'].includes(b.type))) state.blocked = true
    if (isExactStop(data.message, state.nonce)) { state.stopped = true; state.lastPlain = false; return }
    state.lastPlain = pureText
    const output = data.usage?.outputTokens
    if (!Number.isSafeInteger(output) || output < 0) state.usageKnown = false
    else state.outputTokens += output
    // A max-tokens terminal finish is a budget limit, not an invitation to spend another request.
    if (data.stream.some(r => r.type === 'chunk' && r.chunk?.type === 'finish' && r.chunk.reason?.kind === 'max-tokens')) state.blocked = true
  }
  async stop({ agent, turn, signal }) {
    const state = this.states.get(agent.id)
    if (this.closed || !state || state.turn !== turn || signal.aborted || !this.enabled() || state.blocked || state.stopped || !state.lastPlain || !state.usageKnown || !state.bindingId) return
    const binding = this.binding(state.bindingId); const policy = binding?.continuation
    if (!binding?.enabled || binding.sessionId !== agent.id || !policy?.enabled || state.issued >= policy.maxAdditionalReplies || state.outputTokens >= policy.maxOutputTokens || this.now() - state.startedAt >= policy.maxDurationMs || agent.inbox.nextTurn.length || agent.inbox.nextStep.length) return
    const nonce = this.uuid()
    const source = { kind: CONTROL_SOURCE_KIND, version: 1, bindingId: state.bindingId, turn, nonce }
    const message = { id: this.uuid(), role: 'user', source, content: [{ type: 'text', text: `Optional direct follow-up for this same conversation. If another useful plain-text reply is necessary, write only that reply. Do not repeat yourself or use tools/media for this follow-up. If nothing more is needed, respond with exactly ${stopToken(nonce)} and nothing else. This control requests no permission or approval.` }] }
    const proposal = { source, message, issued: state.issued + 1, remainingOutputTokens: policy.maxOutputTokens - state.outputTokens }
    // Durable sidecar control identity exists before its native inbox call. Late cancellation is checked again after IO.
    await this.prepare(agent.id, proposal)
    if (this.closed || signal.aborted || state.blocked || this.states.get(agent.id) !== state || !this.enabled() || agent.inbox.nextTurn.length || agent.inbox.nextStep.length || this.now() - state.startedAt >= policy.maxDurationMs) return
    state.nonce = nonce; state.issued++; state.lastPlain = false
    this.send(agent, message)
    await this.audit(agent.id, source.bindingId, 'requested', { turn, nonce, issued: state.issued })
  }
  close() { this.closed = true; this.states.clear() }
}
