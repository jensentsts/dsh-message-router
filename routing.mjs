import { SOURCE_KIND, CONTROL_SOURCE_KIND, isExactStop, jsonCopy, publicReply } from './contract.mjs'

const nativeContextKinds = new Set(['runtime-context', 'time-context', 'agent-instructions', 'skill-catalog', 'system-prompt', 'session-reference', 'repeat-tool-reminder'])
export const isNativeContext = source => nativeContextKinds.has(source?.kind)

/** Session-derived routing only: no recent-sender timeout or ambient transport state. */
export function initialCursor(sessionId) {
  return { version: 1, sessionId, lastSeq: -1, nextTurn: [], nextStep: [], turn: null, tainted: true, bindings: [], traces: [], controlNonce: null }
}
function addOwner(state, item) {
  if (item?.context) return
  if (!item?.bindingId) { state.tainted = true; return }
  if (!state.bindings.includes(item.bindingId)) state.bindings.push(item.bindingId)
  if (item.traceId && !state.traces.includes(item.traceId)) state.traces.push(item.traceId)
}
/** Fold a contiguous canonical Session event, preserving enough pending identity to route the next turn. */
export function foldRouting(previous, event) {
  if (event.seq <= previous.lastSeq) return { cursor: previous, reply: null, duplicate: true }
  if (event.seq !== previous.lastSeq + 1) return { cursor: previous, reply: null, gap: true }
  const state = jsonCopy(previous)
  state.lastSeq = event.seq
  switch (event.type) {
    case 'turn/start':
      state.turn = event.data.turn; state.tainted = false; state.bindings = []; state.traces = []; state.controlNonce = null
      break
    case 'agent/inbox/spliced': {
      const d = event.data
      const target = d.target === 'next-turn' ? 'nextTurn' : 'nextStep'
      const entries = (d.inserted ?? []).map(message => {
        const s = message.source
        if (s?.kind === CONTROL_SOURCE_KIND || isNativeContext(s)) return { id: message.id, context: true }
        return s?.kind === SOURCE_KIND && s.version === 1
          ? { id: message.id, bindingId: s.bindingId, traceId: s.traceId }
          : { id: message.id }
      })
      const removed = state[target].splice(d.start, d.removedCount ?? 0, ...entries)
      if (d.outcome !== 'canceled' && removed.length && state.turn !== null) for (const item of removed) addOwner(state, item)
      break
    }
    case 'user/message': {
      const s = event.data.source
      if (s?.kind === SOURCE_KIND && s.version === 1) addOwner(state, s)
      else if (s?.kind === CONTROL_SOURCE_KIND && s.version === 1 && s.turn === state.turn && state.bindings.length === 1 && s.bindingId === state.bindings[0]) state.controlNonce = s.nonce
      else if (!isNativeContext(s)) state.tainted = true
      break
    }
    case 'turn/end':
      state.turn = null; state.bindings = []; state.traces = []; state.tainted = true
      break
  }
  const controlOnly = event.type === 'assistant/message' && event.surfaceOp === 'append' && !event.data.interrupted && isExactStop(event.data.message, state.controlNonce)
  const reply = controlOnly ? null : publicReply(state.sessionId, event)
  return { cursor: state, reply, controlOnly, routable: Boolean(reply && state.turn === reply.turn && !state.tainted && state.bindings.length === 1 && state.traces.length) }
}
