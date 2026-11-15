import { IoError } from './contract.mjs'
import { isNativeContext } from './routing.mjs'

const nonempty = value => typeof value === 'string' && value.length > 0

function ownerPromptKey(message) {
  const source = message?.source
  if (message?.role !== 'user' || !source || source.principal !== undefined) return
  if (source.kind === 'user' && nonempty(source.rpcId)) return JSON.stringify([source.kind, source.rpcId])
  // Workspace Reference emits this source after the owner confirms a composer route.
  if (source.kind === 'workspace-reference' && nonempty(source.originSessionId) && nonempty(source.requestId)
    && Number.isSafeInteger(source.blockIndex) && source.blockIndex >= 0) {
    return JSON.stringify([source.kind, source.originSessionId, source.requestId, source.blockIndex])
  }
}

/** Bind explicit tool calls to the live AgentLoop and its actual requesting assistant. */
export function installExplicitAuthority(ctx) {
  const turns = new WeakMap()
  const stateFor = agent => agent && ctx.agents.get(agent.id) === agent ? turns.get(agent.session) : undefined
  const eligible = state => Boolean(state?.claims.size && !state.tainted)
  const stopClaim = ctx.on('agent/inbox/claimed', ({ agent, message, turn }) => {
    const state = stateFor(agent)
    if (state?.turn !== turn || isNativeContext(message.source)) return
    const key = ownerPromptKey(message)
    if (key) state.claims.set(message.id, key)
    else state.tainted = true
  })
  const stopSession = ctx.on('session/event', (session, event) => {
    const data = event.data
    if (event.type === 'turn/start') {
      turns.set(session, { turn: data.turn, claims: new Map(), ownerCommitted: false, tainted: false })
      return
    }
    const state = turns.get(session)
    if (!state) return
    switch (event.type) {
      case 'step/start':
        state.step = data.step
        state.assistant = undefined
        state.calls = new Map()
        break
      case 'user/message': {
        if (isNativeContext(data.source)) break
        const key = ownerPromptKey(data)
        if (state.step !== undefined && event.surfaceOp === 'append' && key && state.claims.get(data.id) === key) state.ownerCommitted = true
        else state.tainted = true
        break
      }
      case 'assistant/message': {
        const message = data.message
        if (data.turn !== state.turn || data.step !== state.step || event.surfaceOp !== 'append' || data.interrupted || message?.role !== 'assistant' || message.source?.kind !== 'model') break
        const { replayState, ...source } = message.source
        if (!source.provider || !source.model) break
        state.assistant = {
          blocks: new Map(message.content.filter(block => block.type === 'tool-call').map(block => [block.id, block.name])),
          reply: { version: 1, sessionId: session.id, seq: event.seq, messageId: message.id, turn: data.turn, step: data.step, time: event.time, source },
        }
        break
      }
      case 'tool/call':
        if (data.turn === state.turn && data.step === state.step && state.assistant?.blocks.get(data.callId) === data.name) state.calls.set(data.callId, { name: data.name, root: data.callId })
        break
      case 'tool/ptc-dispatch-start': {
        const parent = state.calls?.get(data.parentCallId)
        if (parent?.root === data.rootCallId) state.calls.set(data.subCallId, { name: data.name, root: data.rootCallId, nested: true })
        break
      }
      case 'step/end':
        state.step = undefined
        state.assistant = undefined
        state.calls = undefined
        break
      case 'turn/end':
        turns.delete(session)
        break
    }
  })
  return {
    when: ({ agent }) => eligible(stateFor(agent)),
    anchor(exec) {
      exec.signal.throwIfAborted()
      const state = stateFor(exec.agent)
      const call = state?.calls?.get(exec.callId)
      if (!eligible(state) || !state.ownerCommitted || !state.assistant || !call || call.name !== exec.name || call.root !== (exec.rootCallId ?? exec.callId) || Boolean(call.nested) !== Boolean(exec.parent)) {
        throw new IoError('OUTBOUND_FORBIDDEN', 'Explicit destinations require a committed owner prompt and a native tool call in its active turn')
      }
      return { callId: exec.callId, reply: state.assistant.reply }
    },
    dispose() { stopSession(); stopClaim() },
  }
}
