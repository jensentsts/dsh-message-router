import { ReplyContinuation } from './continuation.mjs'

/** Dual ownership: either Agent disposal or plugin unload removes the same scoped listener. */
export function installContinuation(ctx, io) {
  const owners = new Map(); const native = io.host
  const controller = new ReplyContinuation({
    binding: id => io.store.get('bindings', id),
    enabled: () => !io.closed && io.config.enabled && io.config.continuationEnabled,
    async prepare(sessionId, proposal) {
      await io._audit('continuation', proposal.source.nonce, 'prepared', { bindingId: proposal.source.bindingId, sessionId, turn: proposal.source.turn, nonce: proposal.source.nonce, issued: proposal.issued, remainingOutputTokens: proposal.remainingOutputTokens, messageId: proposal.message.id })
    },
    send(agent, message) { native.send(agent, message, 'steer') },
    audit(sessionId, bindingId, state, detail) { return io._audit('continuation', detail.nonce, state, { sessionId, bindingId, ...detail }) },
  })
  const bind = agent => {
    if (!agent.ctx || owners.has(agent)) return
    const remove = agent.ctx.effect(() => {
      const stopEvent = agent.ctx.on('session/event', (session, event) => {
        if (session.id === agent.id) controller.observe(session.id, event)
      })
      const stopBoundary = agent.ctx.on('agent/turn-stopping', payload => controller.stop(payload))
      return () => { stopEvent(); stopBoundary(); owners.delete(agent) }
    })
    owners.set(agent, remove)
  }
  const uncreate = ctx.on('agent/created', ({ agent }) => { bind(agent) })
  for (const agent of ctx.agents.list()) if (agent.ctx) bind(agent)
  return () => { controller.close(); uncreate(); for (const remove of [...owners.values()]) remove(); owners.clear() }
}
