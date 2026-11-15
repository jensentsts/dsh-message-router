import { IoError } from './contract.mjs'
import { boundedEventPage } from './history.mjs'

const archivedSessions = ctx => ctx.get?.('workspaceRegistry')?.archivedSessionIds ?? []

/** Only documented Host APIs. Borrowed Agents are never disposed on plugin unload. */
export function createNativeHost(ctx) {
  return {
    async resolveAgent(sessionId) {
      const found = await ctx.sessionController.resolveAgent(sessionId)
      if ('error' in found) throw found.error
      return found.agent
    },
    liveAgent(sessionId) { return ctx.agents.get(sessionId) },
    async listSessions(signal) {
      const { items } = await ctx.sessionController.list({}, signal)
      const archived = new Set(archivedSessions(ctx))
      return items.filter(row => row.origin !== 'subagent' && row.formatStatus !== 'migration-required' && !archived.has(row.sessionId))
        .map(row => ({ sessionId: row.sessionId, label: (row.projections?.values.title?.trim() || row.sessionId).slice(0, 160) }))
    },
    isSessionArchived(sessionId) { return archivedSessions(ctx).includes(sessionId) },
    async createSession(request) {
      if (!request || Object.keys(request).some(k => !['cwd', 'agentPreset'].includes(k))) throw new IoError('INVALID_INPUT', 'createSession accepts only cwd and agentPreset')
      return ctx.sessionController.create(request)
    },
    async validateAdmission(agent, message) {
      if (ctx.agents.get(agent.id) !== agent) throw new IoError('STALE_AGENT', 'Agent changed during admission')
      const attachments = ctx.get('attachments')
      const images = message.content.filter(b => b.type === 'image')
      const files = message.content.filter(b => b.type === 'file')
      if ((images.length || files.length) && !attachments) throw new IoError('MEDIA_UNAVAILABLE', 'Attachment service is unavailable')
      if (images.length) {
        const selection = agent.model.current()
        if (selection) {
          const llm = ctx.get('llm')
          if (!llm) throw new IoError('MEDIA_UNAVAILABLE', 'Model capability service is unavailable')
          const info = await llm.resolveModelInfo(selection.provider, selection.model)
          if (info.inputModalities && !info.inputModalities.includes('image')) throw new IoError('MODEL_DOES_NOT_SUPPORT_IMAGES', 'Selected model does not accept images')
        }
        const limits = attachments.imageLimits
        if (images.length > limits.maxImagesPerMessage || images.reduce((sum, b) => sum + b.attachment.bytes, 0) > limits.maxMessageImageBytes) throw new IoError('MEDIA_TOO_LARGE', 'Image message admission policy refused content')
        for (const b of images) await attachments.readImage(b.attachment)
      }
      // Durable file references must be supplied by a trusted adapter after its own file size/path admission.
      if (ctx.agents.get(agent.id) !== agent) throw new IoError('STALE_AGENT', 'Agent changed during attachment validation')
    },
    send(agent, message, mode) {
      if (ctx.agents.get(agent.id) !== agent) throw new IoError('STALE_AGENT', 'Agent changed before sending')
      return ctx.agents.withInitiator(agent, () => mode === 'steer' ? agent.steer(message) : agent.followup(message))
    },
    flush() {
      const persistence = ctx.get('sessionPersistence')
      if (!persistence) throw new IoError('DURABILITY_UNAVAILABLE', 'No session durability service is mounted')
      return persistence.flush()
    },
    async eventsThrough(sessionId, fromSeq, throughSeq) {
      const observation = await ctx.sessionQuery.observeSession(sessionId, { projectionMode: 'none' })
      try { return observation.events.slice(fromSeq, throughSeq + 1) }
      finally { observation[Symbol.dispose]() }
    },
    async preview(sessionId, bounds) {
      const observation = await ctx.sessionQuery.observeSession(sessionId, { projectionMode: 'none' })
      try { return boundedEventPage(sessionId, observation.events, bounds) }
      finally { observation[Symbol.dispose]() }
    },
    async readEvent(sessionId, seq, bounds) {
      if (![bounds.before, bounds.after].every(n => Number.isSafeInteger(n) && n >= 0 && n <= 5)) throw new IoError('INVALID_INPUT', 'Adjacent context must be 0..5 events')
      const window = await ctx.sessionQuery.readEvent({ sessionId, seq, before: bounds.before, after: bounds.after })
      return boundedEventPage(sessionId, window.events, { maxBytes: bounds.maxBytes, limit: 11, targetSeq: seq })
    },
  }
}
