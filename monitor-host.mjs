import { Service } from '@deepseek-ai/cordis'
import { bindTypertRemote } from '@deepseek-ai/dsh-typert-protocol'
import { z } from 'zod'
import { queryMonitor } from './monitor.mjs'
import { MONITOR_NAMESPACE, monitorHostContribution } from './monitor-rpc.mjs'
/** Exact Service method performs authority checks; schema/visibility alone is never authority. */
export class IoMonitorService extends Service {
  constructor(ctx, io) { super(ctx, MONITOR_NAMESPACE); this.io = io; this.typertRemote = bindTypertRemote(this, MONITOR_NAMESPACE, { namespace: MONITOR_NAMESPACE }) }
  async query(request, signal) {
    const invocation = this.ctx.invocation
    if (invocation !== undefined && invocation.peer?.principal?.kind !== 'owner') return { ok: false, error: { code: 'OWNER_REQUIRED', message: 'Only the owner can read IO transport receipts.' } }
    try { return await queryMonitor(this.io, request, signal) }
    catch (error) { return { ok: false, error: { code: String(error?.code ?? 'MONITOR_FAILED').slice(0, 100), message: error?.code ? String(error.message).slice(0, 300) : 'IO monitor data is unavailable.' } } }
  }
}
export function installMonitor(ctx, io) {
  const service = new IoMonitorService(ctx, io)
  const remove = ctx.typert.register(monitorHostContribution(z))
  return () => { void service; return remove() }
}
