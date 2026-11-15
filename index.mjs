import z from '@deepseek-ai/schemastery'
import { defineTool } from '@deepseek-ai/dsh-tools'
import { IoError, SERVICE_KEY } from './contract.mjs'
import { IoCore } from './core.mjs'
import { DomainStore } from './store.mjs'
import { domainSpec } from './domain.mjs'
import { createNativeHost } from './native-host.mjs'
import { historyTools, outboundTools } from './tools.mjs'
import { installExplicitAuthority } from './explicit-authority.mjs'
import { installContinuation } from './continuation-host.mjs'
import { installMonitor } from './monitor-host.mjs'

export const name = 'super-agent-io'
export const inject = ['storageDomain', 'sessionController', 'sessionQuery', 'agents', 'tools']
const integer = (value, min, max) => z.number().step(1).min(min).max(max).default(value)
/** All settings are live schema-derived fields; configuration never rewrites profiles by hand. */
export const Config = z.object({
  enabled: z.boolean().default(false),
  sendEnabled: z.boolean().default(false),
  continuationEnabled: z.boolean().default(false),
  maxPendingPerBinding: integer(100, 1, 1000),
  maxConcurrentSessions: integer(4, 1, 32),
  maxConsecutiveSteers: integer(3, 0, 20),
  maxTextChars: integer(32768, 1, 262144),
  maxBlocks: integer(24, 1, 100),
  maxRecords: integer(10000, 100, 100000),
  historyLimit: integer(30, 1, 100),
  historyMaxBytes: integer(32768, 512, 262144),
  admissionTimeoutMs: integer(30000, 100, 120000),
  deliveryTimeoutMs: integer(30000, 100, 120000),
}).volatile()

/** Mount one durable transport-neutral service. No external network endpoint or approval policy is installed. */
export async function apply(ctx, config) {
  const store = new DomainStore(await ctx.storageDomain.open(domainSpec), domainSpec)
  const io = new IoCore({ store, host: createNativeHost(ctx), config: () => config.get(), report: error => ctx.logger.warn(`super-agent-io: ${String(error?.code ?? error?.name ?? 'ERROR')}`) })
  const release = []
  // A single owner shuts admission first, releases consumers, then drains and closes its domain.
  ctx.effect(() => async () => { for (const dispose of release.reverse()) await dispose(); await io.close() })
  try {
    await io.start()
    const authority = installExplicitAuthority(ctx)
    release.push(authority.dispose)
    const sendEnabled = () => config.get().enabled && config.get().sendEnabled
    const maySend = scope => sendEnabled() && authority.when(scope)
    const registerOwnerTool = (definition, available = () => true) => ctx.tools.register({
      ...definition,
      execute(args, exec) {
        const authorize = () => {
          if (!sendEnabled() || !available()) throw new IoError('OUTBOUND_FORBIDDEN', 'Outbound owner tools are unavailable')
          return authority.anchor(exec)
        }
        authorize()
        return definition.execute(args, exec, authorize)
      },
    }, { when: scope => maySend(scope) && available() })
    // A registered channel capability is available to any live Agent; it owns
    // its own destination and delivery checks instead of inheriting owner-turn routing.
    const registerChannelTool = (definition, available = () => true) => ctx.tools.register({
      ...definition,
      execute(args, exec) {
        if (!sendEnabled() || !available()) throw new IoError('OUTBOUND_FORBIDDEN', 'Channel tools are unavailable')
        return definition.execute(args, exec)
      },
    }, { when: () => sendEnabled() && available() })
    const methods = ['registerTransport', 'putBinding', 'listBindings', 'receive', 'receipt', 'preload', 'preview', 'createSession', 'history', 'historyRead', 'cancel', 'drain', 'flushOutbound', 'updateBuffered']
    const service = Object.freeze({ ...Object.fromEntries(methods.map(key => [key, io[key].bind(io)])), registerOwnerTool, registerChannelTool, contractVersion: 1 })
    ctx.provide(SERVICE_KEY, service)
    release.push(installContinuation(ctx, io))
    const monitorFiber = ctx.inject(['typert'], monitorCtx => { monitorCtx.effect(() => installMonitor(monitorCtx, io)) })
    release.push(() => monitorFiber.dispose())
    release.push(ctx.on('session/event', (session, event) => { void io.observe(session.id, event).catch(error => ctx.logger.warn(`super-agent-io event: ${String(error?.code ?? error?.name ?? 'ERROR')}`)) }))
    for (const definition of historyTools(io, defineTool)) release.push(ctx.tools.register(definition, { when: ({ agent }) => config.get().enabled && io.listBindings().some(b => b.enabled && b.sessionId === agent.id) }))
    for (const definition of outboundTools(io, defineTool, authority)) release.push(ctx.tools.register(definition, { when: maySend }))
    // Bindings persist independently of QQ/remote plugin restarts. Loading never starts a model request.
    if (config.get().enabled) for (const b of io.listBindings()) if (b.enabled && b.preload) void io.preload(b.id).catch(error => ctx.logger.warn(`super-agent-io preload ${b.id}: ${String(error?.code ?? error?.name ?? 'ERROR')}`))
  } catch (error) { await io.close(); throw error }
}
export { SERVICE_KEY, CONTRACT_VERSION } from './contract.mjs'
