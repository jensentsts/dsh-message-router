import { MAX_MONITOR_BYTES, monitorBytes, monitorBound } from './monitor.mjs'
export const MONITOR_NAMESPACE = 'superAgentIoMonitor'
const PACKAGE = 'dsh-super-agent-io/monitor'
export function monitorSchemas(z) {
  const id = () => z.string().min(1).max(512)
  const cursor = () => z.string().min(1).max(4096).optional()
  const limit = () => z.number().int().min(1).max(100).optional()
  const request = z.discriminatedUnion('action', [
    z.strictObject({ action: z.literal('bindings'), cursor: cursor(), limit: limit() }),
    z.strictObject({ action: z.literal('history'), bindingId: id(), kind: z.enum(['all', 'ingress', 'delivery', 'audit']).optional(), cursor: cursor(), limit: limit() }),
    z.strictObject({ action: z.literal('receipt'), traceId: id() }),
  ]).refine(value => monitorBytes(value) <= 8192, 'Monitor request exceeds 8 KiB')
  const outcome = z.discriminatedUnion('ok', [z.strictObject({ ok: z.literal(true), value: z.json() }), z.strictObject({ ok: z.literal(false), error: z.strictObject({ code: id(), message: z.string().max(512) }) })]).refine(value => monitorBytes(value) <= MAX_MONITOR_BYTES, 'Monitor response exceeds 64 KiB')
  return { request, outcome }
}
export function monitorDescriptor(z) {
  return {
    id: `${PACKAGE}#${MONITOR_NAMESPACE}/query`, service: MONITOR_NAMESPACE, namespace: MONITOR_NAMESPACE, method: 'query', invocation: { kind: 'direct' },
    parameters: [{ name: 'request', wire: 'request', source: 'json', codec: { mode: 'strict', typeSymbol: 'IoMonitorRequest', create: () => monitorSchemas(z).request } }],
    cancellation: { parameter: 'signal' },
    result: { mode: 'strict', typeSymbol: 'IoMonitorOutcome', create: () => monitorSchemas(z).outcome, encode: monitorBound },
    description: 'Owner-only bounded read-only IO bindings, receipts and public history. Never activates a Session or delivers work.',
  }
}
export function monitorHostContribution(z) {
  return { package: PACKAGE, face: 'host', schemas: [{ name: 'Request', create: () => monitorSchemas(z).request }, { name: 'Outcome', create: () => monitorSchemas(z).outcome }], invocations: [monitorDescriptor(z)], model: { services: [{ key: MONITOR_NAMESPACE, exportName: 'IoMonitorService', tags: [], members: [{ kind: 'method', name: 'query', signature: 'query(request: IoMonitorRequest, signal?: AbortSignal): Promise<IoMonitorOutcome>', summary: 'Read-only owner monitor; no Session activation, input or external sends.' }], types: [] }], events: [], objects: [] } }
}
export function monitorClientContribution(z) { return { package: PACKAGE, descriptors: [monitorDescriptor(z)] } }
