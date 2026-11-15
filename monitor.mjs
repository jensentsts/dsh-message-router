import { IoError, jsonCopy, requiredString } from './contract.mjs'
export const MAX_MONITOR_BYTES = 64 * 1024
export const monitorBytes = value => new TextEncoder().encode(JSON.stringify(value)).byteLength
export function monitorBound(value) {
  if (monitorBytes(value) > MAX_MONITOR_BYTES) return { ok: false, error: { code: 'RESULT_TOO_LARGE', message: 'The complete monitor result exceeds 64 KiB. Reduce the page size.' } }
  return value
}
/** Whitelist read operations even for trusted callers; no receive, drain, stop, model or write member is routable. */
export function parseMonitorRequest(value) {
  const input = jsonCopy(value)
  if (!input || typeof input !== 'object' || Array.isArray(input) || monitorBytes(input) > 8192) throw new IoError('INVALID_INPUT', 'Invalid bounded monitor query')
  const shapes = { bindings: ['action', 'cursor', 'limit'], history: ['action', 'bindingId', 'kind', 'cursor', 'limit'], receipt: ['action', 'traceId'] }
  const allowed = shapes[input.action]
  if (!allowed || Object.keys(input).some(k => !allowed.includes(k))) throw new IoError('INVALID_INPUT', 'Only declared read-only monitor fields are accepted')
  if (input.limit !== undefined && (!Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > 100)) throw new IoError('INVALID_INPUT', 'limit must be 1..100')
  if (input.cursor !== undefined) requiredString(input.cursor, 'cursor', 4096)
  if (input.action === 'history') {
    requiredString(input.bindingId, 'bindingId', 128)
    if (input.kind !== undefined && !['all', 'ingress', 'delivery', 'audit'].includes(input.kind)) throw new IoError('INVALID_INPUT', 'Unknown history kind')
  }
  if (input.action === 'receipt') requiredString(input.traceId, 'traceId')
  return input
}
/** Derives only receipt sidecars. Reading never wakes an Agent or advances an outbox. */
export async function queryMonitor(io, raw, signal) {
  const request = parseMonitorRequest(raw); signal?.throwIfAborted()
  if (request.action === 'receipt') return monitorBound({ ok: true, value: { action: 'receipt', receipt: io.receipt(request.traceId) ?? null } })
  if (request.action === 'history') {
    const binding = io.listBindings().find(b => b.id === request.bindingId)
    if (!binding) throw new IoError('BINDING_NOT_FOUND', 'No binding has that ID')
    const history = await io.history({ bindingId: request.bindingId, ...(request.kind ? { kind: request.kind } : {}), ...(request.cursor ? { cursor: request.cursor } : {}), ...(request.limit ? { limit: request.limit } : {}) }, { sessionId: binding.sessionId })
    signal?.throwIfAborted()
    return monitorBound({ ok: true, value: { action: 'history', bindingId: binding.id, sessionId: binding.sessionId, ...history } })
  }
  let after = ''
  if (request.cursor) {
    try { const parsed = JSON.parse(Buffer.from(request.cursor, 'base64url').toString('utf8')); if (parsed.action !== 'bindings' || typeof parsed.after !== 'string') throw new Error(); after = parsed.after }
    catch (error) { throw new IoError('INVALID_CURSOR', 'Invalid binding continuation cursor') }
  }
  const bindings = io.listBindings().filter(b => b.id > after).sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0)
  const selected = bindings.slice(0, request.limit ?? 30)
  const items = selected.map(b => {
    const inputStates = {}; const outputStates = {}
    for (const [, t] of io.store.entries('traces')) if (t.bindingId === b.id) inputStates[t.state] = (inputStates[t.state] ?? 0) + 1
    for (const [, d] of io.store.entries('deliveries')) if (d.bindingId === b.id) outputStates[d.state] = (outputStates[d.state] ?? 0) + 1
    return { ...b, inputs: inputStates, outputs: outputStates }
  })
  const more = selected.length < bindings.length
  return monitorBound({ ok: true, value: { action: 'bindings', contractVersion: io.contractVersion, gates: { enabled: io.config.enabled, sendEnabled: io.config.sendEnabled, continuationEnabled: io.config.continuationEnabled }, items, truncated: more, ...(more ? { nextCursor: Buffer.from(JSON.stringify({ action: 'bindings', after: selected.at(-1).id })).toString('base64url') } : {}) } })
}
