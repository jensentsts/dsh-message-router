/** Public, dependency-free contract for dsh-super-agent-io 0.0.1. */
export const SERVICE_KEY = 'superAgentIo'
export const CONTRACT_VERSION = 1
export const SOURCE_KIND = 'super-agent-io'
export const CONTROL_SOURCE_KIND = 'super-agent-io-control'
export const stopToken = nonce => `[io-stop:${nonce}]`
export function isExactStop(message, nonce) { return Boolean(nonce && message?.content?.length === 1 && message.content[0].type === 'text' && message.content[0].text === stopToken(nonce)) }
export const DOMAIN_NAME = 'super_agent_io'
export const TABLE_NAMES = Object.freeze(['bindings', 'traces', 'deliveries', 'cursors', 'audit'])
export const INPUT_MODES = Object.freeze(['queue', 'steer', 'auto'])
export const TRACE_STATES = Object.freeze(['pending', 'admitting', 'accepted', 'failed', 'unknown', 'cancelled'])
export const DELIVERY_STATES = Object.freeze(['pending', 'sending', 'buffered', 'delivered', 'failed', 'unknown', 'cancelled', 'suppressed'])
export class IoError extends Error {
  constructor(code, message) { super(message); this.name = 'IoError'; this.code = code }
}
export function requiredString(value, label, max = 512) {
  if (typeof value !== 'string' || !value.trim() || value.length > max) throw new IoError('INVALID_INPUT', `${label} must be a nonempty string of at most ${max} characters`)
  return value
}
/** A key encodes components rather than joining with a delimiter callers can inject. */
export function identityKey(...parts) { return JSON.stringify(parts.map(String)) }
export function jsonCopy(value) {
  const seen = new Set()
  const check = (v) => {
    if (v === null || typeof v === 'string' || typeof v === 'boolean') return
    if (typeof v === 'number' && Number.isFinite(v) && !Object.is(v, -0)) return
    if (typeof v !== 'object' || seen.has(v)) throw new IoError('INVALID_JSON', 'Value must be lossless JSON')
    if (!Array.isArray(v) && Object.getPrototypeOf(v) !== Object.prototype && Object.getPrototypeOf(v) !== null) throw new IoError('INVALID_JSON', 'Value must be a plain JSON record')
    seen.add(v)
    if (Array.isArray(v)) { for (let i = 0; i < v.length; i++) { if (!(i in v)) throw new IoError('INVALID_JSON', 'Sparse arrays are not JSON'); check(v[i]) } }
    else for (const key of Object.keys(v)) check(v[key])
    seen.delete(v)
  }
  check(value)
  return structuredClone(value)
}
/** Host-trusted binding management. It never creates/resumes a Session by itself. */
export function normalizeBinding(input) {
  const value = jsonCopy(input)
  const id = requiredString(value.id, 'binding.id', 128)
  const transport = requiredString(value.transport, 'binding.transport', 128)
  const sessionId = requiredString(value.sessionId, 'binding.sessionId')
  const endpoint = requiredString(value.endpoint, 'binding.endpoint')
  for (const key of ['enabled', 'sendEnabled', 'receiveOnly', 'preload']) if (value[key] !== undefined && typeof value[key] !== 'boolean') throw new IoError('INVALID_INPUT', `binding.${key} must be a boolean`)
  const mode = value.mode ?? 'auto'
  if (!INPUT_MODES.includes(mode)) throw new IoError('INVALID_INPUT', 'binding.mode must be queue, steer, or auto')
  if (value.routing !== undefined && value.routing !== 'exclusive-turn') throw new IoError('INVALID_INPUT', 'Only fail-closed exclusive-turn routing is supported')
  let continuation
  if (value.continuation !== undefined) {
    const c = value.continuation
    if (!c || typeof c !== 'object' || Object.keys(c).some(k => !['enabled', 'maxAdditionalReplies', 'maxOutputTokens', 'maxDurationMs'].includes(k))) throw new IoError('INVALID_INPUT', 'Invalid continuation policy')
    if (typeof c.enabled !== 'boolean') throw new IoError('INVALID_INPUT', 'continuation.enabled must be a boolean')
    for (const [key, min, max] of [['maxAdditionalReplies', 1, 8], ['maxOutputTokens', 1, 32768], ['maxDurationMs', 100, 120000]]) if (!Number.isSafeInteger(c[key]) || c[key] < min || c[key] > max) throw new IoError('INVALID_INPUT', `continuation.${key} must be ${min}..${max}`)
    continuation = { enabled: c.enabled === true, maxAdditionalReplies: c.maxAdditionalReplies, maxOutputTokens: c.maxOutputTokens, maxDurationMs: c.maxDurationMs }
  }
  const receiveOnly = value.receiveOnly === true
  if (receiveOnly && continuation?.enabled) throw new IoError('INVALID_INPUT', 'Receive-only bindings do not request continuation replies')
  return { version: 1, id, transport, sessionId, endpoint, label: String(value.label ?? id).slice(0, 160), enabled: value.enabled === true, sendEnabled: !receiveOnly && value.sendEnabled === true, ...(receiveOnly ? { receiveOnly } : {}), preload: value.preload === true, mode, routing: 'exclusive-turn', ...(continuation ? { continuation } : {}) }
}
/** Accept only public text or already-admitted durable media references, never reasoning/tools/raw bytes. */
export function validatePublicContent(content, { maxTextChars = 32768, maxBlocks = 24 } = {}) {
  if (!Array.isArray(content) || content.length === 0 || content.length > maxBlocks) throw new IoError('INVALID_CONTENT', `content must contain 1..${maxBlocks} blocks`)
  const blocks = jsonCopy(content)
  let textLength = 0
  for (const block of blocks) {
    if (!block || typeof block !== 'object') throw new IoError('INVALID_CONTENT', 'Invalid content block')
    if (block.type === 'text') {
      if (typeof block.text !== 'string' || Object.keys(block).some(k => k !== 'type' && k !== 'text')) throw new IoError('INVALID_CONTENT', 'Text block must contain only type and text')
      textLength += block.text.length
    } else if (block.type === 'image' || block.type === 'file') {
      const a = block.attachment
      if (!a || typeof a !== 'object' || typeof a.attachmentId !== 'string' || !a.attachmentId || !Number.isSafeInteger(a.bytes) || a.bytes < 0) throw new IoError('INVALID_CONTENT', 'Media must carry an admitted durable attachment reference')
      if (Object.keys(block).some(k => k !== 'type' && k !== 'attachment' && !(k === 'offloaded' && block.type === 'image'))) throw new IoError('INVALID_CONTENT', 'Raw media is not accepted')
      const allowed = block.type === 'file' ? ['attachmentId', 'bytes', 'name'] : ['attachmentId', 'bytes', 'mediaType', 'width', 'height', 'name', 'originalDimensions']
      if (Object.keys(a).some(k => !allowed.includes(k)) || (a.name !== undefined && typeof a.name !== 'string') || (block.offloaded !== undefined && block.offloaded !== true)) throw new IoError('INVALID_CONTENT', 'Invalid durable reference fields')
      requiredString(a.attachmentId, 'attachmentId')
      if (block.type === 'file') requiredString(a.name, 'file.name')
      if (a.originalDimensions && (!Number.isSafeInteger(a.originalDimensions.width) || a.originalDimensions.width <= 0 || !Number.isSafeInteger(a.originalDimensions.height) || a.originalDimensions.height <= 0 || Object.keys(a.originalDimensions).some(k => k !== 'width' && k !== 'height'))) throw new IoError('INVALID_CONTENT', 'Invalid original image dimensions')
      if (block.type === 'image' && (!['image/png', 'image/jpeg', 'image/webp', 'image/gif'].includes(a.mediaType) || !Number.isSafeInteger(a.width) || a.width <= 0 || !Number.isSafeInteger(a.height) || a.height <= 0)) throw new IoError('INVALID_CONTENT', 'Invalid image reference')
    } else throw new IoError('INVALID_CONTENT', 'Only text, image and file blocks are public IO content')
  }
  if (textLength > maxTextChars) throw new IoError('CONTENT_TOO_LARGE', `Text exceeds ${maxTextChars} characters`)
  if (!blocks.some(b => b.type !== 'text' || b.text.trim())) throw new IoError('INVALID_CONTENT', 'Empty input is not a prompt')
  return blocks
}
/** Adapter-facing ingress excludes identity/role/source supplied by an external sender. */
export function normalizeIngress(input, options) {
  const value = jsonCopy(input)
  const result = {
    bindingId: requiredString(value.bindingId, 'bindingId', 128),
    externalMessageId: requiredString(value.externalMessageId, 'externalMessageId'),
    content: validatePublicContent(value.content, options),
  }
  if (value.mode !== undefined) {
    if (!INPUT_MODES.includes(value.mode)) throw new IoError('INVALID_INPUT', 'mode must be queue, steer, or auto')
    result.mode = value.mode
  }
  if (value.replyTo !== undefined) result.replyTo = requiredString(value.replyTo, 'replyTo')
  if (value.senderId !== undefined) result.senderId = requiredString(value.senderId, 'senderId')
  if (value.replySource !== undefined) {
    const source = value.replySource
    if (!source || typeof source !== 'object' || Array.isArray(source) || Object.keys(source).some(key => !['actor', 'provider', 'model', 'sessionId', 'messageId', 'seq', 'turn', 'step'].includes(key))) throw new IoError('INVALID_INPUT', 'Invalid reply source')
    const replySource = {}
    for (const key of ['actor', 'provider', 'model', 'sessionId', 'messageId']) replySource[key] = requiredString(source[key], `replySource.${key}`)
    for (const key of ['seq', 'turn', 'step']) {
      if (!Number.isSafeInteger(source[key]) || source[key] < 0) throw new IoError('INVALID_INPUT', `replySource.${key} must be a nonnegative safe integer`)
      replySource[key] = source[key]
    }
    result.replySource = replySource
  }
  return result
}
/** One committed model message, not an attempt or a replacement copy. */
export function publicReply(sessionId, event) {
  if (event?.type !== 'assistant/message' || event.surfaceOp !== 'append' || event.data?.interrupted === true) return null
  const m = event.data.message
  if (m?.role !== 'assistant' || m.source?.kind !== 'model' || !m.source.provider || !m.source.model || !Array.isArray(m.content)) return null
  const content = m.content.filter(b => b?.type === 'text' ? typeof b.text === 'string' && b.text.trim() : (b?.type === 'image' || b?.type === 'file') && b.attachment)
  if (!content.length) return null
  const { replayState, ...source } = m.source
  return jsonCopy({ version: 1, sessionId, seq: event.seq, messageId: m.id, turn: event.data.turn, step: event.data.step, time: event.time, source, content, ...(event.data.usage ? { usage: event.data.usage } : {}) })
}
