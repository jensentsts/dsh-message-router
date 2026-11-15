import { IoError } from './contract.mjs'

/** Public-facing native history never exposes system prompts, tool results, reasoning or provider replay state. */
export function publicEventSummary(event) {
  let message
  if (event.type === 'user/message') {
    // Context snapshots and trigger/context messages can use role:user without being public user input.
    if (event.surfaceOp !== 'append' || event.sourceEventSeqs?.length || !['user', 'super-agent-io'].includes(event.data.source?.kind)) return null
    message = event.data
  } else if (event.type === 'assistant/message' && event.surfaceOp === 'append' && !event.data.interrupted) message = event.data.message
  else return null
  const content = message.content ?? []
  const text = content.filter(b => b.type === 'text').map(b => b.text).join('\n')
  const source = message.source
  const { replayState, ...publicModelSource } = source
  const visibleSource = source.kind === 'model'
    ? publicModelSource
    : source.kind === 'super-agent-io'
      ? { kind: source.kind, transport: source.transport, bindingId: source.bindingId, traceId: source.traceId }
      : { kind: source.kind }
  return { seq: event.seq, time: event.time, messageId: message.id, role: message.role, source: visibleSource, text: text.slice(0, 2048), textTruncated: text.length > 2048, media: content.filter(b => b.type === 'image' || b.type === 'file').map(b => ({ type: b.type, attachment: b.attachment })).slice(0, 12) }
}
/** Bound the complete wrapper, not only its text. Every omission is explicit. */
export function boundedEventPage(sessionId, events, { maxBytes, limit = 30, beforeSeq, targetSeq } = {}) {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100 || !Number.isSafeInteger(maxBytes) || maxBytes < 256) throw new IoError('INVALID_INPUT', 'Invalid history bounds')
  if (beforeSeq !== undefined && (!Number.isSafeInteger(beforeSeq) || beforeSeq < 0)) throw new IoError('INVALID_INPUT', 'beforeSeq must be nonnegative')
  const visible = events.filter(e => beforeSeq === undefined || e.seq < beforeSeq).map(publicEventSummary).filter(Boolean)
  const selected = targetSeq === undefined ? visible.slice(-limit) : visible.slice(0, limit)
  const result = []; let omitted = visible.length > selected.length
  for (let i = selected.length - 1; i >= 0; i--) {
    const next = [selected[i], ...result]
    const candidate = { sessionId, events: next, truncated: true, ...(next.length ? { beforeSeq: next[0].seq } : {}) }
    if (Buffer.byteLength(JSON.stringify(candidate)) > maxBytes) { omitted = true; break }
    result.unshift(selected[i])
  }
  const output = { sessionId, events: result, truncated: omitted, ...(omitted && result.length ? { beforeSeq: result[0].seq } : {}), ...(targetSeq === undefined ? {} : { requestedSeq: targetSeq, targetIncluded: result.some(e => e.seq === targetSeq) }) }
  if (Buffer.byteLength(JSON.stringify(output)) > maxBytes || (!result.length && selected.length)) throw new IoError('OUTPUT_TOO_LARGE', 'History byte limit cannot fit one bounded message')
  return output
}
