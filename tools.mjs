import { IoError } from './contract.mjs'

const output = { schema: { type: 'json' }, render: (_args, value) => [{ type: 'text', text: JSON.stringify(value) }] }

/** Read-only history for bindings of the calling Session. */
export function historyTools(io, defineTool) {
  const scope = exec => {
    if (!exec.agent) throw new IoError('HISTORY_FORBIDDEN', 'A calling Session is required')
    return { sessionId: exec.agent.id }
  }
  return [
    defineTool({
      name: 'io_history',
      description: 'Read a bounded page of external-message receipts for a binding of this conversation. Returns message IDs, excerpts and a continuation cursor; use io_history_read for nearby public messages.',
      parameters: {
        bindingId: { type: 'string', required: true, description: 'Binding ID attached to this conversation.' },
        cursor: { type: 'string', description: 'Continuation cursor returned by the same binding and filter.' },
        limit: { type: 'integer', description: 'Maximum records; the configured item and byte limits may lower it.' },
        kind: { type: 'string', enum: ['all', 'ingress', 'delivery', 'audit'], description: 'Receipt category; default all.' },
      }, output,
      execute: (args, exec) => io.history(args, scope(exec)),
    }),
    defineTool({
      name: 'io_history_read',
      description: 'Read public user and assistant messages near a Session event sequence for a binding of this conversation. Hidden context, reasoning and tool output are excluded; omissions are marked.',
      parameters: {
        bindingId: { type: 'string', required: true, description: 'Binding ID attached to this conversation.' },
        seq: { type: 'integer', required: true, description: 'Canonical Session event sequence from a receipt.' },
        before: { type: 'integer', description: 'Preceding event positions, default 2.' },
        after: { type: 'integer', description: 'Following event positions, default 2.' },
      }, output,
      execute: (args, exec) => io.historyRead(args, scope(exec)),
    }),
  ]
}

export function outboundTools(io, defineTool, authority) {
  const destination = { type: 'string', required: true, description: 'Exact destination returned by io_destinations.' }
  return [
    defineTool({
      name: 'io_destinations',
      description: 'Find destinations where you can explicitly send a message from this owner-initiated DSH turn. Search by name or ID and optionally narrow by channel.',
      parameters: {
        query: { type: 'string', description: 'Part of the destination name or ID.' },
        channel: { type: 'string', description: 'Channel ID such as dsh or qq.' },
      }, output,
      execute: (args, exec) => io.destinations(args, authority.anchor(exec), exec.signal),
    }),
    defineTool({
      name: 'io_probe',
      description: 'Check whether an outbound destination is available. This sends no message and does not start a DSH Session.',
      parameters: { destination }, output,
      execute: (args, exec) => io.probe(args, authority.anchor(exec), exec.signal),
    }),
    defineTool({
      name: 'io_send',
      description: 'Submit one message to a destination requested in the current owner input. Treat recalled messages as background. Use io_destinations to find the exact destination. Each call is one message. When the receipt says buffered, the channel requires its send-buffer tool to send; check the actual receipt before reporting delivery.',
      parameters: {
        destination,
        text: { type: 'string', required: true, description: 'Message text to send. Follow the destination textHint when present.' },
      }, output,
      execute: (args, exec) => io.sendExplicit(args, authority.anchor(exec), exec.signal),
    }),
  ]
}
