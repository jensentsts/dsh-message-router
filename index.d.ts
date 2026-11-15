import type { ToolDefinition } from '@deepseek-ai/dsh-tools'
export const SERVICE_KEY: 'superAgentIo'
export const CONTRACT_VERSION: 1
export type InputMode = 'queue' | 'steer' | 'auto'
export type Content = Readonly<{ type: 'text'; text: string } | { type: 'image' | 'file'; attachment: Record<string, unknown> }>
export interface Binding { version?: 1; id: string; transport: string; endpoint: string; sessionId: string; label?: string; enabled?: boolean; sendEnabled?: boolean; receiveOnly?: boolean; preload?: boolean; mode?: InputMode; routing?: 'exclusive-turn'; continuation?: { enabled: boolean; maxAdditionalReplies: number; maxOutputTokens: number; maxDurationMs: number } }
export interface ExternalReplySource { actor: string; provider: string; model: string; sessionId: string; messageId: string; seq: number; turn: number; step: number }
export interface Ingress { bindingId: string; externalMessageId: string; content: readonly Content[]; mode?: InputMode; replyTo?: string; senderId?: string; replySource?: ExternalReplySource }
export interface Receipt { traceId: string; bindingId: string; sessionId: string; messageId: string; state: 'pending' | 'admitting' | 'accepted' | 'failed' | 'unknown' | 'cancelled'; mode: 'queue' | 'steer'; requestedMode: InputMode; duplicate?: boolean; acceptedSeq?: number; messageCommitted?: boolean; durable?: boolean; reason?: string }
export interface Reply { version: 1; sessionId: string; seq: number; messageId: string; turn: number; step: number; time: number; source: { kind: 'model'; provider: string; model: string; [key: string]: unknown }; content: readonly Content[]; usage?: Record<string, number> }
export interface Delivery { deliveryId: string; bindingId: string; endpoint: string; reply: Reply; replyTo?: string; traceIds: string[]; intent?: 'explicit' }
export interface OutboundTarget { endpoint: string; label: string }
export interface OutboundDestination extends OutboundTarget { id: string; transport: string; enabled: true; sendEnabled: true; outboundOnly: true }
export interface Transport {
  id: string
  /** Optional destination channel name, defaulting to the transport ID. */
  channel?: string
  /** Opt in to owner-initiated sends through enabled bindings and send-only targets. */
  explicitSend?: boolean
  /** Optional message formatting hint shown in destination discovery. */
  textHint?: string
  /** Return configured send-only targets synchronously from local state. No Session or network calls. */
  outboundTargets?(): readonly OutboundTarget[]
  /** Probe the adapter and destination without delivering a message. */
  probe?(binding: Readonly<Binding | OutboundDestination>, context: { signal: AbortSignal }): Promise<{ available: boolean }>
  /** Timeout/throw means unknown delivery, never an automatic retry. Called only when global and binding send switches are on. */
  deliver(delivery: Readonly<Delivery>, context: { signal: AbortSignal; idempotencyKey: string }): Promise<{ state: 'buffered'; bufferId: string } | { state: 'delivered'; externalMessageId: string } | { state: 'failed'; reason: string } | { state: 'unknown'; reason: string }>
}
export interface HistoryRequest { bindingId: string; cursor?: string; limit?: number; kind?: 'all' | 'ingress' | 'delivery' | 'audit' }
export interface SuperAgentIo {
  readonly contractVersion: 1
  registerTransport(transport: Transport): () => void
  /** Register an adapter action available only to an owner-initiated native tool call. */
  registerOwnerTool(definition: Omit<ToolDefinition, 'execute'> & {
    execute(args: unknown, exec: Parameters<ToolDefinition['execute']>[1], authorize: () => { callId: string; reply: Omit<Reply, 'content'> }): Promise<unknown>
  }, available?: () => boolean): () => void
  /** Register a channel capability for live Agents, independent of the turn's input source. */
  registerChannelTool(definition: ToolDefinition, available?: () => boolean): () => void
  putBinding(binding: Binding): Promise<Binding>
  listBindings(): Binding[]
  /** Direct public methods are for trusted Host plugins; adapters must not expose them as unauthenticated network routes. */
  receive(input: Ingress): Promise<Receipt>
  receipt(traceId: string): Receipt | undefined
  preload(bindingId: string): Promise<{ sessionId: string; ready: true }>
  /** Read-only, cold-safe, bounded native history. Does not activate Agent. */
  preview(bindingId: string, options?: { limit?: number; beforeSeq?: number }): Promise<unknown>
  /** Explicit creation only. No receive, preload or preview operation silently creates. */
  createSession(request: { cwd?: string; agentPreset?: string }): Promise<{ sessionId: string; agentPreset?: string }>
  history(request: HistoryRequest, scope: { sessionId: string }): Promise<{ items: unknown[]; nextCursor?: string; truncated: boolean }>
  historyRead(request: { bindingId: string; seq: number; before?: number; after?: number }, scope: { sessionId: string }): Promise<unknown>
  cancel(bindingId: string, options?: { keepInbox?: boolean }): Promise<{ requested: boolean; sessionId: string }>
  /** Persists queued committed deliveries. Does not retry ambiguous sending/unknown records. */
  drain(): Promise<void>
  /** Flush already-committed public output for one live Session into registered transports. */
  flushOutbound(sessionId: string): Promise<void>
  /** Reconcile one adapter-owned buffered receipt; does not cause delivery. */
  updateBuffered(bufferId: string, update: { state: 'content' | 'sending' | 'delivered' | 'failed' | 'unknown' | 'cleared'; content?: readonly Content[]; externalMessageId?: string; reason?: string }): Promise<boolean>
}
declare module '@deepseek-ai/cordis' { interface Context { superAgentIo: SuperAgentIo } }
declare module '@deepseek-ai/dsh-llm' { interface MessageSourceMap { 'super-agent-io-control': { kind: 'super-agent-io-control'; version: 1; bindingId: string; turn: number; nonce: string }; 'super-agent-io': { kind: 'super-agent-io'; version: 1; transport: string; bindingId: string; endpoint: string; traceId: string; externalMessageId: string; inputMode?: 'steer' | 'queue'; replyTo?: string; senderId?: string; replySource?: ExternalReplySource } } }
