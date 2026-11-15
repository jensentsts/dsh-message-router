import { createHash, randomUUID } from 'node:crypto'
import { CONTRACT_VERSION, SOURCE_KIND, CONTROL_SOURCE_KIND, IoError, identityKey, jsonCopy, normalizeBinding, normalizeIngress, requiredString, validatePublicContent } from './contract.mjs'
import { initialCursor, foldRouting } from './routing.mjs'

/** Deployment choices are resolved once; the Host exposes all of them in its Config schema. */
export function resolveOptions(value = {}) {
  const defaults = { enabled: false, sendEnabled: false, continuationEnabled: false, maxPendingPerBinding: 100, maxConcurrentSessions: 4, maxConsecutiveSteers: 3, maxTextChars: 32768, maxBlocks: 24, maxRecords: 10000, historyLimit: 30, historyMaxBytes: 32768, admissionTimeoutMs: 30000, deliveryTimeoutMs: 30000 }
  const result = { ...defaults, ...value }
  for (const key of Object.keys(defaults)) {
    if (typeof defaults[key] === 'boolean') { if (typeof result[key] !== 'boolean') throw new IoError('INVALID_CONFIG', key) }
    else if (!Number.isSafeInteger(result[key]) || result[key] < (key === 'maxConsecutiveSteers' ? 0 : 1)) throw new IoError('INVALID_CONFIG', key)
  }
  if (result.historyMaxBytes < 512 || result.deliveryTimeoutMs > 2147483647 || result.admissionTimeoutMs > 2147483647) throw new IoError('INVALID_CONFIG', 'Invalid output byte bound or timeout')
  return result
}
const digest = (...parts) => createHash('sha256').update(identityKey(...parts)).digest('hex')
const receiptOf = t => jsonCopy(Object.fromEntries(['traceId', 'bindingId', 'sessionId', 'messageId', 'state', 'mode', 'requestedMode', 'acceptedSeq', 'messageCommitted', 'durable', 'reason'].filter(k => t[k] !== undefined).map(k => [k, t[k]])))
const reasonOf = e => String(e?.code ?? e?.name ?? 'ERROR').slice(0, 100)
const DSH_CHANNEL = 'dsh'
const OUTBOUND_TARGET = ':outbound:'
const dshDestination = sessionId => `${DSH_CHANNEL}:${sessionId}`
const dshBindingId = sessionId => dshDestination(digest(sessionId))
const isDshBinding = b => b?.transport === DSH_CHANNEL && b.receiveOnly === true && b.endpoint === b.sessionId && b.id === dshBindingId(b.sessionId)
const compareDeliveries = ([aKey, a], [bKey, b]) => (a.queueOrder ?? 0) - (b.queueOrder ?? 0) || a.reply.seq - b.reply.seq || aKey.localeCompare(bKey)
const deliveryReceipt = (d, duplicate = false) => ({
  destination: d.transport === DSH_CHANNEL ? dshDestination(d.sessionId) : d.bindingId,
  deliveryId: d.deliveryId, state: d.state, ...(d.externalMessageId ? { externalMessageId: d.externalMessageId } : {}), ...(d.bufferId ? { bufferId: d.bufferId } : {}),
  ...(d.reason ? { reason: d.reason } : {}), ...(duplicate ? { duplicate: true } : {}),
})

function linkAbort(signal, controller) {
  if (!signal) return () => {}
  const abort = () => controller.abort(signal.reason)
  if (signal.aborted) abort()
  else signal.addEventListener('abort', abort, { once: true })
  return () => signal.removeEventListener('abort', abort)
}

/** Pure service implementation. `host` owns canonical Agent/Session operations; no network or profile code lives here. */
export class IoCore {
  constructor({ store, host, config, now = Date.now, uuid = randomUUID, report = () => {} }) {
    this.contractVersion = CONTRACT_VERSION; this.store = store; this.host = host
    Object.defineProperty(this, 'config', { get: () => resolveOptions(typeof config === 'function' ? config() : config) })
    void this.config
    this.now = now; this.uuid = uuid; this.report = report; this.transports = new Map(); this.flights = new Map(); this.sessionQueues = new Map(); this.readySessions = []; this.activeSessions = 0
    this.steerCounts = new Map(); this.traceChains = new Map(); this.resumes = new Map(); this.eventChains = new Map(); this.deliveryChains = new Map(); this.controllers = new Map(); this.admissionControllers = new Map(); this.pendingReservations = new Map(); this.jobs = new Set(); this.closed = false; this.epochs = new Map()
    this.explicitFlights = new Map(); this.explicitSignals = new Map()
    this.deliveryWrites = Promise.resolve(); this.deliveryOrder = 0
    this.dshTransport = { explicitSend: true, deliver: (d, context) => this._deliverNative(d, context.signal) }
  }
  async start() {
    // A crash between persisted intent and the native/transport call is ambiguous. Never re-run that call automatically.
    const deliveries = this.store.entries('deliveries').sort(compareDeliveries)
    for (const [, d] of deliveries) this.deliveryOrder = Math.max(this.deliveryOrder, d.queueOrder ?? 0)
    for (const [key, t] of this.store.entries('traces')) {
      if (t.state === 'admitting') await this._setTrace(key, { ...t, state: 'unknown', reason: 'RESTART_DURING_ADMISSION' })
      else if (t.state === 'pending' && t.transport === DSH_CHANNEL) await this._setTrace(key, { ...t, state: 'cancelled', reason: 'RESTART_BEFORE_DSH_ADMISSION' })
      else if (t.state === 'pending' && this.config.enabled && this.store.get('bindings', t.bindingId)?.enabled) this._track(this._enqueue(t.bindingId, () => this._admit(key)))
    }
    for (const [key, d] of deliveries) {
      if (d.state === 'sending') await this._setDelivery(key, { ...d, state: 'unknown', reason: 'RESTART_DURING_DELIVERY' })
    }
    for (const [key, d] of deliveries) if (d.state === 'pending' && d.transport === DSH_CHANNEL) this._scheduleDelivery(key)
    return this
  }
  _assertOpen() { if (this.closed) throw new IoError('DISPOSED', 'IO service is disposed') }
  _track(promise) {
    this.jobs.add(promise)
    promise.then(() => this.jobs.delete(promise), error => { this.jobs.delete(promise); this.report(error) })
    return promise
  }
  async _audit(kind, id, state, detail) {
    await this.store.put('audit', identityKey(kind, id, state), { version: 1, kind, id, state, at: this.now(), ...detail })
  }
  _setTrace(key, value) { return this._updateTrace(key, () => value) }
  _updateTrace(key, transform) {
    const previous = this.traceChains.get(key) ?? Promise.resolve()
    const next = previous.then(async () => {
      const value = transform(this.store.get('traces', key))
      const record = { ...value, updatedAt: this.now() }
      await this.store.put('traces', key, record)
      await this._audit('ingress', record.traceId, record.state, { bindingId: record.bindingId, sessionId: record.sessionId, ...(record.reason ? { reason: record.reason } : {}) })
      return record
    })
    this.traceChains.set(key, next.catch(error => this.report(error)))
    return next
  }
  async _setDelivery(key, value) {
    const next = { ...value, updatedAt: this.now() }
    await this.store.put('deliveries', key, next)
    if (value.bindingId) await this._audit('delivery', value.deliveryId, value.state, { bindingId: value.bindingId, sessionId: value.sessionId, ...(value.reason ? { reason: value.reason } : {}) })
    return next
  }
  _appendDelivery(key, value) {
    // Different originating Sessions may target the same outbox. Persist its order before scheduling any of them.
    const write = this.deliveryWrites.then(async () => {
      if (this.store.entries('deliveries').length >= this.config.maxRecords) throw new IoError('CAPACITY', 'Delivery capacity reached')
      return this._setDelivery(key, { ...value, queueOrder: ++this.deliveryOrder })
    })
    this.deliveryWrites = write.catch(() => {})
    return write
  }
  registerTransport(transport) {
    this._assertOpen(); requiredString(transport.id, 'transport.id', 128)
    if (transport.id === DSH_CHANNEL || transport.channel === DSH_CHANNEL) throw new IoError('INVALID_TRANSPORT', 'The dsh channel is managed by Message Routing')
    if (typeof transport.deliver !== 'function') throw new IoError('INVALID_TRANSPORT', 'deliver is required')
    if (transport.explicitSend !== undefined && typeof transport.explicitSend !== 'boolean') throw new IoError('INVALID_TRANSPORT', 'explicitSend must be a boolean')
    if (transport.channel !== undefined) requiredString(transport.channel, 'transport.channel', 128)
    if (transport.textHint !== undefined) requiredString(transport.textHint, 'transport.textHint', 160)
    if (transport.probe !== undefined && typeof transport.probe !== 'function') throw new IoError('INVALID_TRANSPORT', 'probe must be a function')
    if (transport.outboundTargets !== undefined && (typeof transport.outboundTargets !== 'function' || transport.explicitSend !== true || transport.id.includes(OUTBOUND_TARGET))) {
      throw new IoError('INVALID_TRANSPORT', 'outboundTargets requires explicit send and a valid transport ID')
    }
    if (this.transports.has(transport.id)) throw new IoError('DUPLICATE_TRANSPORT', transport.id)
    this.transports.set(transport.id, transport)
    for (const [key] of this.store.entries('deliveries').filter(([, d]) => d.transport === transport.id && d.state === 'pending').sort(compareDeliveries)) this._scheduleDelivery(key)
    let active = true
    return () => {
      if (!active) return; active = false
      if (this.transports.get(transport.id) !== transport) return
      this.transports.delete(transport.id)
      for (const [key, item] of this.controllers) if (item.transport === transport.id) item.controller.abort(new IoError('TRANSPORT_DISPOSED', key))
    }
  }
  listBindings() { return this.store.entries('bindings').map(([, b]) => b) }
  async putBinding(input) {
    this._assertOpen(); const binding = normalizeBinding(input)
    if (binding.transport === DSH_CHANNEL || binding.id.startsWith(`${DSH_CHANNEL}:`)) throw new IoError('INVALID_BINDING', 'The dsh channel is managed by Message Routing')
    if (binding.id.includes(OUTBOUND_TARGET)) throw new IoError('INVALID_BINDING', 'Outbound target IDs are reserved for transports')
    return this._putBinding(binding)
  }
  async _putBinding(binding) {
    const previous = this.store.get('bindings', binding.id)
    if (previous && ['transport', 'endpoint', 'sessionId'].some(k => previous[k] !== binding[k]) && (this.flights.size || this.pendingReservations.get(binding.id))) throw new IoError('BINDING_BUSY', 'Do not change a destination while reception is in progress')
    if (previous && ['transport', 'endpoint', 'sessionId'].some(k => previous[k] !== binding[k]) && this.store.entries('traces').some(([, t]) => t.bindingId === binding.id)) throw new IoError('BINDING_IDENTITY_LOCKED', 'Use a new binding id to change a previously used destination')
    if (!previous && this.listBindings().length >= this.config.maxRecords) throw new IoError('CAPACITY', 'Binding capacity reached')
    await this.store.put('bindings', binding.id, binding)
    await this._audit('binding', binding.id, binding.enabled ? 'enabled' : 'disabled', { bindingId: binding.id, sessionId: binding.sessionId })
    return binding
  }
  _binding(id, enabled = false) {
    const b = this.store.get('bindings', id)
    if (!b) throw new IoError('BINDING_NOT_FOUND', id)
    if (enabled && (!b.enabled || !this.config.enabled)) throw new IoError('DISABLED', 'IO or binding is disabled')
    return b
  }
  async _resolve(sessionId) {
    let pending = this.resumes.get(sessionId)
    if (!pending) {
      pending = Promise.resolve().then(() => this.host.resolveAgent(sessionId))
      this.resumes.set(sessionId, pending)
      pending.then(() => { if (this.resumes.get(sessionId) === pending) this.resumes.delete(sessionId) }, () => { if (this.resumes.get(sessionId) === pending) this.resumes.delete(sessionId) })
    }
    return pending
  }
  async preload(bindingId) {
    this._assertOpen(); const b = this._binding(bindingId, true)
    await this._resolve(b.sessionId); this._assertOpen()
    await this._audit('preload', b.id, 'ready', { bindingId: b.id, sessionId: b.sessionId })
    return { sessionId: b.sessionId, ready: true }
  }
  async createSession(request) { this._assertOpen(); return this.host.createSession(jsonCopy(request)) }
  async preview(bindingId, options = {}) {
    this._assertOpen(); const b = this._binding(bindingId)
    return this.host.preview(b.sessionId, { limit: Math.min(this.config.historyLimit, Math.max(1, options.limit ?? this.config.historyLimit)), ...(options.beforeSeq === undefined ? {} : { beforeSeq: options.beforeSeq }), maxBytes: this.config.historyMaxBytes })
  }
  _sendEnabled() { return this.config.enabled && this.config.sendEnabled }
  _explicitTransport(b) {
    if (!b?.enabled) return undefined
    if (isDshBinding(b)) return this.dshTransport
    const transport = this.transports.get(b.transport)
    return b.sendEnabled && !b.receiveOnly && transport?.explicitSend === true ? transport : undefined
  }
  _outboundTargets(transport) {
    if (!transport?.outboundTargets) return []
    try {
      const endpoints = new Set()
      return transport.outboundTargets().map(target => {
        const endpoint = requiredString(target.endpoint, 'outbound target endpoint', 375)
        if (endpoints.has(endpoint)) throw new IoError('INVALID_TRANSPORT', 'Duplicate outbound target endpoint')
        endpoints.add(endpoint)
        return {
          id: transport.id + OUTBOUND_TARGET + endpoint, endpoint, transport: transport.id,
          label: requiredString(target.label, 'outbound target label', 160),
          enabled: true, sendEnabled: true, outboundOnly: true,
        }
      })
    } catch (error) {
      this.report(error)
      return []
    }
  }
  _outboundTarget(id) {
    const separator = id.indexOf(OUTBOUND_TARGET)
    if (separator <= 0) return undefined
    const transport = this.transports.get(id.slice(0, separator))
    return this._outboundTargets(transport).find(target => target.id === id)
  }
  async destinations({ query = '', channel }, origin, signal) {
    this._assertOpen(); signal.throwIfAborted()
    if (typeof query !== 'string' || query.length > 160) throw new IoError('INVALID_INPUT', 'query must be text up to 160 characters')
    if (channel !== undefined) requiredString(channel, 'channel', 128)
    if (!this._sendEnabled()) return { items: [], truncated: false }
    const filter = query.trim().toLowerCase()
    const items = []
    const add = (item, transport) => {
      if (filter && ![item.destination, item.label].some(value => value.toLowerCase().includes(filter))) return true
      if (transport?.textHint) item.textHint = transport.textHint
      if (items.length >= this.config.historyLimit || Buffer.byteLength(JSON.stringify({ items: [...items, item], truncated: true })) > this.config.historyMaxBytes) return false
      items.push(item)
      return true
    }
    for (const b of this.listBindings()) {
      const transport = this._explicitTransport(b)
      if (!transport || transport === this.dshTransport) continue
      const label = transport.channel ?? b.transport
      if (channel && channel !== label && channel !== b.transport) continue
      if (!add({ destination: b.id, channel: label, label: b.label }, transport)) return { items, truncated: true }
    }
    for (const transport of this.transports.values()) {
      const label = transport.channel ?? transport.id
      if (channel && channel !== label && channel !== transport.id) continue
      for (const target of this._outboundTargets(transport)) {
        if (!add({ destination: target.id, channel: label, label: target.label, outboundOnly: true }, transport)) return { items, truncated: true }
      }
    }
    if ((!channel || channel === DSH_CHANNEL) && this.host.listSessions) {
      for (const row of await this.host.listSessions(signal)) {
        if (row.sessionId === origin.reply.sessionId) continue
        if (!add({ destination: dshDestination(row.sessionId), channel: DSH_CHANNEL, label: row.label })) return { items, truncated: true }
      }
    }
    signal.throwIfAborted()
    return this._sendEnabled() ? { items, truncated: false } : { items: [], truncated: false }
  }
  async _explicitDestination(destination, originSession, signal) {
    signal.throwIfAborted()
    if (!this._sendEnabled()) return undefined
    if (destination.startsWith(`${DSH_CHANNEL}:`)) {
      const sessionId = destination.slice(DSH_CHANNEL.length + 1)
      if (sessionId === originSession || !this.host.listSessions) return undefined
      const row = (await this.host.listSessions(signal)).find(row => row.sessionId === sessionId)
      this._assertOpen(); signal.throwIfAborted()
      if (!row || !this._sendEnabled()) return undefined
      const id = dshBindingId(sessionId)
      const binding = this.store.get('bindings', id) ?? normalizeBinding({ id, transport: DSH_CHANNEL, endpoint: sessionId, sessionId, label: row.label, enabled: true, receiveOnly: true, mode: 'queue' })
      return isDshBinding(binding) && binding.enabled ? { binding, transport: this.dshTransport, channel: DSH_CHANNEL } : undefined
    }
    const binding = this.store.get('bindings', destination) ?? this._outboundTarget(destination)
    const transport = this._explicitTransport(binding)
    return transport ? { binding, transport, channel: transport.channel ?? binding.transport } : undefined
  }
  async probe({ destination }, origin, signal) {
    this._assertOpen(); requiredString(destination, 'destination', 516)
    const target = await this._explicitDestination(destination, origin.reply.sessionId, signal)
    if (!target) return { destination, available: false, reason: 'DESTINATION_UNAVAILABLE' }
    const { binding, transport, channel } = target
    if (transport === this.dshTransport) return { destination, channel, available: true }
    if (!transport.probe) return { destination, channel, available: null, reason: 'PROBE_UNAVAILABLE' }
    const controller = new AbortController()
    const unlink = linkAbort(signal, controller)
    const stillAvailable = () => {
      const current = this.store.get('bindings', binding.id) ?? this._outboundTarget(binding.id)
      return !this.closed && this._sendEnabled() && this._matchingBinding(current, binding) && this._explicitTransport(current) === transport
    }
    try {
      const value = await this._bounded(Promise.resolve().then(() => {
        controller.signal.throwIfAborted()
        if (stillAvailable()) return transport.probe(binding, { signal: controller.signal })
      }), controller, this.config.deliveryTimeoutMs, 'PROBE_TIMEOUT')
      return { destination, channel, available: stillAvailable() && value?.available === true }
    } catch (error) {
      signal.throwIfAborted()
      return { destination, channel, available: false, reason: reasonOf(error) }
    } finally { unlink() }
  }
  sendExplicit({ destination, text }, origin, signal) {
    this._assertOpen(); signal.throwIfAborted(); requiredString(destination, 'destination', 516)
    const content = validatePublicContent([{ type: 'text', text }], this.config)
    const key = digest('explicit', origin.reply.sessionId, origin.reply.seq, origin.callId)
    const checkIntent = (priorDestination, priorText) => {
      if (priorDestination !== destination || priorText !== text) throw new IoError('OUTBOUND_CALL_CONFLICT', 'This tool call already has an outbound destination and message')
    }
    const flight = this.explicitFlights.get(key)
    if (flight) {
      checkIntent(flight.destination, flight.text)
      return flight.promise.then(receipt => ({ ...receipt, duplicate: true }))
    }
    const prior = this.store.get('deliveries', key)
    if (prior) {
      const receipt = deliveryReceipt(prior, true)
      checkIntent(receipt.destination, prior.intentText ?? prior.reply.content[0]?.text)
      return Promise.resolve(receipt)
    }
    this.explicitSignals.set(key, signal)
    const promise = this._sendExplicit(key, destination, { ...origin.reply, content }, signal).finally(() => {
      this.explicitFlights.delete(key); this.explicitSignals.delete(key)
    })
    this.explicitFlights.set(key, { destination, text, promise })
    return promise
  }
  async _sendExplicit(key, destination, reply, signal) {
    const target = await this._explicitDestination(destination, reply.sessionId, signal)
    if (!target) throw new IoError('DESTINATION_UNAVAILABLE', 'Choose an enabled destination from io_destinations')
    this._assertOpen(); signal.throwIfAborted()
    const { binding } = target
    if (isDshBinding(binding) && !this.store.get('bindings', binding.id)) await this._putBinding(binding)
    this._assertOpen(); signal.throwIfAborted()
    await this._appendDelivery(key, { version: 1, intent: 'explicit', intentText: reply.content[0]?.text, deliveryId: key, bindingId: binding.id, sessionId: binding.sessionId ?? reply.sessionId, transport: binding.transport, endpoint: binding.endpoint, reply, traceIds: [], state: 'pending', createdAt: this.now() })
    await this._scheduleDelivery(key)
    const row = this.store.get('deliveries', key)
    return deliveryReceipt(this.closed && row.state === 'pending' ? await this._setDelivery(key, { ...row, state: 'cancelled', reason: 'DISPOSED' }) : row)
  }
  async _deliverNative(delivery, signal) {
    const { reply } = delivery
    const rows = await this.host.listSessions(signal)
    signal.throwIfAborted()
    if (!rows.some(row => row.sessionId === delivery.endpoint)) return { state: 'failed', reason: 'DSH_SESSION_UNAVAILABLE' }
    const sender = rows.find(row => row.sessionId === reply.sessionId)
    const actor = sender?.label && sender.label !== reply.sessionId ? sender.label : 'DSH'
    const replySource = { actor, sessionId: reply.sessionId, provider: reply.source.provider, model: reply.source.model, messageId: reply.messageId, seq: reply.seq, turn: reply.turn, step: reply.step }
    const receipt = await this._receiveFromBinding({ bindingId: delivery.bindingId, externalMessageId: delivery.deliveryId, content: reply.content, replySource, mode: 'queue' }, signal)
    if (receipt.state === 'accepted') return { state: 'delivered', externalMessageId: receipt.messageId }
    return { state: ['failed', 'cancelled'].includes(receipt.state) ? 'failed' : 'unknown', reason: receipt.reason ?? 'DSH_ADMISSION_' + receipt.state.toUpperCase() }
  }
  receive(input) {
    if (typeof input?.bindingId === 'string' && input.bindingId.startsWith(`${DSH_CHANNEL}:`)) throw new IoError('INVALID_BINDING', 'The dsh channel is managed by Message Routing')
    return this._receiveFromBinding(input)
  }
  _receiveFromBinding(input, signal) {
    this._assertOpen(); signal?.throwIfAborted(); const ingress = normalizeIngress(input, this.config); const b = this._binding(ingress.bindingId, true)
    const key = digest(b.id, b.transport, b.endpoint, ingress.externalMessageId)
    const flight = this.flights.get(key)
    if (flight) return flight.then(receipt => ({ ...receipt, duplicate: true }))
    const prior = this.store.get('traces', key)
    if (prior && (prior.state !== 'failed' || input.retryFailed !== true)) return Promise.resolve({ ...receiptOf(prior), duplicate: true })
    const pending = prior ? this._retryFailed(key, b, signal) : this._receive(key, b, ingress, signal)
    this.flights.set(key, pending)
    pending.then(() => this.flights.delete(key), () => this.flights.delete(key))
    return pending
  }
  async _retryFailed(key, binding, signal) {
    // A failed trace is written only before the native inbox was invoked. Keep
    // its original message identity and content when a channel retries it.
    const count = this.store.entries('traces').filter(([, row]) => row.bindingId === binding.id && (row.state === 'pending' || row.state === 'admitting')).length
    const reserved = this.pendingReservations.get(binding.id) ?? 0
    if (count + reserved >= this.config.maxPendingPerBinding) throw new IoError('BACKPRESSURE', 'Binding queue is full')
    signal?.throwIfAborted()
    const prior = this.store.get('traces', key)
    if (!prior || prior.state !== 'failed') return prior ? { ...receiptOf(prior), duplicate: true } : undefined
    const { reason, ...trace } = prior
    this.pendingReservations.set(binding.id, reserved + 1)
    try { await this._setTrace(key, { ...trace, state: 'pending' }) }
    finally { this.pendingReservations.set(binding.id, (this.pendingReservations.get(binding.id) ?? 1) - 1) }
    try { return await this._enqueue(binding.id, () => this._admit(key, signal)) }
    catch (error) {
      const current = this.store.get('traces', key)
      if (current?.state === 'pending') await this._setTrace(key, { ...current, state: 'failed', reason: reasonOf(error) })
      throw error
    }
  }
  async _receive(key, binding, ingress, signal) {
    const reservations = [...this.pendingReservations.values()].reduce((sum, n) => sum + n, 0)
    if (this.store.entries('traces').length + reservations >= this.config.maxRecords) throw new IoError('CAPACITY', 'Trace capacity reached; dedupe records are not silently evicted')
    const reserved = this.pendingReservations.get(binding.id) ?? 0
    const count = this.store.entries('traces').filter(([, t]) => t.bindingId === binding.id && (t.state === 'pending' || t.state === 'admitting')).length
    if (count + reserved >= this.config.maxPendingPerBinding) throw new IoError('BACKPRESSURE', 'Binding queue is full')
    this.pendingReservations.set(binding.id, reserved + 1)
    const epoch = this.epochs.get(binding.sessionId) ?? 0
    let reservedActive = true
    const releaseReservation = () => {
      if (!reservedActive) return
      reservedActive = false; this.pendingReservations.set(binding.id, (this.pendingReservations.get(binding.id) ?? 1) - 1)
    }
    try {
      const requestedMode = ingress.mode ?? binding.mode
      const trace = { version: 1, key, traceId: this.uuid(), messageId: this.uuid(), bindingId: binding.id, sessionId: binding.sessionId, transport: binding.transport, endpoint: binding.endpoint, externalMessageId: ingress.externalMessageId, content: ingress.content, requestedMode, mode: 'queue', state: 'pending', messageCommitted: false, durable: false, createdAt: this.now(), ...(ingress.replyTo ? { replyTo: ingress.replyTo } : {}), ...(ingress.senderId ? { senderId: ingress.senderId } : {}), ...(ingress.replySource ? { replySource: ingress.replySource } : {}) }
      await this._setTrace(key, trace)
      releaseReservation()
      if (epoch !== (this.epochs.get(binding.sessionId) ?? 0) || this.closed || signal?.aborted) return receiptOf(await this._setTrace(key, { ...trace, state: 'cancelled', reason: 'CANCELLED_DURING_RECEPTION' }))
      return await this._enqueue(binding.id, () => this._admit(key, signal))
    } finally { releaseReservation() }
  }
  _enqueue(bindingId, action) {
    if (this.closed) return Promise.reject(new IoError('DISPOSED', 'IO service is disposed'))
    const binding = this._binding(bindingId)
    let queue = this.sessionQueues.get(binding.sessionId)
    if (!queue) { queue = { buckets: new Map(), order: [], running: false }; this.sessionQueues.set(binding.sessionId, queue) }
    let bucket = queue.buckets.get(bindingId)
    if (!bucket) { bucket = []; queue.buckets.set(bindingId, bucket); queue.order.push(bindingId) }
    const promise = new Promise((resolve, reject) => bucket.push({ action, resolve, reject }))
    if (!queue.running && !this.readySessions.includes(binding.sessionId)) this.readySessions.push(binding.sessionId)
    this._pump()
    return promise
  }
  _pump() {
    while (!this.closed && this.activeSessions < this.config.maxConcurrentSessions && this.readySessions.length) {
      const sessionId = this.readySessions.shift(); const queue = this.sessionQueues.get(sessionId)
      if (!queue || queue.running || !queue.order.length) continue
      queue.running = true; this.activeSessions++
      const bindingId = queue.order.shift(); const bucket = queue.buckets.get(bindingId); const job = bucket.shift()
      if (bucket.length) queue.order.push(bindingId); else queue.buckets.delete(bindingId)
      this._track(Promise.resolve().then(job.action).then(job.resolve, job.reject).finally(() => {
        this.activeSessions--; queue.running = false
        if (queue.order.length) this.readySessions.push(sessionId); else this.sessionQueues.delete(sessionId)
        this._pump()
      }))
    }
  }
  async _admit(key, signal) {
    let t = this.store.get('traces', key)
    if (!t || t.state !== 'pending') return t ? receiptOf(t) : undefined
    const controller = new AbortController(); this.admissionControllers.set(key, { controller, sessionId: t.sessionId })
    const unlink = linkAbort(signal, controller)
    const epoch = this.epochs.get(t.sessionId) ?? 0
    const checkAdmission = () => {
      this._assertOpen(); controller.signal.throwIfAborted()
      const binding = this._binding(t.bindingId, true)
      if (t.transport === DSH_CHANNEL && (!this.config.sendEnabled || !isDshBinding(binding) || this.host.isSessionArchived?.(t.sessionId))) throw new IoError('DELIVERY_DISABLED', 'Native delivery is disabled')
    }
    let sendAttempted = false
    try {
      checkAdmission()
      const agent = await this._bounded(this._resolve(t.sessionId), controller, this.config.admissionTimeoutMs, 'ADMISSION_TIMEOUT')
      checkAdmission()
      t = this.store.get('traces', key)
      if (t.state !== 'pending') return receiptOf(t)
      const busy = agent.status === 'running'
      const count = busy ? (this.steerCounts.get(t.sessionId) ?? 0) : 0
      let mode = t.requestedMode === 'queue' ? 'queue' : busy ? 'steer' : 'queue'
      // An unlimited stream of external steering must not keep existing queued turns from ever running.
      if (mode === 'steer' && (count >= this.config.maxConsecutiveSteers || agent.inbox?.nextTurn?.length)) mode = 'queue'
      this.steerCounts.set(t.sessionId, mode === 'steer' ? count + 1 : count)
      t = await this._setTrace(key, { ...t, state: 'admitting', mode })
      const source = { kind: SOURCE_KIND, version: 1, transport: t.transport, bindingId: t.bindingId, endpoint: t.endpoint, traceId: t.traceId, externalMessageId: t.externalMessageId, inputMode: mode, ...(t.replyTo ? { replyTo: t.replyTo } : {}), ...(t.senderId ? { senderId: t.senderId } : {}), ...(t.replySource ? { replySource: t.replySource } : {}) }
      const message = { id: t.messageId, role: 'user', content: t.content, source }
      // Validate durable attachments and the exact live Agent BEFORE the non-retryable native send.
      if (this.host.validateAdmission) await this._bounded(this.host.validateAdmission(agent, message), controller, this.config.admissionTimeoutMs, 'ADMISSION_TIMEOUT')
      checkAdmission()
      if (epoch !== (this.epochs.get(t.sessionId) ?? 0) || this.store.get('traces', key).state === 'cancelled') return receiptOf(this.store.get('traces', key))
      sendAttempted = true
      if (this.host.send) this.host.send(agent, message, mode)
      else if (mode === 'steer') agent.steer(message); else agent.followup(message)
      t = await this._updateTrace(key, current => ({ ...current, state: 'accepted' }))
      // Native return acknowledges inbox admission, not a user/message seq or a durability barrier.
      return receiptOf(t)
    } catch (error) {
      const current = this.store.get('traces', key) ?? t
      if (current?.state === 'accepted' || current?.state === 'cancelled') return receiptOf(current)
      // Once native send could have happened, only reconciliation against canonical history may recover the outcome.
      const state = sendAttempted ? 'unknown' : signal?.aborted ? 'cancelled' : 'failed'
      const failed = await this._setTrace(key, { ...current, state, reason: reasonOf(error) })
      return receiptOf(failed)
    } finally { unlink(); this.admissionControllers.delete(key) }
  }
  async _bounded(promise, controller, milliseconds, code) {
    controller.signal.throwIfAborted()
    let timer; let onAbort
    try {
      const interrupted = new Promise((_, reject) => {
        onAbort = () => reject(controller.signal.reason)
        controller.signal.addEventListener('abort', onAbort, { once: true })
        timer = setTimeout(() => controller.abort(new IoError(code, code)), milliseconds)
      })
      return await Promise.race([promise, interrupted])
    } finally { clearTimeout(timer); controller.signal.removeEventListener('abort', onAbort) }
  }
  receipt(traceId) { const t = this.store.entries('traces').find(([, value]) => value.traceId === traceId)?.[1]; return t ? receiptOf(t) : undefined }
  observe(sessionId, event) {
    if (this.closed || !this.listBindings().some(b => b.sessionId === sessionId)) return Promise.resolve()
    const previous = this.eventChains.get(sessionId) ?? Promise.resolve()
    const next = previous.then(() => this._observe(sessionId, event))
    this.eventChains.set(sessionId, next.catch(error => this.report(error)))
    return this._track(next)
  }
  async _observe(sessionId, event) {
    let cursor = this.store.get('cursors', sessionId) ?? initialCursor(sessionId)
    if (event.seq <= cursor.lastSeq) return
    if (event.seq !== cursor.lastSeq + 1) {
      if (!this.host.eventsThrough) throw new IoError('EVENT_GAP', 'Canonical replay is required before output can be routed')
      const events = await this.host.eventsThrough(sessionId, cursor.lastSeq + 1, event.seq - 1)
      // Recover routing and inbound receipts from history before handling the live event.
      for (const item of events) cursor = await this._fold(sessionId, item, cursor, { replay: true })
      await this.store.put('cursors', sessionId, cursor)
    }
    cursor = await this._fold(sessionId, event, cursor)
    await this.store.put('cursors', sessionId, cursor)
  }
  async _fold(sessionId, event, previous, { replay = false } = {}) {
    let routingEvent = event
    if (event.type === 'user/message' && event.data.source?.kind === CONTROL_SOURCE_KIND) {
      const source = event.data.source
      const control = this.store.get('audit', identityKey('continuation', source.nonce, 'prepared'))
      if (!control || control.sessionId !== sessionId || control.bindingId !== source.bindingId || control.messageId !== event.data.id || control.turn !== source.turn) routingEvent = { ...event, data: { ...event.data, source: { kind: 'untrusted-io-control' } } }
    }
    const result = foldRouting(previous, routingEvent)
    if (result.gap) throw new IoError('EVENT_GAP', 'Session event gap')
    if (result.duplicate) return previous
    if (result.controlOnly) {
      const bindingId = result.cursor.bindings[0]
      if (bindingId) await this._audit('continuation', result.cursor.controlNonce, 'stopped', { sessionId, bindingId, turn: event.data.turn, nonce: result.cursor.controlNonce })
    }
    if (event.type === 'turn/start') this.steerCounts.delete(sessionId)
    if (event.type === 'user/message' && event.surfaceOp === 'append' && event.data.source?.kind === SOURCE_KIND) {
      const message = event.data; const source = message.source
      const match = this.store.entries('traces').find(([, t]) => t.sessionId === sessionId && t.bindingId === source.bindingId && t.traceId === source.traceId && t.messageId === message.id)
      if (match) {
        const [key] = match
        await this._updateTrace(key, current => ({ ...current, state: 'accepted', messageCommitted: true, acceptedSeq: event.seq, durable: false }))
        // This barrier is started strictly after the exact user/message commit, never after inbox acceptance alone.
        if (this.host.flush) this._track(Promise.resolve().then(() => this.host.flush({ id: sessionId })).then(async () => {
          await this._updateTrace(key, current => current.acceptedSeq === event.seq ? { ...current, durable: true } : current)
        }, async error => { await this._audit('durability', source.traceId, 'failed', { bindingId: source.bindingId, sessionId, reason: reasonOf(error) }) }))
      }
    }
    if (!replay && result.reply) {
      const bindingId = result.cursor.bindings.length === 1 ? result.cursor.bindings[0] : ''
      const b = bindingId ? this.store.get('bindings', bindingId) : undefined
      const key = digest(sessionId, event.seq, result.reply.messageId, bindingId)
      // Inbound-only transport bindings retain the canonical Session reply
      // without occupying the outbound queue or its record budget.
      if (b?.receiveOnly !== true && !this.store.get('deliveries', key)) {
        const traceIds = result.cursor.traces
        const traces = this.store.entries('traces').map(([, t]) => t).filter(t => traceIds.includes(t.traceId))
        const safe = result.routable && b?.enabled && traces.length === traceIds.length && traces.every(t => t.sessionId === sessionId && t.bindingId === bindingId)
        const d = { version: 1, deliveryId: key, bindingId, sessionId, transport: b?.transport ?? '', endpoint: b?.endpoint ?? '', reply: result.reply, traceIds, state: safe ? 'pending' : 'suppressed', createdAt: this.now(), ...(traces.length === 1 ? { replyTo: traces[0].externalMessageId } : {}), ...(!safe ? { reason: 'AMBIGUOUS_OR_UNTRUSTED_TURN' } : {}) }
        await this._appendDelivery(key, d)
        if (safe) this._scheduleDelivery(key)
      }
    }
    return result.cursor
  }
  _scheduleDelivery(key) {
    const d = this.store.get('deliveries', key)
    if (!d || d.state !== 'pending' || this.closed) return
    const previous = this.deliveryChains.get(d.bindingId) ?? Promise.resolve()
    const next = previous.then(async () => {
      // A pending predecessor remains an ordering fence while send is disabled or a transport is absent.
      const earlier = this.store.entries('deliveries').filter(entry => entry[1].bindingId === d.bindingId && entry[1].state === 'pending' && compareDeliveries(entry, [key, d]) < 0).sort(compareDeliveries)
      for (const [prior] of earlier) {
        await this._deliver(prior)
        if (this.store.get('deliveries', prior)?.state !== 'pending') continue
        const current = this.store.get('deliveries', key)
        if (current?.intent === 'explicit' && current.state === 'pending' && !this.closed) {
          const cancelled = this.explicitSignals.get(key)?.aborted
          await this._setDelivery(key, { ...current, state: cancelled ? 'cancelled' : 'suppressed', reason: cancelled ? 'CALL_CANCELLED' : 'OUTBOX_BLOCKED' })
        }
        return
      }
      await this._deliver(key)
    })
    this.deliveryChains.set(d.bindingId, next.catch(error => this.report(error)))
    return this._track(next)
  }
  _matchingBinding(b, d) {
    return !!b && b.transport === d.transport && b.endpoint === d.endpoint
      && (b.outboundOnly ? d.intent === 'explicit' || d.outboundOnly === true : b.sessionId === d.sessionId)
  }
  _deliveryBinding(delivery) {
    return this.store.get('bindings', delivery.bindingId)
      ?? (delivery.intent === 'explicit' ? this._outboundTarget(delivery.bindingId) : undefined)
  }
  _deliveryTransport(b, d) {
    if (!this._sendEnabled() || !b?.enabled) return undefined
    return d.intent === 'explicit' ? this._explicitTransport(b) : b.sendEnabled && !b.receiveOnly ? this.transports.get(d.transport) : undefined
  }
  async _deliver(key) {
    let d = this.store.get('deliveries', key)
    if (!d || d.state !== 'pending' || this.closed) return
    const b = this._deliveryBinding(d)
    const explicit = d.intent === 'explicit'
    const signal = this.explicitSignals.get(key)
    if (signal?.aborted) { await this._setDelivery(key, { ...d, state: 'cancelled', reason: 'CALL_CANCELLED' }); return }
    if (b?.receiveOnly === true && !(explicit && isDshBinding(b))) { await this._setDelivery(key, { ...d, state: 'suppressed', reason: 'BINDING_RECEIVE_ONLY' }); return }
    if (b && !this._matchingBinding(b, d)) { await this._setDelivery(key, { ...d, state: 'suppressed', reason: 'BINDING_IDENTITY_CHANGED' }); return }
    const transport = this._deliveryTransport(b, d)
    if (!transport) {
      if (explicit) await this._setDelivery(key, { ...d, state: 'suppressed', reason: 'DELIVERY_DISABLED' })
      return
    }
    const controller = new AbortController(); this.controllers.set(key, { controller, transport: d.transport })
    const unlink = linkAbort(signal, controller)
    let attempted = false
    try {
      d = await this._setDelivery(key, { ...d, state: 'sending' })
      controller.signal.throwIfAborted()
      const value = await this._bounded(Promise.resolve().then(() => {
        controller.signal.throwIfAborted()
        const currentBinding = this._deliveryBinding(d)
        if (this.closed || !this._matchingBinding(currentBinding, d) || this._deliveryTransport(currentBinding, d) !== transport || this.store.get('deliveries', key)?.state !== 'sending') throw new IoError('DELIVERY_DISABLED', 'Delivery no longer authorized')
        const delivery = jsonCopy({ deliveryId: d.deliveryId, bindingId: d.bindingId, endpoint: d.endpoint, reply: d.reply, traceIds: d.traceIds, ...(d.replyTo ? { replyTo: d.replyTo } : {}), ...(explicit ? { intent: 'explicit' } : {}) })
        attempted = true
        return transport.deliver(delivery, { signal: controller.signal, idempotencyKey: key })
      }), controller, this.config.deliveryTimeoutMs, 'DELIVERY_TIMEOUT')
      if (!value || !['buffered', 'delivered', 'failed', 'unknown'].includes(value.state) || (value.state === 'delivered' && (typeof value.externalMessageId !== 'string' || !value.externalMessageId)) || (value.state === 'buffered' && (typeof value.bufferId !== 'string' || !value.bufferId))) throw new IoError('INVALID_RECEIPT', 'Transport returned no authoritative receipt')
      const current = this.store.get('deliveries', key)
      if (current.state !== 'sending') return
      await this._setDelivery(key, { ...d, state: value.state, ...(value.state === 'delivered' ? { externalMessageId: value.externalMessageId } : value.state === 'buffered' ? { bufferId: value.bufferId } : { reason: String(value.reason ?? 'TRANSPORT_FAILURE').slice(0, 160) }) })
    } catch (error) {
      const current = this.store.get('deliveries', key)
      if (current?.state === 'sending') {
        const state = !explicit || attempted ? 'unknown' : controller.signal.aborted ? 'cancelled' : 'suppressed'
        await this._setDelivery(key, { ...d, state, reason: reasonOf(error) })
      }
    } finally { unlink(); this.controllers.delete(key) }
  }
  async cancel(bindingId, options = {}) {
    this._assertOpen(); const b = this._binding(bindingId); this.epochs.set(b.sessionId, (this.epochs.get(b.sessionId) ?? 0) + 1)
    for (const item of this.admissionControllers.values()) if (item.sessionId === b.sessionId) item.controller.abort(new IoError('USER_CANCELLED', 'Cancelled'))
    // Cancel pending native work only by explicit trusted Host request, never from a history tool.
    const agent = this.host.liveAgent?.(b.sessionId)
    if (agent) agent.cancel({ kind: 'user' }, { keepInbox: options.keepInbox !== false })
    for (const [key, t] of this.store.entries('traces')) if (t.sessionId === b.sessionId && (t.state === 'pending' || t.state === 'admitting')) await this._setTrace(key, { ...t, state: 'cancelled', reason: 'USER_CANCELLED' })
    for (const [key, d] of this.store.entries('deliveries')) if (d.sessionId === b.sessionId && d.state === 'pending') await this._setDelivery(key, { ...d, state: 'cancelled', reason: 'USER_CANCELLED' })
    for (const [key, item] of this.controllers) { const d = this.store.get('deliveries', key); if (d?.sessionId === b.sessionId) item.controller.abort(new IoError('USER_CANCELLED', 'Cancelled')) }
    return { requested: Boolean(agent), sessionId: b.sessionId }
  }
  _scope(bindingId, scope) {
    const b = this._binding(bindingId)
    if (!scope || scope.sessionId !== b.sessionId) throw new IoError('HISTORY_FORBIDDEN', 'History is limited to bindings of the calling Session')
    return b
  }
  async history(request, scope) {
    this._assertOpen(); this._scope(request.bindingId, scope)
    const kind = request.kind ?? 'all'
    if (!['all', 'ingress', 'delivery', 'audit'].includes(kind)) throw new IoError('INVALID_INPUT', 'Invalid history kind')
    const limit = Math.min(this.config.historyLimit, Math.max(1, request.limit ?? this.config.historyLimit))
    if (!Number.isSafeInteger(limit)) throw new IoError('INVALID_INPUT', 'limit must be an integer')
    let after = ''
    if (request.cursor) {
      try { const parsed = JSON.parse(Buffer.from(request.cursor, 'base64url').toString('utf8')); if (parsed.bindingId !== request.bindingId || parsed.kind !== kind || typeof parsed.after !== 'string') throw new Error(); after = parsed.after }
      catch (error) { throw new IoError('INVALID_CURSOR', 'Cursor belongs to another history query') }
    }
    const items = []
    for (const table of kind === 'all' ? ['traces', 'deliveries', 'audit'] : [kind === 'ingress' ? 'traces' : kind === 'delivery' ? 'deliveries' : 'audit']) for (const [key, value] of this.store.entries(table)) {
      if (value.bindingId !== request.bindingId) continue
      const order = `${String(value.createdAt ?? value.at ?? 0).padStart(16, '0')}:${table}:${key}`
      if (order <= after) continue
      items.push({ order, value: summarizeStored(table, value) })
    }
    items.sort((a, b) => a.order.localeCompare(b.order))
    const result = []; let last = after
    for (const item of items.slice(0, limit)) {
      const candidate = { items: [...result, item.value], truncated: true, nextCursor: historyCursor(request.bindingId, kind, item.order) }
      if (Buffer.byteLength(JSON.stringify(candidate)) > this.config.historyMaxBytes) break
      result.push(item.value); last = item.order
    }
    if (!result.length && items.length) throw new IoError('OUTPUT_TOO_LARGE', 'History byte limit cannot fit one bounded item')
    const more = result.length < items.length
    return { items: result, ...(more ? { nextCursor: historyCursor(request.bindingId, kind, last) } : {}), truncated: more }
  }
  async historyRead(request, scope) {
    this._assertOpen(); const b = this._scope(request.bindingId, scope)
    if (!Number.isSafeInteger(request.seq) || request.seq < 0) throw new IoError('INVALID_INPUT', 'seq must be a nonnegative safe integer')
    return this.host.readEvent(b.sessionId, request.seq, { before: Math.min(5, Math.max(0, request.before ?? 2)), after: Math.min(5, Math.max(0, request.after ?? 2)), maxBytes: this.config.historyMaxBytes })
  }
  /** Synchronize public output already committed by this live Session before an adapter reads its buffer. */
  async flushOutbound(sessionId) {
    this._assertOpen()
    const bindings = this.listBindings().filter(binding => binding.sessionId === sessionId)
    if (!bindings.length) return
    const agent = this.host.liveAgent?.(sessionId)
    if (!agent?.session?.snapshotEvents) throw new IoError('SESSION_UNAVAILABLE', 'Live Session is required')
    await this.eventChains.get(sessionId)
    const events = agent.session.snapshotEvents()
    let last = this.store.get('cursors', sessionId)?.lastSeq ?? -1
    for (const event of events) if (event.seq > last) { await this.observe(sessionId, event); last = event.seq }
    for (const binding of bindings) await this.deliveryChains.get(binding.id)
    if (this.closed) throw new IoError('DISPOSED', 'Message Routing was stopped')
  }
  /** Adapter reconciliation for a previously buffered delivery; this never schedules a send. */
  async updateBuffered(bufferId, update) {
    this._assertOpen()
    const found = this.store.entries('deliveries').find(([, row]) => row.bufferId === bufferId)
    if (!found) return false
    const [key, row] = found
    if (!['buffered', 'sending', 'failed'].includes(row.state)) return false
    if (!update || !['content', 'sending', 'delivered', 'failed', 'unknown', 'cleared'].includes(update.state)) throw new IoError('INVALID_INPUT', 'Invalid buffered update')
    const next = { ...row }
    delete next.reason; delete next.externalMessageId
    if (update.state === 'content') {
      next.state = 'buffered'; next.reply = { ...next.reply, content: validatePublicContent(update.content) }
    } else if (update.state === 'cleared') { next.state = 'suppressed'; next.reason = 'BUFFER_CLEARED' }
    else {
      next.state = update.state
      if (update.state === 'delivered') next.externalMessageId = requiredString(update.externalMessageId, 'externalMessageId')
      else if (update.reason) next.reason = String(update.reason).slice(0, 160)
    }
    await this._setDelivery(key, next)
    return true
  }
  async drain() {
    if (!this.closed && this.config.enabled) for (const [key, t] of this.store.entries('traces')) if (t.state === 'pending' && t.transport !== DSH_CHANNEL && this.store.get('bindings', t.bindingId)?.enabled) this._track(this._enqueue(t.bindingId, () => this._admit(key)))
    for (const [key] of this.store.entries('deliveries').filter(([, d]) => d.state === 'pending').sort(compareDeliveries)) this._scheduleDelivery(key)
    for (;;) {
      const jobs = [...this.jobs]
      if (!jobs.length) return
      await Promise.allSettled(jobs)
    }
  }
  async close() {
    if (this.closed) return; this.closed = true
    for (const queue of this.sessionQueues.values()) for (const bucket of queue.buckets.values()) for (const job of bucket) job.reject(new IoError('DISPOSED', 'Service disposed'))
    this.readySessions = []; this.transports.clear()
    for (const { controller } of this.controllers.values()) controller.abort(new IoError('DISPOSED', 'Service disposed'))
    for (const { controller } of this.admissionControllers.values()) controller.abort(new IoError('DISPOSED', 'Service disposed'))
    await this.drain(); await Promise.allSettled([...this.explicitFlights.values()].map(flight => flight.promise)); await Promise.allSettled([...this.flights.values()]); await this.store.close()
  }
}
function historyCursor(bindingId, kind, after) { return Buffer.from(JSON.stringify({ bindingId, kind, after })).toString('base64url') }
function summarizeStored(table, value) {
  if (table === 'audit') return value
  const base = { kind: table === 'traces' ? 'ingress' : 'delivery', id: value.traceId ?? value.deliveryId, bindingId: value.bindingId, sessionId: value.sessionId, state: value.state, time: value.createdAt, ...(value.reason ? { reason: value.reason } : {}) }
  const blocks = value.content ?? value.reply?.content ?? []
  const text = blocks.filter(b => b.type === 'text').map(b => b.text).join('\n')
  const anchor = value.reply ? { source: value.reply.source, ...(value.intent === 'explicit' ? { intent: 'explicit', originSessionId: value.reply.sessionId } : { seq: value.reply.seq }) } : value.acceptedSeq === undefined ? { messageCommitted: false, durable: value.durable } : { seq: value.acceptedSeq, messageCommitted: true, durable: value.durable }
  return { ...base, ...anchor, messageId: value.messageId ?? value.reply?.messageId, text: text.slice(0, 1024), textTruncated: text.length > 1024, media: blocks.filter(b => b.type === 'image' || b.type === 'file').slice(0, 10), ...(value.externalMessageId ? { externalMessageId: value.externalMessageId } : {}), ...(value.bufferId ? { bufferId: value.bufferId } : {}), transport: value.transport, endpoint: value.endpoint, ...(value.replyTo ? { replyTo: value.replyTo } : {}), ...(value.traceIds ? { traceIds: value.traceIds } : {}), ...(value.senderId ? { senderId: value.senderId } : {}) }
}
