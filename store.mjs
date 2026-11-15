import { createHash } from 'node:crypto'
import { TABLE_NAMES, IoError, identityKey, jsonCopy } from './contract.mjs'

const encodedKey = /^dshk_[a-f0-9]{64}$/i
const windowsDevice = /^(?:con|prn|aux|nul|com[0-9]|lpt[0-9])$/i
/** Keep existing file-safe keys and give compound identifiers bounded filenames. */
function diskKey(key) {
  if (/^[a-zA-Z0-9_-]{1,128}$/.test(key) && !encodedKey.test(key) && !windowsDevice.test(key)) return key
  return 'dshk_' + createHash('sha256').update(JSON.stringify(key)).digest('hex')
}
const logicalKey = {
  bindings: value => value.id,
  traces: value => value.key,
  deliveries: value => value.deliveryId,
  cursors: value => value.sessionId,
  audit: value => identityKey(value.kind, value.id, value.state),
}

/** Deterministic test store; production uses DomainStore and never silently falls back to memory. */
export class MemoryStore {
  constructor(seed = {}) { this.tables = new Map(TABLE_NAMES.map(name => [name, new Map(Object.entries(jsonCopy(seed[name] ?? {})))])) }
  get(table, key) { const value = this.tables.get(table)?.get(key); return value === undefined ? undefined : jsonCopy(value) }
  entries(table) { return [...this.tables.get(table).entries()].map(([key, value]) => [key, jsonCopy(value)]) }
  async put(table, key, value) { if (!this.tables.has(table)) throw new IoError('UNKNOWN_TABLE', table); this.tables.get(table).set(key, jsonCopy(value)) }
  async close() {}
  snapshot() { return jsonCopy(Object.fromEntries([...this.tables].map(([name, values]) => [name, Object.fromEntries(values)]))) }
}
/** storageDomain handle adapter. Every write resolves only after the backend accepts it. */
export class DomainStore {
  constructor(domain, spec) { this.domain = domain; this.spec = spec }
  get(table, key) {
    const value = this.domain.table(table).get(diskKey(key))
    return value === undefined ? undefined : jsonCopy(value)
  }
  entries(table) {
    return [...this.domain.table(table).entries()].map(([key, value]) => [
      encodedKey.test(key) ? logicalKey[table](value) : key, jsonCopy(value),
    ])
  }
  put(table, key, value) {
    const schema = this.spec?.tables[table]?.valueSchema
    return this.domain.table(table).put(diskKey(key), schema ? schema.parse(jsonCopy(value)) : jsonCopy(value))
  }
  close() { return this.domain.close() }
}
