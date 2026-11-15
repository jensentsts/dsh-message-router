import test from 'node:test'
import assert from 'node:assert/strict'
import vm from 'node:vm'
import { readFileSync } from 'node:fs'
import React from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { z } from 'zod'

const en = JSON.parse(readFileSync(new URL('../locale/en.json', import.meta.url), 'utf8')).monitor
function artifact() {
  let plugin
  const window = { __ModuleLoader__: { load(row) { assert.equal(row.id, 'dsh-super-agent-io'); plugin = row.factory(name => { if (name === 'react') return React; if (name === 'zod') return { z }; throw new Error(`Unexpected browser runtime dependency ${name}`) }) } } }
  vm.runInNewContext(readFileSync(new URL('../client.js', import.meta.url), 'utf8'), { window, TextEncoder, URL, structuredClone, Buffer, console })
  return plugin
}
async function clientFixture() {
  const disposers = []; const slots = []; const protocols = []; const calls = []; let mounted
  const remote = { query: async request => { calls.push(request); return { ok: true, value: { action: 'bindings', items: [] } } } }
  const ctx = { remote: { async $mount(contribution) { mounted = contribution } }, async inject(keys, apply) { assert.deepEqual(Array.from(keys), ['remote.superAgentIoMonitor']); return apply(inner) } }
  const inner = { 'remote.superAgentIoMonitor': remote, effect(effect) { const dispose = effect(); if (typeof dispose === 'function') disposers.push(dispose) }, locale: { register(ns, dictionaries) { assert.equal(ns, 'independent.superAgentIo'); assert.ok(dictionaries.zh.nav); return () => {} }, bind() { return key => en[key] ?? key } }, resources: { register(provider) { protocols.push(provider); return () => protocols.splice(protocols.indexOf(provider), 1) } }, slots: { inject(name, callback) { assert.equal(name, 'settings.section'); disposers.push(callback()) }, register(options, Component) { slots.push({ options, Component }); return () => slots.pop() } } }
  const plugin = artifact(); await plugin.apply(ctx)
  return { slots, protocols, calls, mounted: () => mounted, dispose() { for (const fn of disposers.reverse()) fn() } }
}
test('browser artifact mounts only read query RPC and an owned Settings useResource view', async () => {
  const f = await clientFixture()
  assert.equal(f.slots.length, 1); assert.equal(f.slots[0].options.name, 'settings.section'); assert.equal(f.slots[0].options.order, 53); assert.equal(f.slots[0].options.label(), en.nav)
  assert.equal(f.mounted().descriptors.length, 1); assert.equal(f.mounted().descriptors[0].method, 'query')
  const provider = f.protocols[0]; const frames = []; const signal = new AbortController().signal
  for await (const frame of provider.open('dsh-resource://super-agent-io-monitor/%7B%22action%22%3A%22bindings%22%7D?r=1', { signal })) frames.push(frame)
  assert.equal(f.calls.length, 1); assert.equal(f.calls[0].action, 'bindings'); assert.equal(frames[0].ok, true)
  f.dispose(); assert.equal(f.slots.length, 0); assert.equal(f.protocols.length, 0)
})
test('monitor renders transport direction and receipt stages with escaped text, not fake Chat rows', async () => {
  const f = await clientFixture(); const Component = f.slots[0].Component; const resources = []
  const binding = { id: 'binding', label: 'QQ channel', transport: 'qq', endpoint: 'group:123', sessionId: 'native-session', mode: 'auto', inputs: { accepted: 1 }, outputs: { unknown: 1 } }
  const html = renderToStaticMarkup(React.createElement(Component, { t: key => en[key] ?? key, useResource(address) { resources.push(address); const request = address ? JSON.parse(decodeURIComponent(new URL(address).pathname.slice(1))) : null; return { status: 'live', value: { ok: true, value: request?.action === 'bindings' ? { action: 'bindings', gates: { enabled: true, sendEnabled: false, continuationEnabled: false }, items: [binding] } : { action: 'history', items: [{ kind: 'ingress', id: 't', state: 'accepted', messageId: 'm', seq: 9, text: '<script>evil()</script>', messageCommitted: true, durable: false, transport: 'qq', endpoint: 'group:123', replyTo: 'quote-id' }], truncated: false } } } } }))
  assert.ok(html.includes(en.readOnly)); assert.ok(html.includes('group:123')); assert.ok(html.includes('native-session')); assert.ok(html.includes(en.committed)); assert.ok(html.includes(en.durable)); assert.ok(html.includes('&lt;script&gt;')); assert.equal(html.includes('<script>'), false); assert.equal(resources.length, 2)
  assert.equal(f.calls.length, 0, 'rendering uses the framework resource hook, no callback RPC or effect fetch')
  f.dispose()
})
