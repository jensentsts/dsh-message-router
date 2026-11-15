import React from 'react'
import { z } from 'zod'
import { monitorClientContribution } from './monitor-rpc.mjs'
import en from './locale/en.json' with { type: 'json' }
import zh from './locale/zh.json' with { type: 'json' }
const h = React.createElement
const NS = 'independent.superAgentIo'
const PROTOCOL = 'super-agent-io-monitor'
const address = (request, revision) => request ? `dsh-resource://${PROTOCOL}/${encodeURIComponent(JSON.stringify(request))}?r=${revision}` : ''
const css = `.dsh-io-monitor{color:var(--dsw-alias-label-primary);display:flex;flex-direction:column;gap:14px;min-width:0}.dsh-io-monitor h2,.dsh-io-monitor h3,.dsh-io-monitor p{margin:0}.dsh-io-monitor small,.dsh-io-muted{color:var(--dsw-alias-label-secondary)}.dsh-io-toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.dsh-io-monitor button,.dsh-io-monitor select{font:inherit;color:var(--dsw-alias-label-primary);background:var(--dsw-alias-bg-layer-2);border:1px solid var(--dsw-alias-border-l4);border-radius:6px;padding:6px 10px;max-width:100%}.dsh-io-monitor button:disabled{opacity:.5}.dsh-io-panel{border:1px solid var(--dsw-alias-border-l4);border-radius:8px;padding:12px;display:flex;flex-direction:column;gap:9px;min-width:0}.dsh-io-list{display:flex;flex-direction:column;gap:8px}.dsh-io-row{display:flex;flex-direction:column;gap:6px;border-bottom:1px solid var(--dsw-alias-border-l4);padding:10px 0;min-width:0}.dsh-io-row:last-child{border-bottom:0}.dsh-io-row pre{white-space:pre-wrap;overflow-wrap:anywhere;margin:0;font:inherit}.dsh-io-row code{overflow-wrap:anywhere;white-space:normal}.dsh-io-grid{display:grid;grid-template-columns:max-content 1fr;gap:5px 12px}.dsh-io-grid dd{margin:0;overflow-wrap:anywhere}.dsh-io-state{border:1px solid var(--dsw-alias-border-l4);padding:2px 6px;border-radius:5px;font-size:12px}.dsh-io-tablewrap{overflow:auto}.dsh-io-monitor table{width:100%;border-collapse:collapse;text-align:left}.dsh-io-monitor th,.dsh-io-monitor td{padding:8px;border-bottom:1px solid var(--dsw-alias-border-l4);vertical-align:top;overflow-wrap:anywhere}.dsh-io-failure{color:var(--dsw-alias-label-primary);border-left:3px solid var(--dsw-alias-border-l4);padding:8px}.dsh-io-monitor button:focus-visible,.dsh-io-monitor select:focus-visible{outline:2px solid var(--dsw-alias-label-secondary);outline-offset:2px}@media(max-width:650px){.dsh-io-grid{grid-template-columns:1fr}.dsh-io-monitor table{min-width:520px}}`
function ErrorState({ state, t }) {
  if (state.status === 'loading') return h('p', { role: 'status' }, t('loading'))
  if (state.status === 'none') return h('p', { className: 'dsh-io-muted' }, t('unavailable'))
  if (state.status === 'failed' || state.value?.ok === false) return h('p', { role: 'alert', className: 'dsh-io-failure' }, t('failure'), ' ', state.value?.error?.code ?? state.failure?.code ?? '')
  return null
}
function ReceiptRows({ rows, t }) {
  if (!rows.length) return h('p', { className: 'dsh-io-muted' }, t('emptyHistory'))
  return h('div', { className: 'dsh-io-list' }, ...rows.map(row => h('article', { className: 'dsh-io-row', key: `${row.kind}:${row.id}:${row.state}` },
    h('div', { className: 'dsh-io-toolbar' }, h('strong', null, t(row.kind === 'ingress' ? 'input' : row.kind === 'delivery' ? 'output' : 'audit')), h('span', { className: 'dsh-io-state' }, t(`state.${row.state}`)), h('code', null, row.id)),
    row.messageId && h('small', null, t('messageId'), ': ', row.messageId, row.seq === undefined ? '' : ` · #${row.seq}`),
    row.endpoint && h('small', null, t('endpoint'), ': ', row.transport, '/', row.endpoint),
    row.replyTo && h('small', null, t('replyTo'), ': ', row.replyTo),
    row.senderId && h('small', null, t('sender'), ': ', row.senderId),
    row.source?.model && h('small', null, t('model'), ': ', row.source.provider, '/', row.source.model),
    row.text && h('pre', null, row.text), row.textTruncated && h('small', null, t('excerpt')),
    row.media?.length > 0 && h('small', null, t('media'), ': ', row.media.map(b => b.attachment?.name ?? b.type).join(', ')),
    row.externalMessageId && h('small', null, t('externalId'), ': ', row.externalMessageId),
    row.messageCommitted !== undefined && h('small', null, t('committed'), ': ', t(row.messageCommitted ? 'yes' : 'no'), ' · ', t('durable'), ': ', t(row.durable ? 'yes' : 'no')),
    row.reason && h('code', null, row.reason))))
}
function Monitor(props) {
  const { useResource, t } = props
  const [revision, setRevision] = React.useState(0)
  const [bindingCursor, setBindingCursor] = React.useState(undefined)
  const [choice, setChoice] = React.useState('')
  const [kind, setKind] = React.useState('all')
  const [historyCursor, setHistoryCursor] = React.useState(undefined)
  const catalogState = useResource(address({ action: 'bindings', limit: 30, ...(bindingCursor ? { cursor: bindingCursor } : {}) }, revision))
  const catalog = catalogState.value?.ok ? catalogState.value.value : undefined
  const binding = catalog?.items.find(b => b.id === choice) ?? catalog?.items[0]
  const historyState = useResource(binding ? address({ action: 'history', bindingId: binding.id, kind, limit: 20, ...(historyCursor ? { cursor: historyCursor } : {}) }, revision) : '')
  const history = historyState.value?.ok ? historyState.value.value : undefined
  return h('section', { className: 'dsh-io-monitor', 'aria-label': t('nav') }, h('style', null, css),
    h('div', { className: 'dsh-io-toolbar' }, h('h2', null, t('title')), h('button', { type: 'button', onClick: () => setRevision(revision + 1) }, t('refresh'))),
    h('p', { className: 'dsh-io-muted' }, t('readOnly')), h('p', { className: 'dsh-io-muted' }, t('acceptedHint')), h(ErrorState, { state: catalogState, t }),
    catalog && h(React.Fragment, null,
      h('div', { className: 'dsh-io-panel' }, h('h3', null, t('gates')), h('div', { className: 'dsh-io-toolbar' }, ...['enabled', 'sendEnabled', 'continuationEnabled'].map(key => h('span', { key }, t(`gate.${key}`), ': ', t(catalog.gates[key] ? 'on' : 'off')))), h('p', { className: 'dsh-io-muted' }, t('gateHint'))),
      h('div', { className: 'dsh-io-toolbar' }, h('label', null, t('binding'), ' ', h('select', { value: binding?.id ?? '', onChange: e => { setChoice(e.target.value); setHistoryCursor(undefined) } }, ...catalog.items.map(b => h('option', { key: b.id, value: b.id }, `${b.label} · ${b.id}`)))), catalog.truncated && h('button', { type: 'button', onClick: () => { setBindingCursor(catalog.nextCursor); setHistoryCursor(undefined); setChoice('') } }, t('nextBindings')), bindingCursor && h('button', { type: 'button', onClick: () => { setBindingCursor(undefined); setHistoryCursor(undefined); setChoice('') } }, t('firstPage'))),
      !binding && h('p', { className: 'dsh-io-muted' }, t('emptyBindings')),
      binding && h(React.Fragment, null,
        h('dl', { className: 'dsh-io-panel dsh-io-grid' }, h('dt', null, t('fromTo')), h('dd', null, `${binding.transport} / ${binding.endpoint} ↔ ${binding.sessionId}`), h('dt', null, t('mode')), h('dd', null, binding.mode), h('dt', null, t('inputStates')), h('dd', null, JSON.stringify(binding.inputs)), h('dt', null, t('outputStates')), h('dd', null, JSON.stringify(binding.outputs))),
        h('div', { className: 'dsh-io-toolbar' }, h('label', null, t('history'), ' ', h('select', { value: kind, onChange: e => { setKind(e.target.value); setHistoryCursor(undefined) } }, ...['all', 'ingress', 'delivery', 'audit'].map(k => h('option', { key: k, value: k }, t(`filter.${k}`))))), historyCursor && h('button', { type: 'button', onClick: () => setHistoryCursor(undefined) }, t('firstPage')), history?.truncated && h('button', { type: 'button', onClick: () => setHistoryCursor(history.nextCursor) }, t('nextPage'))),
        h(ErrorState, { state: historyState, t }), history && h(ReceiptRows, { rows: history.items, t }), history?.truncated && h('p', { className: 'dsh-io-muted' }, t('truncated')))))
}
export const inject = ['slots', 'locale', 'remote', 'resources']
export async function apply(ctx) {
  await ctx.remote.$mount(monitorClientContribution(z))
  ctx.inject(['remote.superAgentIoMonitor'], inner => {
    const remote = inner['remote.superAgentIoMonitor']
    inner.effect(() => inner.locale.register(NS, { en: en.monitor, zh: zh.monitor }))
    const t = inner.locale.bind(NS)
    inner.effect(() => inner.resources.register({ protocol: PROTOCOL, async *open(url, { signal }) {
      let request
      try { request = JSON.parse(decodeURIComponent(new URL(url).pathname.slice(1))) }
      catch (error) { yield { ok: true, value: { ok: false, error: { code: 'INVALID_ADDRESS', message: 'Invalid read-only monitor address' } } }; return }
      try { const result = await remote.query(request, signal); if (!signal.aborted) yield result }
      catch (error) { if (!signal.aborted) yield { ok: true, value: { ok: false, error: { code: 'READ_FAILED', message: 'Read-only monitor request failed' } } } }
    } }))
    inner.slots.inject('settings.section', () => inner.slots.register({ name: 'settings.section', id: 'independent.super-agent-io.monitor', order: 53, locale: NS, label: () => t('nav') }, Monitor))
  })
}
