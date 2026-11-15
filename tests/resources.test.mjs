import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync, existsSync } from 'node:fs'
const root = new URL('../', import.meta.url)
const read = name => readFileSync(new URL(name, root), 'utf8')
test('manifest exports runtime, localized metadata and an original bounded SVG icon', () => {
  const manifest = JSON.parse(read('package.json')); assert.equal(manifest.name, 'dsh-super-agent-io'); assert.equal(manifest.version, '0.0.1'); assert.equal(manifest.dsh.bundle.patch, './cordis.patch.yml')
  assert.equal(manifest.exports['.'].default, './index.mjs'); assert.equal(manifest.exports['./icon'], './icon.svg'); assert.ok(manifest.files.includes('locale/*.json'))
  for (const name of ['index.mjs', 'index.d.ts', 'contract.mjs', 'core.mjs', 'domain.mjs', 'native-host.mjs', 'continuation.mjs', 'continuation-host.mjs', 'README.md', 'LICENSE', 'cordis.patch.yml', 'icon.svg']) assert.ok(existsSync(new URL(name, root)), name)
  const en = JSON.parse(read('locale/en.json')); const zh = JSON.parse(read('locale/zh.json')); assert.ok(en.meta.title && en.meta.description); assert.ok(zh.meta.title && zh.meta.description); assert.deepEqual(Object.keys(en.config), Object.keys(zh.config)); assert.ok(zh.config.continuationEnabled.includes('准入'))
  const icon = read('icon.svg'); assert.ok(icon.includes('xmlns="http://www.w3.org/2000/svg"')); assert.ok(icon.includes('viewBox="0 0 64 64"')); assert.ok(Buffer.byteLength(icon) < 262144); assert.equal(/<(script|foreignObject)|https?:\/\/(?!www\.w3\.org\/2000\/svg)/.test(icon), false)
  assert.ok(read('cordis.patch.yml').includes('enabled: false')); assert.ok(read('cordis.patch.yml').includes('sendEnabled: false')); assert.ok(read('README.md').includes('Known Limitations'))
})
