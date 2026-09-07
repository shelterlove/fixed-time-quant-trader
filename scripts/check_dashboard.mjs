// Execute the actual page script against a strict DOM stub and local QA API.
// This verifies render behavior and missing elements, not browser layout.
import assert from 'node:assert/strict';
import vm from 'node:vm';

const base = process.argv[2] || 'http://127.0.0.1:18081';
const html = await (await fetch(base)).text();
const snapshot = await (await fetch(base + '/api/status')).json();
const elements = new Map([...html.matchAll(/id="([^"]+)"/g)].map(m => [m[1], {innerHTML: '', textContent: ''}]));
const document = {
  getElementById(id) { assert(elements.has(id), `missing DOM element ${id}`); return elements.get(id); },
  querySelectorAll() { return []; }, addEventListener() {}, hidden: false,
};
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
await vm.runInNewContext(script, {document, console, setTimeout: () => 0, clearTimeout() {},
  fetch: async () => ({ok: true, json: async () => snapshot})});
assert(!elements.get('status').innerHTML.includes('读取失败'));
assert(html.includes('净收益（不含资金费）'));
assert(elements.get('kpis').innerHTML.includes('其中资金费'));
assert(elements.get('trades').innerHTML.includes('5仓'));
assert.equal((elements.get('trades').innerHTML.match(/PLANNED_EXIT/g) || []).length, 3);
assert(elements.get('timings').innerHTML.includes('提交'));
assert(elements.get('timings').innerHTML.includes('收到响应'));
console.log('Dashboard render passed: legacy version, fee label, deduplicated exit reasons and order timings.');
