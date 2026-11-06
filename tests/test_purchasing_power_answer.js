const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const repo = path.join(__dirname, '..');
const html = fs.readFileSync(path.join(repo, 'Index.html'), 'utf8');
const app = fs.readFileSync(path.join(repo, 'App.html'), 'utf8');
const elements = {};
for (const match of html.matchAll(/id="([^"]+)"/g)) {
  elements[match[1]] = {innerHTML:'', textContent:'', style:{}, classList:{remove(){}}, querySelectorAll(){return [];}};
}
const context = vm.createContext({
  document:{getElementById(id){assert.ok(elements[id], 'Missing actual HTML element: '+id);return elements[id];}},
  Chart:function(){this.destroy=function(){};},setTimeout(){}, console
});
const js = app.match(/<script>([\s\S]*?)<\/script>/)[1].replace('wire();load();', '');
vm.runInContext(js, context);
const call = (name, ...args) => {
  context.input = args;
  return vm.runInContext(name+'(...input)', context);
};
const now = Date.now();
const definition = {itemCount:2, items:[{id:'eggs'}, {id:'milk'}]};
const snapshot = (offset, sats, overrides={}) => ({
  ts:new Date(now-offset*86400000).toISOString(),btcUsd:100000,basketUsd:53,basketSats:sats,
  missingCount:0,staleCount:0,items:[
    {itemId:'eggs',valid:true,usd:25,sats:sats/2,isStale:false},
    {itemId:'milk',valid:true,usd:28,sats:sats/2,isStale:false}
  ],...overrides
});
const base = snapshot(29,53000);
const latest = snapshot(0,42400);
const answer = rows => call('basketAnswer', rows, definition, now);
assert.equal(answer([base,latest]).change,25);
assert.equal(answer([base,snapshot(0,58300)]).change.toFixed(4),'-9.0909');
assert.equal(answer([base,snapshot(0,53000)]).change,0);
assert.equal(answer([latest,base]).change,25, 'Sort before comparing');
assert.equal(answer([latest]).status,'insufficient');
assert.equal(answer([]).status,'empty');
assert.equal(answer([base,latest,snapshot(-1,30000)]).last.ts,latest.ts,'Ignore future observations');
for (const value of [null, undefined, '', ' ', false, true, NaN, Infinity, 0, -1]) {
  assert.equal(call('positiveValue',value),false);
  assert.equal(answer([base,snapshot(0,value)]).status,'incomplete');
}
assert.equal(answer([base,latest,snapshot(0,null,{missingCount:1})]).status,'incomplete','Do not silently show an older complete result');
assert.equal(answer([base,snapshot(0,42400,{items:[latest.items[0],{...latest.items[1],itemId:'bread'}]})]).status,'incomplete','Same count is not the same basket');
assert.equal(answer([base,snapshot(0,42400,{items:[latest.items[0],latest.items[0]]})]).status,'incomplete','Duplicate identity cannot stand in for missing item');
assert.equal(answer([base,snapshot(0,42400,{staleCount:1})]).provisional,true);
assert.equal(answer([base,snapshot(0,42400,{items:latest.items.map(i=>({...i,isStale:true}))})]).carried,true);
assert.equal(answer([base,snapshot(3,42400)]).outdated,true);
assert.equal(answer([snapshot(29,53000,{staleCount:1}),latest]).provisional,true,'Baseline carry also matters');
assert.equal(answer([snapshot(29,null),base,latest]).status,'ready');
function render(rows) {
  context.fixture = {snapshots:rows,basketDefinition:definition,itemSeries:[],references:{},reliability:{}};
  vm.runInContext('state.data=fixture; render();',context);
  assert.ok(!/NaN|Infinity/.test(elements.hero.innerHTML));
  return elements.hero.innerHTML;
}
assert.match(render([base,latest]),/Bitcoin buys 25.00% more groceries/);
assert.match(render([base,snapshot(0,58300)]),/Bitcoin buys 9.09% fewer groceries/);
assert.match(render([base,snapshot(0,53000)]),/Bitcoin buys the same groceries/);
assert.match(render([base,snapshot(0,42400,{staleCount:1})]),/Provisional comparison/);
assert.match(elements.insight.textContent,/carried-forward/);
assert.match(render([base,snapshot(3,42400)]),/Provisional comparison/);
assert.match(elements.insight.textContent,/over 48 hours old/);
assert.match(render([base,latest,snapshot(0,null,{missingCount:1})]),/Latest basket is incomplete/);
assert.doesNotMatch(elements.hero.innerHTML,/Bitcoin buys [\d.]+% (more|fewer) groceries/);
assert.match(render([latest]),/One complete observation so far/);
assert.match(render([]),/No comparable basket history/);
assert.match(html,/data-unit="sats" class="active"/);
assert.equal(vm.runInContext('state.unit',context),'sats');
assert.doesNotMatch(html,/Solo Mining Dashboard|127\.0\.0\.1/);
assert.match(elements.chartNote.textContent,/Lower means/);
console.log('Purchasing-power answer checks passed: direction, inverse ratio, null/zero guards, basket identity, latest completeness, stale boundaries, future dates, empty/single observations, actual render wiring and sats default.');
