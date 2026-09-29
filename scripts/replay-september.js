const fs=require('fs'),vm=require('vm');
const ctx={console,Date,Math,JSON,isFinite};vm.createContext(ctx);
for(const f of fs.readdirSync('.').filter(f=>f.endsWith('.gs')))vm.runInContext(fs.readFileSync(f,'utf8'),ctx);
// Inputs: connector range JSON objects with .values, including raw columns A:O and history A:AD.
// Run from repository root. No network requests and no Sheet writes.
const [historyFile,...rawFiles]=process.argv.slice(2);
if(!historyFile||!rawFiles.length)throw Error('Usage: node scripts/replay-september.js HISTORY.json RAW1.json [RAW2.json ...]');
const raw=rawFiles.flatMap(file=>JSON.parse(fs.readFileSync(file)).values).filter(r=>r[0].startsWith('9/')&&r[0].endsWith('/2026'));
const history=JSON.parse(fs.readFileSync(historyFile)).values;
const core=ctx.getActiveCatalog_({}).filter(ctx.isCoreItem_);
let total=0,added=[],removed=[];
const summary=core.map(item=>{const offers=raw.filter(r=>r[1]===item.id),days={};for(const r of offers){const candidate={title:r[3],price:Number(r[4]),vendor:r[2],url:r[12],provider:r[13]};const v=ctx.validateCandidate_(item,candidate);if(!days[r[0]])days[r[0]]=[];days[r[0]].push(candidate);if(v.pass&&r[9]!=='pass')added.push({id:item.id,title:r[3],price:r[4],reason:r[10]});if(!v.pass&&r[9]==='pass')removed.push({id:item.id,title:r[3],reason:v.failReason});}
const replay=Object.entries(days).filter(([d,rs])=>ctx.aggregateCandidates_(item,rs,84000,new Date(d),[]).valid).map(x=>x[0]);total+=replay.length;
const h=history.filter(r=>r[2]===item.id),fresh=h.filter(r=>r[18]==='FALSE'&&r[20]==='validated');return{id:item.id,scheduled:['apples','bananas','eggs','milk','butter','bread'].includes(item.id)?14:15,queriesWithOffers:Object.keys(days).length,fresh:fresh.length,replayed:replay.length,lastFresh:fresh.at(-1)?.[0],lastReplay:replay.sort((a,b)=>new Date(a)-new Date(b)).at(-1),offers:offers.length,passing:offers.filter(r=>r[9]==='pass').length};});
const unique=xs=>[...new Map(xs.map(x=>[x.id+'|'+x.title,x])).values()];
console.log(JSON.stringify({summary,total,added:unique(added),removed:unique(removed)},null,2));
