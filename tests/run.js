const fs=require('fs'),vm=require('vm'),assert=require('assert');
const props={};
const context={console,Date,Math,JSON,isFinite,Number,String,Boolean,Array,Object,RegExp,
  Session:{getScriptTimeZone:()=> 'America/Chicago'},
  Utilities:{formatDate(date,zone,pattern){const parts=new Intl.DateTimeFormat('en-CA',{timeZone:zone,year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(date),p={};parts.forEach(x=>p[x.type]=x.value);return pattern==='yyyy-MM'?`${p.year}-${p.month}`:`${p.year}-${p.month}-${p.day}`;}},
  PropertiesService:{getScriptProperties:()=>({getProperty:k=>props[k]??null,setProperty:(k,v)=>{props[k]=v;},deleteProperty:k=>{delete props[k];}})}};
vm.createContext(context);
for(const file of fs.readdirSync('.').filter(f=>f.endsWith('.gs')))vm.runInContext(fs.readFileSync(file,'utf8'),context,{filename:file});
console.log('Existing tests:',context.testMeasurementContract());
let count=0;
function test(name,fn){fn();count++;console.log('PASS',name);}
const core=context.getActiveCatalog_({}).filter(context.isCoreItem_),ids=core.map(x=>x.id),fingerprint=ids.slice().sort().join(',');
const now=new Date('2026-09-30T14:00:00Z');
const series=core.map(x=>({id:x.id,history:[{ts:now.toISOString(),usd:1,isStale:false}]}));
const ledger=[];
for(let day=1;day<=30;day++)for(let k=0;k<6;k++)ledger.push({day:`2026-09-${String(day).padStart(2,'0')}`,itemId:ids[((day-1)*6+k)%10],status:'success',methodology:'scheduled-grocery-v3',coreIds:fingerprint});
const grade=(rows=ledger,s=series,time=now)=>context.scheduledReliability_([],s,ids,rows,time);
test('30 days, 180 genuine grocery opportunities earns A',()=>assert.equal(grade().grade,'A'));
test('Exactly 95% earns A; 170/180 cannot',()=>{const r=ledger.map((x,i)=>({...x,status:i<9?'failed':'success'}));assert.equal(grade(r).refreshSuccessPct,95);assert.equal(grade(r).grade,'A');r[9].status='failed';assert.notEqual(grade(r).grade,'A');});
test('Metals never affect numerator, denominator, or grocery grade',()=>{const a=grade(ledger.concat([{...ledger[0],itemId:'gold',status:'failed'},{...ledger[0],itemId:'silver',status:'success'}]));assert.deepEqual(a,grade());});
test('Actual scheduled opportunities, including pending/empty/error, form denominator',()=>{const rows=ledger.slice(0,157).map((x,i)=>({...x,status:i<10?'scheduled':'success'}));assert.equal(grade(rows).expectedRefreshes,157);assert.equal(grade(rows).successfulRefreshes,147);});
test('Duplicate runs cannot inflate results',()=>assert.deepEqual(grade(ledger.concat(ledger)),grade()));
test('Missing collection days cannot earn A',()=>assert.notEqual(grade(ledger.filter(x=>x.day!=='2026-09-12')).grade,'A'));
test('Sparse perfect schedule cannot earn A',()=>assert.notEqual(grade(ledger.filter((x,i)=>i%6===0)).grade,'A'));
test('Stale, missing and future observations block A',()=>{for(const replacement of [{ts:'2026-09-27T14:00:00Z',usd:1,isStale:false},{ts:now.toISOString(),usd:null,isStale:false},{ts:'2026-10-01T14:00:00Z',usd:1,isStale:false}]){const s=JSON.parse(JSON.stringify(series));s[0].history=[replacement];assert.notEqual(grade(ledger,s).grade,'A');}});
test('48 hour boundary is inclusive',()=>{const s=JSON.parse(JSON.stringify(series));s[0].history[0].ts='2026-09-28T14:00:00Z';assert.equal(grade(ledger,s).grade,'A');s[0].history[0].ts='2026-09-28T13:59:59Z';assert.notEqual(grade(ledger,s).grade,'A');});
test('Pre-ledger history cannot silently get a new A',()=>{assert.equal(grade([]).grade,'Building');assert.equal(grade([]).trackingStartDate,'');});
test('Chicago calendar does not switch at UTC midnight',()=>assert.equal(context.serpApiCalendarKeys_(new Date('2026-09-30T01:00:00Z')).day,'2026-09-29'));
const all=core.concat([context.catalogById_('gold'),context.catalogById_('silver')]);
test('28,29,30,31 day monthly budgets and fair core rotation',()=>{for(const [year,month,days] of [[2026,2,28],[2028,2,29],[2026,9,30],[2026,1,31]]){let used=0,cursor=0,refCursor=0,last=null,previous=[];for(let d=1;d<=days;d++){const state={used,monthlyBudget:220,maxPerDay:8,keys:{day:`${year}-${String(month).padStart(2,'0')}-${String(d).padStart(2,'0')}`,dayNumber:d}},p=context.selectGroceryPlan_(all,{serpApiCoreSearchesPerDay:6,serpApiReferenceIntervalDays:7},state,cursor,refCursor,last);assert.equal(p.coreCount,6);assert.ok(p.items.length<=8);const today=p.items.filter(context.isCoreItem_).map(x=>x.id);if(d>1)assert.equal(new Set(previous.concat(today)).size,10);previous=today;used+=p.items.length;cursor+=p.coreCount;refCursor+=p.referenceCount;if(p.referenceCount)last=d;}assert.ok(used<=220);assert.equal(used,days*6+Math.ceil(days/7)*2);}});
test('Reference requests cannot displace groceries, even under cap 6 or low budget',()=>{for(const cap of [0,2,6,8])for(const used of [0,218,220]){const state={used,monthlyBudget:220,maxPerDay:cap,keys:{day:'2026-09-30',dayNumber:30}},p=context.selectGroceryPlan_(all,{serpApiCoreSearchesPerDay:6},state,0,0,null);assert.equal(p.coreCount,Math.min(6,cap,220-used));assert.ok(p.items.length<=cap&&used+p.items.length<=220);}});
test('Same-day and quota-exhausted repeats use no budget',()=>{Object.keys(props).forEach(k=>delete props[k]);const p={serpApiMonthlyBudget:220,serpApiMaxSearchesPerDay:8,serpApiCoreSearchesPerDay:6};assert.ok(context.planSerpApiRequests_(all,p,now).items.length);assert.equal(context.planSerpApiRequests_(all,p,now).items.length,0);context.markSerpApiExhausted_(now);assert.equal(context.planSerpApiRequests_(all,p,new Date('2026-09-30T16:00Z')).items.length,0);});
function validate(id,title,price=4.99,extra={}){return context.validateCandidate_(context.catalogById_(id),{title,price,vendor:'Store',url:'https://example.com/item',provider:'test',...extra});}
test('Bread need not say sandwich; non-loaf products rejected',()=>{assert.ok(validate('bread','Classic White Bread 20 oz').pass);for(const t of ['White Bread Rolls 20 oz','White Bread Buns 20 oz','White Bread Bagels 20 oz','Garlic Bread 20 oz','Banana Bread 20 oz','White Bread Crumbs 20 oz','White Breaded Chicken 20 oz'])assert.ok(!validate('bread',t).pass,t);});
test('Honey Crisp alias, same variety and equivalent package only',()=>{assert.ok(validate('apples','Honey Crisp Apples 48 oz (3 lb) 1.36 kg').pass);assert.ok(!validate('apples','Gala Apples 3 lb').pass);assert.ok(!validate('apples','Honeycrisp Apples 4 count').pass);});
test('Equivalent mass, metric and fluid units normalize safely',()=>{assert.ok(validate('butter','Unsalted Butter 1 lb').pass);assert.ok(validate('potatoes','Russet Potatoes 2.27 kg').pass);assert.ok(validate('milk','Whole Milk 128 fl oz').pass);assert.ok(!validate('milk','Whole Milk 128 oz').pass);});
test('Multipacks use total mass; wholesale and ambiguous packs rejected',()=>{assert.ok(validate('butter','Unsalted Butter 4 x 4 oz').pass);assert.ok(!validate('butter','Unsalted Butter 16 oz 10 per case',49.90).pass);assert.ok(!validate('potatoes','Russet Potatoes 10 x 5 lb',29).pass);assert.ok(!validate('butter','Unsalted Butter 16 oz club pack').pass);});
test('Safe explicit structured size; missing/serving/query data never inferred',()=>{const e=context.shoppingSizeEvidence_({extensions:['Net weight: 16 oz','Shipping weight: 20 oz'],snippet:'Serving size: 1 oz'});assert.equal(e.length,1);assert.ok(validate('butter','Unsalted Butter',4.99,{sizeEvidence:e}).pass);assert.ok(!validate('butter','Unsalted Butter',4.99).pass);assert.equal(context.shoppingSizeEvidence_({snippet:'Serving size: 16 oz',extensions:['$1 per 16 oz']}).length,0);});
test('Conflicting title/structured sizes rejected',()=>assert.ok(!validate('butter','Unsalted Butter 16 oz',4.99,{sizeEvidence:[{source:'extensions',text:'Size: 8 oz'}]}).pass));
test('White long-grain rice identity remains explicit',()=>{assert.ok(validate('rice','Long-grain White Rice 5 lb').pass);for(const t of ['Long Grain Rice 5 lb','Brown Long Grain Rice 5 lb','White Calrose Rice 5 lb'])assert.ok(!validate('rice',t).pass);});
test('Ambiguous dozen quantities and fractional mass',()=>{assert.ok(validate('eggs','Large Eggs 1 dozen',3).pass);assert.ok(!validate('eggs','Large Eggs 2 dozen',3).pass);assert.ok(validate('butter','Unsalted Butter 1/1 lb').pass);});
test('Basket and sats arithmetic preserved',()=>{const s=context.buildSnapshot_(now.toISOString(),50000,[{valid:true,usd:4,sats:8000,isStale:false},{valid:true,usd:5,sats:10000,isStale:false}],2);assert.equal(s.basketUsd,9);assert.equal(s.basketSats,18000);assert.equal(context.buildSnapshot_(now.toISOString(),50000,[{valid:true,usd:4,sats:8000}],2).basketUsd,null);});
test('Reference-only transport failure preserves grocery validation',()=>{
  Object.keys(props).forEach(k=>delete props[k]);let schedules=0,calls=0;
  context.UrlFetchApp={fetchAll(requests){calls++;assert.ok(schedules>0,'schedule must precede request');if(requests.every(r=>/gold|silver/.test(decodeURIComponent(r.url))))throw Error('reference outage');return requests.map(()=>({getResponseCode:()=>200,getContentText:()=>JSON.stringify({shopping_results:[{title:'Honeycrisp Apples 3 lb',extracted_price:6,source:'Store'}]})}));}};
  const result=context.fetchShoppingCandidates_(all,{serpApiKey:'test',serpApiMonthlyBudget:220,serpApiMaxSearchesPerDay:8,serpApiCoreSearchesPerDay:6},items=>{schedules=items.length;});
  assert.equal(result.apples.length,1);assert.equal(result.gold.length,0);assert.equal(result.__outcomes.gold,'transport_error');assert.equal(result.__outcomes.apples,'candidates_received');assert.equal(calls,2);
});
function fakeSheet(){const cells=[];return{cells,getLastRow:()=>cells.length,getDataRange:()=>({getValues:()=>cells}),getRange(row,col,rows=1,cols=1){return{setValues(values){for(let i=0;i<rows;i++){cells[row-1+i]??=[];for(let j=0;j<cols;j++)cells[row-1+i][col-1+j]=values[i][j];}},setValue(value){cells[row-1]??=[];cells[row-1][col-1]=value;}};}};}
test('Durable schedule logs pending, failed and successful outcomes without duplicate entries',()=>{
  const sheet=fakeSheet();sheet.cells.push(['day','item_id','provider','status','scheduled_at','completed_at','reason','methodology','core_ids']);
  let flushed=0;context.SpreadsheetApp={flush(){flushed++;}};
  const entries=context.beginRefreshSchedule_(sheet,core.slice(0,2),core,'serpapi',now);
  assert.equal(flushed,1);assert.equal(context.readRefreshSchedule_(sheet).length,2);assert.equal(sheet.cells[1][3],'scheduled');
  context.finishRefreshSchedule_(sheet,entries,[{itemId:ids[0],valid:true,isStale:false},{itemId:ids[1],valid:true,isStale:true}],now);
  assert.equal(sheet.cells[1][3],'success');assert.equal(sheet.cells[2][3],'failed');
  assert.equal(context.beginRefreshSchedule_(sheet,core.slice(0,2),core,'serpapi',now).length,0);
});
test('Failure to persist a schedule prevents network calls',()=>{
  Object.keys(props).forEach(k=>delete props[k]);let called=false;context.UrlFetchApp={fetchAll(){called=true;return[];}};
  assert.throws(()=>context.fetchShoppingCandidates_(all,{serpApiKey:'test',serpApiMonthlyBudget:220,serpApiMaxSearchesPerDay:8,serpApiCoreSearchesPerDay:6},()=>{throw Error('sheet unavailable');}));assert.equal(called,false);
});
test('Salt and onions remain inactive; structured data cannot override conflicting package',()=>{
  assert.equal(core.length,10);assert.ok(!ids.includes('salt')&&!ids.includes('yellow_onions'));
  assert.ok(!validate('apples','Honeycrisp Apples 2 lb',5,{sizeEvidence:[{source:'extensions',text:'Size: 3 lb'}]}).pass);
  assert.ok(!validate('bread','White Bread 20 oz | Wheat Bread 20 oz',5).pass);
  assert.ok(!validate('eggs','Large Eggs half dozen',3).pass);
});
test('Missing latest snapshot cannot be hidden by older item series',()=>{
  const result=context.scheduledReliability_([{ts:now.toISOString(),missingCount:1}],series,ids,ledger,now);
  assert.equal(result.missingItems,1);assert.notEqual(result.grade,'A');
});
test('Monthly reset preserves grocery cursor and honors configured budget',()=>{
  Object.keys(props).forEach(k=>delete props[k]);
  const p={serpApiMonthlyBudget:10,serpApiMaxSearchesPerDay:6,serpApiCoreSearchesPerDay:6};
  const first=context.planSerpApiRequests_(all,p,new Date('2026-09-29T14:00Z'));
  const second=context.planSerpApiRequests_(all,p,new Date('2026-09-30T14:00Z'));
  assert.equal(first.items.length,6);assert.equal(second.items.length,4);assert.equal(Number(props.SERPAPI_USAGE_COUNT),10);
  const cursor=Number(props.SERPAPI_CORE_CURSOR);
  const third=context.planSerpApiRequests_(all,p,new Date('2026-10-01T14:00Z'));
  assert.equal(third.items.length,6);assert.equal(Number(props.SERPAPI_CORE_CURSOR),cursor+6);assert.equal(Number(props.SERPAPI_USAGE_COUNT),6);
});
test('Empty, HTTP and malformed responses remain distinguishable failed opportunities',()=>{
  for(const [status,body,reason] of [[200,'{}','empty_results'],[500,'','http_500'],[200,'{broken','invalid_response']]){
    Object.keys(props).forEach(k=>delete props[k]);
    context.UrlFetchApp={fetchAll(requests){return requests.map(()=>({getResponseCode:()=>status,getContentText:()=>body}));}};
    const result=context.fetchShoppingCandidates_(core,{serpApiKey:'test',serpApiMonthlyBudget:220,serpApiMaxSearchesPerDay:6,serpApiCoreSearchesPerDay:6},()=>{});
    assert.equal(result.apples.length,0);assert.equal(result.__outcomes.apples,reason);
  }
});
JSON.parse(fs.readFileSync('appsscript.json','utf8'));
for(const f of ['App.html','Index.html'])for(const match of fs.readFileSync(f,'utf8').matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g))new vm.Script(match[1],{filename:f});
console.log(`${count} new regression tests passed; all existing tests and syntax/manifest checks passed.`);
module.exports={context};
