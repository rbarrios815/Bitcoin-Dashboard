function buildSnapshot_(ts, btcUsd, items, expectedCount) {
  const valid = items.filter(function(row){return row.valid && finitePositive_(row.usd) && finitePositive_(row.sats);});
  const freshCount = valid.filter(function(row){return !row.isStale;}).length;
  const staleCount = valid.filter(function(row){return row.isStale;}).length;
  const missingCount = Math.max(0,expectedCount-valid.length);
  const basketUsd = valid.length===expectedCount?valid.reduce(function(sum,row){return sum+row.usd;},0):null;
  const basketSats = finitePositive_(basketUsd)&&finitePositive_(btcUsd)?usdToSats_(basketUsd,btcUsd):null;
  const quality = quality_(freshCount,staleCount,missingCount,expectedCount);
  return {ts:ts,btcUsd:btcUsd,items:items,basketUsd:basketUsd,basketSats:basketSats,freshCount:freshCount,staleCount:staleCount,missingCount:missingCount,coveragePct:quality.coveragePct,confidenceGrade:quality.confidenceGrade,confidenceScore:quality.confidenceScore,quality:quality};
}

function quality_(fresh, stale, missing, expected) {
  const score = expected ? (fresh + stale*0.35)/expected*100 : 0;
  const grade = score>=95?'A':score>=80?'B':score>=60?'C':score>=40?'D':'F';
  return {freshItems:fresh,staleItems:stale,missingItems:missing,expectedItems:expected,coveragePct:expected?(fresh+stale)/expected*100:0,confidenceScore:score,confidenceGrade:grade,status:(grade==='A'||grade==='B')?'strong':grade==='C'?'limited':'insufficient'};
}
function emptyQuality_(count){return quality_(0,0,count,count);}

function normalizeHistoricalItem_(row,item,btcUsd) {
  if (!row) return null;
  const usd=Number(row.usd);
  const status=String(row.status||'validated').toLowerCase();
  const valid=finitePositive_(usd)&&status==='validated';
  return {valid:valid,itemId:item.id,name:item.name,description:item.description,category:item.category,usd:valid?usd:null,sats:valid&&finitePositive_(btcUsd)?usdToSats_(usd,btcUsd):null,isStale:Boolean(row.isStale),status:status,failReason:row.failReason||'',vendor:row.vendor||'',source:row.source||'',sourceUrl:row.sourceUrl||'',usedCount:row.usedCount||0,matchedCount:row.matchedCount||0,title:row.title||'',ts:row.ts};
}
function carriedItem_(item,usd,btcUsd,ts,current){return {valid:true,itemId:item.id,name:item.name,description:item.description,category:item.category,usd:usd,sats:usdToSats_(usd,btcUsd),isStale:true,status:'validated',failReason:current&&current.failReason||'carried_forward',vendor:current&&current.vendor||'',source:'last_known_validated',sourceUrl:current&&current.sourceUrl||'',usedCount:current&&current.usedCount||0,matchedCount:current&&current.matchedCount||0,title:current&&current.title||'',ts:ts};}
function carriedResult_(item,usd,btcUsd,ts,old){const x=carriedItem_(item,usd,btcUsd,ts.toISOString(),old);return Object.assign(x,{query:item.query,normalizedPrice:old.normalizedPrice||'',unit:item.unit,score:old.score||'',method:'stale_fallback',vendors:old.vendors||old.vendor||'',urls:old.urls||old.sourceUrl||'',scores:old.scores||'',prices:old.prices||'',multi:Boolean(old.multi)});}
function rejectedResult_(item,btcUsd,ts,reason){return {valid:false,itemId:item.id,name:item.name,description:item.description,query:item.query,usd:0,sats:0,normalizedPrice:'',unit:item.unit,isStale:true,status:'rejected',failReason:reason||'no_valid_candidates',source:'error',vendor:'',sourceUrl:'',score:'',matchedCount:0,usedCount:0,method:'none',vendors:'',urls:'',scores:'',prices:'',multi:false,title:'',ts:ts.toISOString()};}

function buildReferenceSeries_(timestamps,grouped,references) {
  const out={};
  references.forEach(function(item){out[item.id]={id:item.id,name:item.name,description:item.description,history:[]};});
  timestamps.forEach(function(ts){
    const group=grouped[ts],btc=Number(group.btcUsd);
    references.forEach(function(item){
      let usd=null,sats=null,row=group.rows[item.id],source='',vendor='',sourceUrl='',isStale=false;
      if(item.id==='cash10'){usd=10;sats=finitePositive_(btc)?usdToSats_(10,btc):null;source='fixed';}
      else if(item.id==='sats10000'){sats=10000;usd=finitePositive_(btc)?10000/100000000*btc:null;source='fixed';}
      else if(row&&finitePositive_(row.usd)){usd=row.usd;sats=finitePositive_(btc)?usdToSats_(usd,btc):row.sats;source=row.source;vendor=row.vendor;sourceUrl=row.sourceUrl;isStale=row.isStale;}
      if(finitePositive_(usd)||finitePositive_(sats)) out[item.id].history.push({ts:ts,usd:usd,sats:sats,btcUsd:btc,source:source,vendor:vendor,sourceUrl:sourceUrl,isStale:isStale});
    });
  });
  return out;
}

function historyRow_(row,h) {
  const date=parseDate_(value_(row,h.timestamp));
  return {ts:date?date.toISOString():null,btcUsd:Number(value_(row,h.btc_usd)),itemId:String(value_(row,h.item_id)||'').trim().toLowerCase(),usd:Number(value_(row,h.usd)),sats:Number(value_(row,h.sats)),normalizedPrice:Number(value_(row,h.normalized_price)),unit:String(value_(row,h.normalized_unit)||''),source:String(value_(row,h.price_source)||''),vendor:String(value_(row,h.price_vendor)||''),sourceUrl:String(value_(row,h.source_url)||''),isStale:truthy_(value_(row,h.is_stale)),status:String(value_(row,h.validation_status)||'validated'),failReason:String(value_(row,h.fail_reason)||''),matchedCount:Number(value_(row,h.matched_source_count)||0),usedCount:Number(value_(row,h.used_source_count)||0),method:String(value_(row,h.aggregation_method)||''),vendors:String(value_(row,h.selected_vendors)||''),urls:String(value_(row,h.selected_source_urls)||''),scores:String(value_(row,h.selected_match_scores)||''),prices:String(value_(row,h.aggregated_normalized_prices)||''),multi:truthy_(value_(row,h.is_multi_source)),score:Number(value_(row,h.match_score)||0),title:String(value_(row,h.raw_vendor_title)||value_(row,h.source_item_description)||'')};
}

function historyValues_(ts,btc,row,basketUsd,basketSats){return [ts,btc,row.itemId,row.name,row.query,row.query,row.description,row.title||'',row.title||'',row.usd||0,row.sats||0,row.normalizedPrice||'',row.unit||'',basketUsd||0,basketSats||0,row.source||'',row.vendor||'',row.sourceUrl||'',Boolean(row.isStale),row.score||'',row.status||'',row.failReason||'',row.matchedCount||0,row.usedCount||0,row.method||'',row.vendors||'',row.urls||'',row.scores||'',row.prices||'',Boolean(row.multi)];}
function rawValues_(ts,row){return [ts,row.itemId,row.vendor,row.title,row.rawPrice,row.parsedQuantity||'',row.parsedUnit||'',row.normalizedPrice||'',row.unit||'',row.pass?'pass':'fail',row.failReason,row.score,row.url,row.provider,Boolean(row.selected)];}

function lastValidatedRows_(sheet){
  const values=sheet.getDataRange().getValues(); if(values.length<2)return{};
  const h=indexHeader_(values[0]),out={};
  for(let i=values.length-1;i>=1;i--){const row=historyRow_(values[i],h);if(out[row.itemId]||!finitePositive_(row.usd)||String(row.status).toLowerCase()!=='validated')continue;out[row.itemId]=row;}
  return out;
}
