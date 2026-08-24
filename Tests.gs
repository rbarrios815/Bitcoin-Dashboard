function testMeasurementContract(){
  const basket=[{usd:4,sats:8000},{usd:5,sats:10000}];
  const totalUsd=basket.reduce(function(sum,row){return sum+row.usd;},0);
  if(totalUsd!==9)throw new Error('Basket must sum package prices.');
  if(isCoreId_('gold'))throw new Error('Gold cannot be a grocery basket item.');
  const butter=validateCandidate_(catalogById_('butter'),{title:'Store Unsalted Butter 16 oz',price:4.99,vendor:'Grocery Store',url:'https://example.com',provider:'test'});
  if(!butter.pass)throw new Error('Unsalted butter matcher regression: '+butter.failReason);
  const outliers=removeOutliers_([{normalizedPrice:1.64},{normalizedPrice:3.98},{normalizedPrice:9.99},{normalizedPrice:118.85}]);
  if(outliers.some(function(row){return row.normalizedPrice===118.85;}))throw new Error('Outlier filter regression.');
  testSerpApiBudget_();
  testReliabilityGrade_();
  return{ok:true,version:PP_VERSION};
}

function testReliabilityGrade_(){
  const dayMs=24*60*60*1000;
  const now=new Date('2026-09-30T12:00:00Z');
  const snapshots=[];
  for(let offset=29;offset>=0;offset--)snapshots.push({ts:new Date(now.getTime()-offset*dayMs).toISOString(),freshCount:6,missingCount:0});
  const series=[];
  for(let i=0;i<12;i++)series.push({id:'item_'+i,history:[{ts:now.toISOString(),usd:1,isStale:false}]});
  const complete=buildReliability_(snapshots,series,12,6,now,'2026-09-01');
  if(complete.grade!=='A'||complete.successfulRefreshes!==180)throw new Error('Complete 30-day reliability record must earn A.');
  const duplicate=snapshots.concat([{ts:new Date(now.getTime()+60*60*1000).toISOString(),freshCount:0,missingCount:0}]);
  if(buildReliability_(duplicate,series,12,6,now,'2026-09-01').successfulRefreshes!==180)throw new Error('A same-day no-search retry must not erase a successful scheduled refresh.');
  const ninetyFive=snapshots.map(function(row,index){return Object.assign({},row,{freshCount:index<28?6:index===28?3:0});});
  const threshold=buildReliability_(ninetyFive,series,12,6,now,'2026-09-01');
  if(threshold.grade!=='A'||threshold.refreshSuccessPct!==95)throw new Error('A threshold must accept 171 of 180 scheduled refreshes.');
  const below=snapshots.map(function(row,index){return Object.assign({},row,{freshCount:index<28?6:index===28?2:0});});
  if(buildReliability_(below,series,12,6,now,'2026-09-01').grade==='A')throw new Error('Below 95% scheduled success cannot earn A.');
  const aged=JSON.parse(JSON.stringify(series));
  aged[0].history[0].ts=new Date(now.getTime()-72*60*60*1000).toISOString();
  const staleGate=buildReliability_(snapshots,aged,12,6,now,'2026-09-01');
  if(staleGate.grade==='A'||staleGate.currentItems!==11)throw new Error('An item older than 48 hours must block A.');
  const building=buildReliability_(snapshots.slice(-2),series,12,6,new Date('2026-09-02T12:00:00Z'),'2026-09-01');
  if(building.grade!=='Building')throw new Error('A requires a full 30-day track record.');
  return{ok:true};
}

function testSerpApiBudget_(){
  if(calculateSerpApiAllowance_(220,0,6,12)!==6)throw new Error('Daily SerpApi cap regression.');
  if(calculateSerpApiAllowance_(220,218,6,12)!==2)throw new Error('Monthly SerpApi cap regression.');
  if(calculateSerpApiAllowance_(220,220,6,12)!==0)throw new Error('Exhausted local budget regression.');
  const items=[];
  for(let i=0;i<12;i++)items.push({id:'item_'+i});
  const first=selectSerpApiRotation_(items,6,0);
  const second=selectSerpApiRotation_(items,6,1);
  const ids={};
  first.concat(second).forEach(function(item){ids[item.id]=true;});
  if(Object.keys(ids).length!==12)throw new Error('Two-day rotation must cover all 12 default SerpApi items.');
  if(!isSerpApiExhaustedResponse_(429,''))throw new Error('HTTP 429 must block further SerpApi calls for the month.');
  if(!isSerpApiExhaustedResponse_(403,'Your searches are exhausted'))throw new Error('Provider exhaustion message must be detected.');
  if(isSerpApiExhaustedResponse_(500,'temporary error'))throw new Error('Transient provider errors must not block the month.');
  return{ok:true};
}
