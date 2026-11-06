// Methodology 3: explicit item/day opportunities, persisted before provider calls.
const REFRESH_HEADER=['day','item_id','provider','status','scheduled_at','completed_at','reason','methodology','core_ids'];
const RELIABILITY_METHOD='scheduled-grocery-v3';

function readRefreshSchedule_(sheet){
  if(!sheet||sheet.getLastRow()<2)return[];
  const values=sheet.getDataRange().getValues(),h=indexHeader_(values[0]);
  return values.slice(1).map(function(r){
    const day=value_(r,h.day);
    return{day:day instanceof Date?serpApiCalendarKeys_(day).day:String(day||''),itemId:String(value_(r,h.item_id)||''),
      status:String(value_(r,h.status)||''),methodology:String(value_(r,h.methodology)||''),coreIds:String(value_(r,h.core_ids)||'')};
  });
}

function beginRefreshSchedule_(sheet,items,core,provider,now){
  const day=serpApiCalendarKeys_(now).day;
  const existing={};
  readRefreshSchedule_(sheet).forEach(function(r){if(r.day===day)existing[r.itemId]=true;});
  const selected=items.filter(function(item){return !existing[item.id];});
  const first=sheet.getLastRow()+1;
  const fingerprint=core.map(function(item){return item.id;}).sort().join(',');
  if(selected.length){
    sheet.getRange(first,1,selected.length,REFRESH_HEADER.length).setValues(selected.map(function(item){
      return[day,item.id,provider,'scheduled',now,'','pending',RELIABILITY_METHOD,fingerprint];
    }));
    SpreadsheetApp.flush(); // A request must never run before its denominator is durable.
  }
  return selected.map(function(item,i){return{itemId:item.id,row:first+i};});
}

function finishRefreshSchedule_(sheet,entries,results,now){
  const byId={};results.forEach(function(r){byId[r.itemId]=r;});
  entries.forEach(function(entry){
    const result=byId[entry.itemId];
    const success=Boolean(result&&result.valid&&!result.isStale);
    sheet.getRange(entry.row,4).setValue(success?'success':'failed');
    sheet.getRange(entry.row,6,1,2).setValues([[now,success?'validated':(result&&result.failReason||'no_valid_candidates')]]);
  });
}

function scheduledReliability_(snapshots,itemSeries,coreIds,opportunities,now){
  const current=now instanceof Date?now:new Date(now||Date.now());
  const keys=serpApiCalendarKeys_(current),dayMs=86400000;
  const ids=coreIds.slice().sort(),fingerprint=ids.join(','),expected=ids.length;
  const core={};ids.forEach(function(id){core[id]=true;});
  const ledger=(opportunities||[]).filter(function(r){return core[r.itemId]&&r.methodology===RELIABILITY_METHOD&&r.coreIds===fingerprint&&/^\d{4}-\d{2}-\d{2}$/.test(r.day)&&r.day<=keys.day;});
  const firstDay=ledger.map(function(r){return r.day;}).sort()[0]||'';
  const trackingDays=firstDay?Math.floor((Date.parse(keys.day)-Date.parse(firstDay))/dayMs)+1:0;
  const windowStart=new Date(Date.parse(keys.day)-29*dayMs).toISOString().slice(0,10);
  const unique={};
  ledger.forEach(function(r){
    if(r.day<windowStart)return;
    const key=r.day+'|'+r.itemId;
    // Duplicate outcomes must not inflate the denominator or convert a failed opportunity to success.
    if(!unique[key])unique[key]=r;
  });
  const rows=Object.keys(unique).map(function(key){return unique[key];});
  const daily={},perItem={};
  rows.forEach(function(r){daily[r.day]=(daily[r.day]||0)+1;perItem[r.itemId]=(perItem[r.itemId]||0)+1;});
  const expectedRefreshes=rows.length,successfulRefreshes=rows.filter(function(r){return r.status==='success';}).length;
  const refreshSuccessPct=expectedRefreshes?successfulRefreshes/expectedRefreshes*100:0;
  // Rate alone is insufficient: require a daily record and enough opportunities for every item.
  const scheduledDays=Object.keys(daily).length;
  const scheduleComplete=scheduledDays===30&&ids.every(function(id){return (perItem[id]||0)>=15;});
  let currentItems=0,missingItems=0;
  ids.forEach(function(id){
    const series=(itemSeries||[]).filter(function(s){return s.id===id;})[0];
    const history=(series&&series.history||[]).filter(function(r){return new Date(r.ts).getTime()<=current.getTime();}).slice().sort(function(a,b){return new Date(a.ts)-new Date(b.ts);});
    const latest=history[history.length-1];
    if(!latest||!finitePositive_(latest.usd)){missingItems++;return;}
    const fresh=history.filter(function(r){return !r.isStale&&finitePositive_(r.usd);}).pop();
    if(fresh&&current.getTime()-new Date(fresh.ts).getTime()<=48*3600000)currentItems++;
  });
  const latestSnapshot=(snapshots||[]).filter(function(s){return new Date(s.ts).getTime()<=current.getTime();}).slice().sort(function(a,b){return new Date(a.ts)-new Date(b.ts);}).pop();
  if(latestSnapshot){
    missingItems=Math.max(missingItems,Math.min(expected,Number(latestSnapshot.missingCount)||0));
    currentItems=Math.min(currentItems,expected-missingItems);
  }
  const currentCoveragePct=expected?currentItems/expected*100:0,mature=trackingDays>=30;
  const aEligible=mature&&scheduleComplete&&expected>0&&currentItems===expected&&missingItems===0&&refreshSuccessPct>=95;
  const score=Math.min(currentCoveragePct,refreshSuccessPct);
  const grade=!mature?'Building':aEligible?'A':score>=80?'B':score>=60?'C':score>=40?'D':'F';
  return{methodology:RELIABILITY_METHOD,trackingStartDate:firstDay,trackingDays:trackingDays,requiredTrackingDays:30,mature:mature,
    currentWindowHours:48,currentItems:currentItems,expectedItems:expected,outdatedItems:Math.max(0,expected-currentItems-missingItems),missingItems:missingItems,
    currentCoveragePct:currentCoveragePct,windowDays:Math.min(30,trackingDays),scheduledDays:scheduledDays,scheduleComplete:scheduleComplete,
    successfulRefreshes:successfulRefreshes,expectedRefreshes:expectedRefreshes,refreshSuccessPct:refreshSuccessPct,grade:grade,
    limitingMetrics:[!mature?'30_day_track_record':'',!scheduleComplete?'schedule_coverage':'',currentItems!==expected?'current_48h_coverage':'',missingItems?'missing_items':'',refreshSuccessPct<95?'scheduled_success_below_95':''].filter(Boolean),
    aRequirements:{minimumDays:30,currentItemsRequired:expected,maximumAgeHours:48,minimumRefreshSuccessPct:95,missingItemsAllowed:0,scheduledDays:30,minimumOpportunitiesPerItem:15}};
}
