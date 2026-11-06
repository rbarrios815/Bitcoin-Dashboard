const SERPAPI_USAGE_MONTH_KEY='SERPAPI_USAGE_MONTH';
const SERPAPI_USAGE_COUNT_KEY='SERPAPI_USAGE_COUNT';
const SERPAPI_LAST_SEARCH_DATE_KEY='SERPAPI_LAST_SEARCH_DATE';
const SERPAPI_BLOCKED_MONTH_KEY='SERPAPI_BLOCKED_MONTH';

function calculateSerpApiAllowance_(monthlyBudget,used,maxPerDay,itemCount){
  const budget=Math.max(0,Math.floor(Number(monthlyBudget)||0));
  const spent=Math.max(0,Math.floor(Number(used)||0));
  const daily=Math.max(0,Math.floor(Number(maxPerDay)||0));
  const items=Math.max(0,Math.floor(Number(itemCount)||0));
  return Math.max(0,Math.min(items,daily,budget-spent));
}

function selectSerpApiRotation_(items,count,dayNumber){
  const source=Array.isArray(items)?items:[];
  const take=Math.max(0,Math.min(source.length,Math.floor(Number(count)||0)));
  if(!take||!source.length)return[];
  const day=Math.max(0,Math.floor(Number(dayNumber)||0));
  const start=(day*take)%source.length;
  const selected=[];
  for(let i=0;i<take;i++)selected.push(source[(start+i)%source.length]);
  return selected;
}

function serpApiCalendarKeys_(now){
  const date=now instanceof Date?now:new Date(now||Date.now());
  const zone=Session.getScriptTimeZone()||'America/Chicago';
  return{
    month:Utilities.formatDate(date,zone,'yyyy-MM'),
    day:Utilities.formatDate(date,zone,'yyyy-MM-dd'),
    dayNumber:Math.floor(Date.parse(Utilities.formatDate(date,zone,'yyyy-MM-dd')+'T00:00:00Z')/86400000)
  };
}

function serpApiBudgetState_(props,now){
  const sp=PropertiesService.getScriptProperties();
  const keys=serpApiCalendarKeys_(now);
  let used=Math.max(0,Math.floor(Number(sp.getProperty(SERPAPI_USAGE_COUNT_KEY))||0));
  if(sp.getProperty(SERPAPI_USAGE_MONTH_KEY)!==keys.month){
    used=0;
    sp.setProperty(SERPAPI_USAGE_MONTH_KEY,keys.month);
    sp.setProperty(SERPAPI_USAGE_COUNT_KEY,'0');
    sp.deleteProperty(SERPAPI_LAST_SEARCH_DATE_KEY);
    sp.deleteProperty(SERPAPI_BLOCKED_MONTH_KEY);
  }
  return{
    keys:keys,
    used:used,
    monthlyBudget:Math.max(0,Math.floor(Number(props.serpApiMonthlyBudget)||0)),
    maxPerDay:Math.max(0,Math.floor(Number(props.serpApiMaxSearchesPerDay)||0)),
    lastSearchDate:sp.getProperty(SERPAPI_LAST_SEARCH_DATE_KEY)||'',
    blockedMonth:sp.getProperty(SERPAPI_BLOCKED_MONTH_KEY)||''
  };
}

// Core and references have independent cursors. A reference never displaces a grocery.
function selectGroceryPlan_(items,props,state,cursor,referenceCursor,lastReferenceDay){
  const core=(items||[]).filter(isCoreItem_);
  const references=(items||[]).filter(function(item){return !isCoreItem_(item);});
  const dailyCore=Math.min(core.length,props.serpApiCoreSearchesPerDay===undefined?6:props.serpApiCoreSearchesPerDay,state.maxPerDay);
  const allowance=calculateSerpApiAllowance_(state.monthlyBudget,state.used,state.maxPerDay,items.length);
  const take=Math.min(dailyCore,allowance);
  const selected=[];
  for(let i=0;i<take;i++)selected.push(core[(cursor+i)%core.length]);
  const parts=state.keys.day.split('-').map(Number);
  const remainingDays=new Date(Date.UTC(parts[0],parts[1],0)).getUTCDate()-parts[2];
  // Reserve all remaining core days before spending surplus on references.
  const surplus=Math.max(0,state.monthlyBudget-state.used-take-remainingDays*dailyCore);
  const due=lastReferenceDay===null||state.keys.dayNumber-lastReferenceDay>=(props.serpApiReferenceIntervalDays||7);
  const referenceTake=due?Math.min(references.length,allowance-take,surplus):0;
  for(let i=0;i<referenceTake;i++)selected.push(references[(referenceCursor+i)%references.length]);
  return{items:selected,coreCount:take,referenceCount:referenceTake};
}

function planSerpApiRequests_(items,props,now){
  const state=serpApiBudgetState_(props,now);
  if(state.blockedMonth===state.keys.month)return{items:[],reason:'provider_quota_exhausted',state:state};
  if(state.lastSearchDate===state.keys.day)return{items:[],reason:'already_searched_today',state:state};
  const sp=PropertiesService.getScriptProperties();
  const cursor=Number(sp.getProperty('SERPAPI_CORE_CURSOR')||0);
  const refCursor=Number(sp.getProperty('SERPAPI_REFERENCE_CURSOR')||0);
  const lastRef=sp.getProperty('SERPAPI_REFERENCE_DAY');
  const plan=selectGroceryPlan_(items,props,state,cursor,refCursor,lastRef===null?null:Number(lastRef));
  if(!plan.items.length)return{items:[],reason:'local_budget_reached',state:state};
  // Conservative reservation: crashes consume the local allowance rather than enable duplicate calls.
  sp.setProperty(SERPAPI_USAGE_COUNT_KEY,String(state.used+plan.items.length));
  sp.setProperty(SERPAPI_LAST_SEARCH_DATE_KEY,state.keys.day);
  sp.setProperty('SERPAPI_CORE_CURSOR',String(cursor+plan.coreCount));
  sp.setProperty('SERPAPI_REFERENCE_CURSOR',String(refCursor+plan.referenceCount));
  if(plan.referenceCount)sp.setProperty('SERPAPI_REFERENCE_DAY',String(state.keys.dayNumber));
  state.used+=plan.items.length;
  return{items:plan.items,reason:'scheduled_rotation',state:state};
}

function isSerpApiExhaustedResponse_(status,body){
  if(Number(status)===429)return true;
  const text=String(body||'').toLowerCase();
  return /search(?:es)?\s+(?:are\s+)?exhausted|used\s+up\s+all\s+(?:of\s+)?your\s+searches|monthly\s+(?:search\s+)?limit|quota\s+(?:has\s+been\s+)?(?:exhausted|reached)/.test(text);
}

function markSerpApiExhausted_(now){
  const keys=serpApiCalendarKeys_(now);
  PropertiesService.getScriptProperties().setProperty(SERPAPI_BLOCKED_MONTH_KEY,keys.month);
}

function getSerpApiBudgetStatus(){
  const props=getProps_();
  const state=serpApiBudgetState_(props,new Date());
  return{
    month:state.keys.month,
    usedByDashboard:state.used,
    monthlyBudget:state.monthlyBudget,
    remaining:Math.max(0,state.monthlyBudget-state.used),
    maxSearchesPerDay:state.maxPerDay,
    lastSearchDate:state.lastSearchDate,
    providerBlockedForMonth:state.blockedMonth===state.keys.month
  };
}

