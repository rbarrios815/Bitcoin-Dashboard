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
    dayNumber:Math.floor(date.getTime()/86400000)
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

function planSerpApiRequests_(items,props,now){
  const state=serpApiBudgetState_(props,now);
  if(state.blockedMonth===state.keys.month)return{items:[],reason:'provider_quota_exhausted',state:state};
  if(state.lastSearchDate===state.keys.day)return{items:[],reason:'already_searched_today',state:state};
  const allowed=calculateSerpApiAllowance_(state.monthlyBudget,state.used,state.maxPerDay,(items||[]).length);
  if(!allowed)return{items:[],reason:'local_monthly_budget_reached',state:state};
  const selected=selectSerpApiRotation_(items,allowed,state.keys.dayNumber);
  const sp=PropertiesService.getScriptProperties();
  sp.setProperty(SERPAPI_USAGE_COUNT_KEY,String(state.used+selected.length));
  sp.setProperty(SERPAPI_LAST_SEARCH_DATE_KEY,state.keys.day);
  state.used+=selected.length;
  return{items:selected,reason:'scheduled_rotation',state:state};
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
