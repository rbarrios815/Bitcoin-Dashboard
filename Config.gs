function getProps_(){
  const sp=PropertiesService.getScriptProperties();
  const key=function(a,b,c){return [a,b,c].filter(Boolean).join('_');};
  return {
    rapidApiKey:sp.getProperty(key('RAPIDAPI','KEY'))||'',
    priceApiHost:sp.getProperty(key('PRICE','API','HOST'))||'',
    priceApiSearchUrl:sp.getProperty(key('PRICE','API','SEARCH_URL'))||'',
    dataSheetName:sp.getProperty(key('DATA','SHEET','NAME'))||'GroceryPriceHistory',
    rawOffersSheetName:sp.getProperty(key('RAW','OFFERS','SHEET_NAME'))||'RawOffers',
    itemList:sp.getProperty(key('ITEM','LIST'))||'',
    countryCode:sp.getProperty(key('COUNTRY','CODE'))||'us',
    excludeDomains:sp.getProperty(key('EXCLUDE','DOMAINS'))||'',
    btcCacheMinutes:Number(sp.getProperty(key('BTC','CACHE','MINUTES'))||30),
    allowStaleFallback:String(sp.getProperty(key('ALLOW','STALE','FALLBACK'))||'true').toLowerCase()==='true',
    serpApiKey:sp.getProperty(key('SERPAPI','KEY'))||'',
    serpApiEngine:sp.getProperty(key('SERPAPI','ENGINE'))||'google_shopping',
    serpApiLocation:sp.getProperty(key('SERPAPI','LOCATION'))||'',
    serpApiNoCache:sp.getProperty(key('SERPAPI','NO_CACHE'))||'',
    serpApiMonthlyBudget:integerProp_(sp,key('SERPAPI','MONTHLY','BUDGET'),220,0),
    serpApiMaxSearchesPerDay:integerProp_(sp,key('SERPAPI','MAX_SEARCHES','PER_DAY'),6,0)
  };
}

function integerProp_(sp,key,defaultValue,minimum){
  const raw=sp.getProperty(key);
  if(raw===null||raw==='')return defaultValue;
  const value=Math.floor(Number(raw));
  return isFinite(value)&&value>=minimum?value:defaultValue;
}

function validateCollectionProps_(p){
  if(!p.serpApiKey&&!(p.rapidApiKey&&p.priceApiHost&&p.priceApiSearchUrl)){
    throw new Error('Configure a supported shopping-price provider in Script Properties.');
  }
}
