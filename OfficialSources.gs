function fetchElectricityBenchmark_() {
  const cache = CacheService.getScriptCache();
  const cached = cache.get('BLS_ELECTRICITY_V2');
  if (cached) return JSON.parse(cached);
  const response = UrlFetchApp.fetch('https://api.bls.gov/publicAPI/v2/timeseries/data/'+BLS_ELECTRICITY_SERIES+'?latest=true',{muteHttpExceptions:true});
  if (response.getResponseCode() !== 200) throw new Error('BLS HTTP '+response.getResponseCode());
  const data = JSON.parse(response.getContentText() || '{}');
  const series = data && data.Results && data.Results.series && data.Results.series[0];
  const point = series && series.data && series.data[0];
  const usdPerKwh = Number(point && point.value);
  if (!finitePositive_(usdPerKwh)) throw new Error('BLS electricity value unavailable');
  const out = {usdPerKwh:usdPerKwh,period:[point.periodName || point.period || '',point.year || ''].filter(Boolean).join(' '),sourceUrl:BLS_ELECTRICITY_URL};
  cache.put('BLS_ELECTRICITY_V2',JSON.stringify(out),21600);
  return out;
}

function electricityResult_(item, benchmark, btcUsd, ts) {
  const usd = benchmark.usdPerKwh * item.quantity;
  return {valid:true,itemId:item.id,name:item.name,description:item.description,query:item.query,usd:usd,sats:usdToSats_(usd,btcUsd),normalizedPrice:benchmark.usdPerKwh,unit:'kwh',isStale:false,status:'validated',failReason:'',source:'bls_average_price',vendor:'U.S. Bureau of Labor Statistics',sourceUrl:benchmark.sourceUrl,score:100,matchedCount:1,usedCount:1,method:'official_monthly_benchmark',vendors:'U.S. Bureau of Labor Statistics',urls:benchmark.sourceUrl,scores:'100',prices:String(benchmark.usdPerKwh),multi:false,title:'BLS U.S. city average electricity per kWh ('+benchmark.period+')',ts:ts.toISOString()};
}

function fetchBtcUsd_(props) {
  const cache = CacheService.getScriptCache();
  const cached = cache.get('BTC_USD_V2');
  if (cached) return Number(cached);
  const urls = ['https://api.coinbase.com/v2/prices/BTC-USD/spot','https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd'];
  for (let i=0;i<urls.length;i++) {
    try {
      const response=UrlFetchApp.fetch(urls[i],{muteHttpExceptions:true});
      if (response.getResponseCode()!==200) continue;
      const data=JSON.parse(response.getContentText()||'{}');
      const value=i===0?Number(data && data.data && data.data.amount):Number(data && data.bitcoin && data.bitcoin.usd);
      if (finitePositive_(value)) { cache.put('BTC_USD_V2',String(value),Math.max(60,props.btcCacheMinutes*60)); return value; }
    } catch(e) {}
  }
  throw new Error('BTC/USD fetch failed');
}
