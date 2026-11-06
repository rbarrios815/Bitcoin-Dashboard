/** Independent, read-only monthly benchmark. Never calls shopping providers or writes Sheets. */
const FOOD_CPI_SERIES = 'CUUR0000SAF11';
function parseFoodCpi_(data, now) {
  const series = data && data.Results && data.Results.series;
  if (data.status !== 'REQUEST_SUCCEEDED' || !series || series.length !== 1 || series[0].seriesID !== FOOD_CPI_SERIES) throw new Error('Unexpected BLS series response');
  const currentMonth = now.toISOString().slice(0,7), seen = {};
  (series[0].data || []).forEach(function(p) {
    if (!/^M(0[1-9]|1[0-2])$/.test(p.period) || !/^\d{4}$/.test(String(p.year))) return;
    if (p.value === '-' || p.value === '.') return; // BLS explicitly missing; never interpolate.
    const month = p.year+'-'+p.period.slice(1), value = Number(p.value);
    if (month >= currentMonth || !finitePositive_(value)) throw new Error('Invalid or unreleased BLS observation');
    if (seen[month]) throw new Error('Duplicate BLS month');
    seen[month] = {month:month,cpi:value};
  });
  const points = Object.keys(seen).sort().map(function(k){return seen[k];});
  if (!points.length) throw new Error('No monthly BLS observations');
  return {seriesId:FOOD_CPI_SERIES,points:points,retrievedAt:now.toISOString(),releaseDate:null,
    sourceUrl:'https://fred.stlouisfed.org/series/'+FOOD_CPI_SERIES,
    apiUrl:'https://api.bls.gov/publicAPI/v2/timeseries/data/'+FOOD_CPI_SERIES,
    releaseCalendarUrl:'https://www.bls.gov/schedule/news_release/cpi.htm'};
}
function getFoodAtHomeBenchmark() {
  try {
    const now = new Date(), year = now.getUTCFullYear(), cache = CacheService.getScriptCache();
    const key = 'FOOD_CPI_V1_'+year, cached = cache.get(key);
    if (cached) return JSON.parse(cached);
    const response = UrlFetchApp.fetch('https://api.bls.gov/publicAPI/v2/timeseries/data/'+FOOD_CPI_SERIES+'?startyear='+(year-9)+'&endyear='+year,{muteHttpExceptions:true});
    if (response.getResponseCode() !== 200) throw new Error('BLS HTTP '+response.getResponseCode());
    const result = parseFoodCpi_(JSON.parse(response.getContentText()),now);
    cache.put(key,JSON.stringify(result),21600);
    return result;
  } catch (e) {
    // No cached-success fallback presented as a fresh retrieval.
    return {seriesId:FOOD_CPI_SERIES,points:[],error:'Official food benchmark unavailable. Try again later.'};
  }
}
