function fetchShoppingCandidates_(items, props) {
  const byId = {};
  items.forEach(function(item){ byId[item.id] = []; });
  if (props.serpApiKey) {
    const now = new Date();
    const plan = planSerpApiRequests_(items, props, now);
    const serpItems = plan.items;
    const requests = serpItems.map(function(item) {
      let url = 'https://serpapi.com/search.json?engine=' + encodeURIComponent(props.serpApiEngine || 'google_shopping') + '&q=' + encodeURIComponent(item.query) + '&api_key=' + encodeURIComponent(props.serpApiKey) + '&num=20';
      if (props.serpApiLocation) url += '&location=' + encodeURIComponent(props.serpApiLocation);
      if (String(props.serpApiNoCache).toLowerCase() === 'true') url += '&no_cache=true';
      return {url:url,muteHttpExceptions:true};
    });
    if (requests.length) UrlFetchApp.fetchAll(requests).forEach(function(response,i) {
      try {
        const status = response.getResponseCode();
        const body = response.getContentText() || '';
        if (isSerpApiExhaustedResponse_(status,body)) markSerpApiExhausted_(now);
        if (status !== 200) return;
        const data = JSON.parse(body || '{}');
        const rows = Array.isArray(data.shopping_results) ? data.shopping_results : [];
        rows.forEach(function(x){ byId[serpItems[i].id].push({provider:'serpapi',title:String(x.title || ''),price:numberFrom_(x.extracted_price || x.price),vendor:String(x.source || ''),url:String(x.product_link || x.link || '')}); });
      } catch (e) {}
    });
  }
  if (props.rapidApiKey && props.priceApiSearchUrl && props.priceApiHost) {
    const requests = items.map(function(item) {
      let url = addQuery_(props.priceApiSearchUrl,'product_title',item.query);
      if (props.countryCode) url = addQuery_(url,'country_code',props.countryCode);
      if (props.excludeDomains) url = addQuery_(url,'exclude_domains',props.excludeDomains);
      return {url:url,muteHttpExceptions:true,headers:{'X-RapidAPI-Key':props.rapidApiKey,'X-RapidAPI-Host':props.priceApiHost}};
    });
    UrlFetchApp.fetchAll(requests).forEach(function(response,i) {
      try {
        if (response.getResponseCode() !== 200) return;
        const data = JSON.parse(response.getContentText() || '{}');
        const rows = Array.isArray(data) ? data : (data.products || data.results || data.items || []);
        rows.slice(0,20).forEach(function(x){ byId[items[i].id].push({provider:'rapidapi',title:String(x.title || x.name || x.description || ''),price:numberFrom_(x.price || x.min_price || x.lowest_price || x.sale_price),vendor:String(x.source || x.seller || x.merchant || x.store || ''),url:String(x.product_url || x.url || x.link || '')}); });
      } catch (e) {}
    });
  }
  return byId;
}

function aggregateCandidates_(item, candidates, btcUsd, ts, rawOffers) {
  const evaluated = (candidates || []).map(function(candidate){ return validateCandidate_(item,candidate); });
  const passing = evaluated.filter(function(row){ return row.pass; });
  const distinct = [];
  const vendorSeen = {};
  passing.sort(function(a,b){ return b.score-a.score || a.normalizedPrice-b.normalizedPrice; }).forEach(function(row) {
    const key = (row.vendor || row.url || row.title).toLowerCase();
    if (vendorSeen[key] || distinct.length >= 5) return;
    vendorSeen[key] = true;
    distinct.push(row);
  });
  const filtered = removeOutliers_(distinct);
  const selectedKeys = {};
  filtered.forEach(function(row){ selectedKeys[row.key] = true; });
  evaluated.forEach(function(row){ rawOffers.push(Object.assign({},row,{itemId:item.id,selected:Boolean(selectedKeys[row.key])})); });
  if (!filtered.length) return {valid:false,failReason:'no_valid_candidates'};
  const unitPrices = filtered.map(function(row){ return row.normalizedPrice; });
  const normalizedPrice = median_(unitPrices);
  const usd = normalizedPrice * item.quantity;
  const vendors = filtered.map(function(row){ return row.vendor; }).filter(Boolean);
  const urls = filtered.map(function(row){ return row.url; }).filter(Boolean);
  return {
    valid:true,itemId:item.id,name:item.name,description:item.description,query:item.query,usd:usd,sats:usdToSats_(usd,btcUsd),
    normalizedPrice:normalizedPrice,unit:item.unit,isStale:false,status:'validated',failReason:'',source:filtered.length>1?'multi_source_median':'single_source_validated',
    vendor:filtered.length>1?filtered.length+'-source aggregate':(filtered[0].vendor || ''),sourceUrl:urls[0] || '',score:filtered[0].score,
    matchedCount:passing.length,usedCount:filtered.length,method:'median_mad',vendors:vendors.join(' | '),urls:urls.join(' | '),
    scores:filtered.map(function(row){return row.score;}).join(' | '),prices:unitPrices.join(' | '),multi:filtered.length>1,title:filtered[0].title,ts:ts.toISOString()
  };
}

function validateCandidate_(item, candidate) {
  const title = String(candidate.title || '').trim();
  const lower = title.toLowerCase();
  const parsed = parseQuantity_(title);
  const reasons = [];
  if (!finitePositive_(candidate.price)) reasons.push('missing_price');
  item.required.forEach(function(word){ if (lower.indexOf(word) < 0) reasons.push('missing_keyword:'+word); });
  item.excluded.forEach(function(word){ if (lower.indexOf(word) >= 0) reasons.push('excluded_keyword:'+word); });
  if (item.id === 'butter' && /(^|[^a-z])salted([^a-z]|$)/i.test(lower)) reasons.push('salted_butter');
  if (item.id === 'chicken' && lower.indexOf('breast') < 0) reasons.push('wrong_cut');
  if (isCoreItem_(item) && MARKETPLACE_RE.test(String(candidate.vendor || '')+' '+String(candidate.url || ''))) reasons.push('marketplace_not_allowed');
  if (!parsed.unit) reasons.push('missing_size');
  if (parsed.unit && normalizeUnit_(parsed.unit) !== normalizeUnit_(item.unit)) reasons.push('unit_mismatch');
  if (finitePositive_(parsed.quantity) && finitePositive_(item.quantity)) {
    const tolerance = item.id === 'bread' ? 0.15 : 0.25;
    if (Math.abs(parsed.quantity-item.quantity)/item.quantity > tolerance) reasons.push('quantity_mismatch');
  }
  const normalizedPrice = parsed.unit && finitePositive_(parsed.quantity) ? Number(candidate.price)/parsed.quantity : NaN;
  if (!finitePositive_(normalizedPrice)) reasons.push('cannot_normalize_price');
  if (item.bounds && finitePositive_(normalizedPrice) && (normalizedPrice < item.bounds[0] || normalizedPrice > item.bounds[1])) reasons.push('implausible_unit_price');
  let score = item.required.reduce(function(sum,word){ return sum + (lower.indexOf(word)>=0?10:0); },0);
  if (normalizeUnit_(parsed.unit) === normalizeUnit_(item.unit)) score += 15;
  if (finitePositive_(parsed.quantity) && finitePositive_(item.quantity)) score += Math.max(0,15-Math.round(Math.abs(parsed.quantity-item.quantity)/item.quantity*100));
  score -= reasons.length*20;
  return {
    key:[String(candidate.vendor||'').toLowerCase(),String(candidate.url||'').toLowerCase(),String(normalizedPrice)].join('|'),
    provider:candidate.provider,title:title,rawPrice:Number(candidate.price),vendor:String(candidate.vendor||''),url:String(candidate.url||''),
    parsedQuantity:parsed.quantity,parsedUnit:parsed.unit,normalizedPrice:normalizedPrice,unit:item.unit,pass:reasons.length===0,
    failReason:reasons.join(';'),score:score
  };
}

function removeOutliers_(rows) {
  if (rows.length < 3) return rows;
  const prices = rows.map(function(row){return row.normalizedPrice;});
  const center = median_(prices);
  const mad = median_(prices.map(function(price){return Math.abs(price-center);}));
  const band = Math.max(center*0.5,mad*3);
  const filtered = rows.filter(function(row){return row.normalizedPrice>=Math.max(0,center-band) && row.normalizedPrice<=center+band;});
  return filtered.length>=2?filtered:rows;
}
