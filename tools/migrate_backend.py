#!/usr/bin/env python3
from pathlib import Path
import re

P = Path('Code.gs')
s = P.read_text()

def lit(old, new, name):
    global s
    if s.count(old) != 1: raise RuntimeError(f'{name}: {s.count(old)} matches')
    s = s.replace(old, new, 1)

def rx(pattern, new, name):
    global s
    s2, n = re.subn(pattern, new, s, count=1, flags=re.S)
    if n != 1: raise RuntimeError(f'{name}: {n} matches')
    s = s2

lit(".setTitle('Commodity Price Tracker')", ".setTitle('Bitcoin Purchasing Power Dashboard')", 'title')
lit("const FIXED_BASKET_ITEMS_ = {\n  cash10: { usd: 10 },\n  sats10000: { sats: 10000 }\n};", """const FIXED_BASKET_ITEMS_ = {
  cash10: { usd: 10 },
  sats10000: { sats: 10000 }
};

const NORMALIZED_PRICE_BOUNDS_ = {
  apples:[0.25,8], bananas:[0.15,4], eggs:[0.05,2], milk:[1,15],
  butter:[0.08,2], bread:[0.03,1.5], rice:[0.15,8], chicken:[1,20],
  ground_beef:[2,30], potatoes:[0.10,5], yellow_onions:[0.10,5], salt:[0.01,1]
};
const BLS_ELECTRICITY_SERIES_ID_ = 'APU000072610';
const BLS_ELECTRICITY_SERIES_URL_ = 'https://data.bls.gov/timeseries/APU000072610';""", 'constants')

rx(r"function basketWeightForItemId_\(itemId\) \{.*?\n\}", """function basketWeightForItemId_(itemId) {
  return isReferenceItemId_(itemId) ? 0 : 1;
}""", 'weight')

rx(r"function computeWeightedBasketIndex_\(items\) \{.*?\n\}\n\nfunction computeCanonicalBasketForSnapshot_", """function computeWeightedBasketIndex_(items) {
  let weightedUsdTotal = 0, weightedSatsTotal = 0, totalWeight = 0, includedCount = 0;
  (items || []).forEach(item => {
    const itemId = item && item.id;
    const usd = Number(item && item.usd), sats = Number(item && item.sats), weight = Number(item && item.weight);
    const status = String(item && item.validation_status || '').toLowerCase();
    if (status && status !== 'validated' && !isFixedBasketItemId_(itemId)) return;
    if (!isFinite(usd) || usd <= 0 || !isFinite(sats) || sats <= 0 || !isFinite(weight) || weight <= 0) return;
    weightedUsdTotal += usd * weight;
    weightedSatsTotal += sats * weight;
    totalWeight += weight;
    includedCount += 1;
  });
  return { usd: weightedUsdTotal, sats: weightedSatsTotal, totalWeight, includedCount };
}

function computeCanonicalBasketForSnapshot_""", 'basket total')
lit("const opts = Object.assign({ allowedItemIds: null, injectFixedItems: true, btcUsd: null }, options || {});", "const opts = Object.assign({ allowedItemIds: null, injectFixedItems: false, btcUsd: null }, options || {});", 'defaults')
lit("""    includedRows.push({
      id: itemId,
      usd: Number(row && row.usd),
      sats: Number(row && row.sats),
      btcUsd: btcUsd,
      validation_status: row && row.validation_status,
      weight: basketWeightForItemId_(itemId)
    });
    if (!seenIds[itemId]) {
      seenIds[itemId] = true;
      includedItemIds.push(itemId);
    }""", """    const weight = basketWeightForItemId_(itemId);
    includedRows.push({ id:itemId, usd:Number(row && row.usd), sats:Number(row && row.sats), btcUsd, validation_status:row && row.validation_status, weight });
    if (!seenIds[itemId]) {
      seenIds[itemId] = true;
      if (weight > 0) includedItemIds.push(itemId);
    }""", 'included ids')
lit('includedCount: includedItemIds.length,', 'includedCount: weighted.includedCount,', 'included count')

lit("const fetchableItems = items.filter(item => !isFixedBasketItemId_(item.id));", "const fetchableItems = items.filter(item => !isFixedBasketItemId_(item.id) && String(item.id || '').toLowerCase() !== 'mwh');", 'fetchable')
lit("""    if (isFixedBasketItemId_(item.id)) {
      const fixed = getFixedBasketValue_(item.id, btcUsd);
      return buildFixedSnapshotItem_(item, fixed, snapshotTs);
    }

    const itemResults = [rapid.byId[item.id], serpById[item.id]];""", """    if (isFixedBasketItemId_(item.id)) {
      const fixed = getFixedBasketValue_(item.id, btcUsd);
      return buildFixedSnapshotItem_(item, fixed, snapshotTs);
    }
    if (String(item.id || '').toLowerCase() === 'mwh') {
      try { return buildElectricitySnapshotItem_(item, fetchElectricityBenchmark_(), btcUsd, snapshotTs); }
      catch (e) {
        if (props.allowStaleFallback && historySheet) {
          const last = getLastKnownValidatedRow_(item.id, historySheet);
          if (last && isFinite(last.usd) && last.usd > 0) return buildFallbackSnapshotItem_(item, last, btcUsd, snapshotTs);
        }
        return buildErrorSnapshotItem_(item, { error:'bls_electricity_fetch_failed' }, snapshotTs);
      }
    }

    const itemResults = [rapid.byId[item.id], serpById[item.id]];""", 'electricity branch')

lit("excluded_keywords: ['salted', 'spread', 'margarine']", "excluded_keywords: ['spread', 'margarine']", 'butter catalog')
lit("if (item.id === 'butter' && /salted/.test(title)) return { ok: false, reason: 'salted_butter' };", "if (item.id === 'butter' && /(^|[^a-z])salted([^a-z]|$)/.test(title)) return { ok: false, reason: 'salted_butter' };", 'butter validation')

lit("""function selectTopPassingOffersForAggregation_(offers, maxSources) {
  const maxCount = Math.max(1, Number(maxSources || 5));
  const ranked = rankPassingCandidateOffers_(offers);
  const byVendor = {};
  const selected = [];
  ranked.forEach(offer => {
    if (selected.length >= maxCount) return;
    const vendorKey = String(offer && offer.vendor || '').trim().toLowerCase() || ('_unknown_vendor_' + selected.length);
    if (byVendor[vendorKey]) return;
    byVendor[vendorKey] = true;
    selected.push(offer);
  });
  return selected;
}""", """function selectTopPassingOffersForAggregation_(offers, maxSources) {
  const maxCount = Math.max(1, Number(maxSources || 5));
  const ranked = rankPassingCandidateOffers_(offers), byVendor = {}, selected = [];
  ranked.forEach(offer => {
    if (selected.length >= maxCount) return;
    const vendorKey = String(offer && offer.vendor || '').trim().toLowerCase() || ('_unknown_vendor_' + selected.length);
    if (byVendor[vendorKey]) return;
    byVendor[vendorKey] = true;
    selected.push(offer);
  });
  return selected;
}

function filterOutlierCandidateOffers_(offers) {
  const rows = (offers || []).filter(x => isFinite(Number(x && x.normalized_price)) && Number(x.normalized_price) > 0);
  if (rows.length < 3) return rows;
  const prices = rows.map(x => Number(x.normalized_price)), center = median_(prices);
  const mad = median_(prices.map(x => Math.abs(x - center))), band = Math.max(center * 0.50, mad * 3);
  const filtered = rows.filter(x => Number(x.normalized_price) >= Math.max(0, center - band) && Number(x.normalized_price) <= center + band);
  return filtered.length >= 2 ? filtered : rows;
}""", 'outlier helper')
lit("""  const passing = getPassingCandidateOffersForItem_(results);
  const deduped = dedupeCandidateOffers_(passing);
  const selected = selectTopPassingOffersForAggregation_(deduped, 5);""", """  const passing = getPassingCandidateOffersForItem_(results);
  const deduped = dedupeCandidateOffers_(passing);
  const selected = filterOutlierCandidateOffers_(selectTopPassingOffersForAggregation_(deduped, 5));""", 'outlier use')

lit("function validateCandidate_(item, candidate, source) {", """function isDisallowedMarketplaceOffer_(item, candidate) {
  if (isReferenceItemId_(item && item.id)) return false;
  const text = [candidate && candidate.vendor, candidate && candidate.source_url].map(x => String(x || '').toLowerCase()).join(' ');
  return /(ebay|etsy|whatnot|shop\\s*lc|alibaba|aliexpress|temu)/i.test(text);
}
function validateNormalizedPriceBounds_(item, value) {
  const b = NORMALIZED_PRICE_BOUNDS_[String(item && item.id || '').trim().toLowerCase()];
  if (!b || !isFinite(value)) return { ok:true };
  return value >= b[0] && value <= b[1] ? { ok:true } : { ok:false, reason:'implausible_unit_price' };
}

function validateCandidate_(item, candidate, source) {""", 'candidate helpers')
lit("""  const normalizedPrice = computeNormalizedPrice_(item, candidate.raw_price, parsed);
  const failReasons = [];""", """  const normalizedPrice = computeNormalizedPrice_(item, candidate.raw_price, parsed);
  const priceBounds = validateNormalizedPriceBounds_(item, normalizedPrice);
  const failReasons = [];""", 'bounds setup')
lit("""  if (!quantityCheck.ok) failReasons.push(quantityCheck.reason);
  if (!isFinite(normalizedPrice) || normalizedPrice <= 0) failReasons.push('cannot_normalize_price');""", """  if (!quantityCheck.ok) failReasons.push(quantityCheck.reason);
  if (!isFinite(normalizedPrice) || normalizedPrice <= 0) failReasons.push('cannot_normalize_price');
  if (!priceBounds.ok) failReasons.push(priceBounds.reason);
  if (isDisallowedMarketplaceOffer_(item, candidate)) failReasons.push('marketplace_not_allowed');""", 'policy checks')

bls = """function parseBlsElectricityResponse_(data) {
  const series = data && data.Results && Array.isArray(data.Results.series) ? data.Results.series : [];
  const points = series.length && Array.isArray(series[0].data) ? series[0].data : [];
  const point = points[0], usdPerKwh = Number(point && point.value);
  if (!point || !isFinite(usdPerKwh) || usdPerKwh <= 0) throw new Error('Invalid BLS electricity response');
  return { usdPerKwh, period:[point.periodName || point.period || '', point.year || ''].filter(Boolean).join(' '), sourceUrl:BLS_ELECTRICITY_SERIES_URL_ };
}
function fetchElectricityBenchmark_() {
  const cache = CacheService.getScriptCache(), key = 'BLS_ELECTRICITY_' + BLS_ELECTRICITY_SERIES_ID_;
  const cached = cache.get(key); if (cached) return JSON.parse(cached);
  const url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/' + BLS_ELECTRICITY_SERIES_ID_ + '?latest=true';
  const response = UrlFetchApp.fetch(url, { method:'get', muteHttpExceptions:true });
  if (response.getResponseCode() !== 200) throw new Error('BLS electricity HTTP ' + response.getResponseCode());
  const parsed = parseBlsElectricityResponse_(JSON.parse(response.getContentText() || '{}'));
  safeCachePutJson_(cache, key, parsed, 21600); return parsed;
}
function buildElectricitySnapshotItem_(item, b, btcUsd, ts) {
  const usd = Number(b.usdPerKwh) * Number(item.target_quantity || 5);
  return { id:item.id, name:item.name, query:item.query, canonical_query:item.canonical_query, item_description:item.canonical_description,
    raw_vendor_title:'BLS U.S. city average electricity per kWh' + (b.period ? ' (' + b.period + ')' : ''), source_item_description:'Official monthly electricity benchmark', ts,
    usd, sats:usdToSats_(usd, btcUsd), tracked_quantity:item.target_quantity, tracked_unit:item.target_unit, source_url:b.sourceUrl,
    price_source:'bls_average_price', price_vendor:'U.S. Bureau of Labor Statistics', is_stale:false, validation_status:'validated', match_score:100,
    normalized_price:Number(b.usdPerKwh), normalized_unit:'kwh', fail_reason:'', matched_source_count:1, used_source_count:1,
    aggregation_method:'official_monthly_benchmark', selected_vendors:'U.S. Bureau of Labor Statistics', selected_source_urls:b.sourceUrl,
    selected_match_scores:'100', aggregated_normalized_prices:String(b.usdPerKwh), is_multi_source:false, selected_offer_keys:[] };
}

"""
lit("/* =========================\n   Trigger installation\n   ========================= */", bls + "/* =========================\n   Trigger installation\n   ========================= */", 'BLS functions')

lit("""function convertElectricityUsdToKwh_(usd, itemId, itemName, itemDescription) {
  const numeric = safeNumber_(usd);
  if (!isFinite(numeric)) return NaN;
  return isElectricityItemMeta_(itemId, itemName, itemDescription) ? (numeric / 1000) : numeric;
}""", """function convertElectricityUsdToKwh_(usd, itemId, itemName, itemDescription) {
  const numeric = safeNumber_(usd);
  if (!isFinite(numeric)) return NaN;
  if (!isElectricityItemMeta_(itemId, itemName, itemDescription)) return numeric;
  return numeric > 25 ? (numeric / 1000) : numeric;
}""", 'electricity migration')

P.write_text(s)
print('backend migrated')
