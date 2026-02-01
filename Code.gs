/***********************
 * Grocery Price Tracker with SerpAPI fallback (Google Apps Script)
 *
 * REQUIRED Script Properties:
 *   RAPIDAPI_KEY
 *   PRICE_API_HOST = product-item-search-price-comparison.p.rapidapi.com
 *   PRICE_API_SEARCH_URL = https://product-item-search-price-comparison.p.rapidapi.com/product_search
 *   DATA_SHEET_NAME = GroceryPriceHistory
 *   ITEM_LIST = apples,bananas,milk,bread,eggs,rice,chicken,ground beef,butter,potatoes
 *
 * OPTIONAL Script Properties:
 *   COUNTRY_CODE = United States
 *   EXCLUDE_DOMAINS = amazon.com
 *   ITEM_CACHE_HOURS = 6
 *   BTC_CACHE_MINUTES = 30
 *   MAX_RESULTS_PER_ITEM = 5
 *   DEBUG = true
 *   ALLOW_STALE_FALLBACK = true
 *   API_RETRY_COUNT = 1
 *
 * SERPAPI Script Properties (for fallback):
 *   SERPAPI_KEY - your SerpAPI key
 *   SERPAPI_ENGINE - (optional) defaults to 'google_shopping'
 *   SERPAPI_LOCATION - (optional) location string (e.g. 'Austin, TX, USA')
 *   SERPAPI_NO_CACHE - (optional) 'true' to bypass SerpAPI cache
 *   SERP_DEBUG - (optional) 'true' to log SerpAPI debug output
 ************************/

/* =========================
   Web app entry
   ========================= */
function doGet() {
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle('Grocery Price Tracker (USD + sats)')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/**
 * RUN ONCE:
 * - validates properties
 * - creates/repairs history sheet header (adds new columns if missing)
 * - installs daily trigger (8 AM)
 * - records an initial snapshot (set recordNow=false if you prefer)
 */
function setupOnce() {
  const props = getProps_();
  validateProps_(props);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = getOrCreateHistorySheet_(ss, props.dataSheetName);

  installDailyTrigger_();

  const recordNow = true;
  let recordResult = null;
  if (recordNow) {
    try {
      recordResult = recordSnapshot();
    } catch (e) {
      // Setup should still succeed even if provider is down
      recordResult = { ok: false, error: String(e) };
    }
  }
  return {
    ok: true,
    sheetName: sheet.getName(),
    triggerInstalled: true,
    recordedNow: recordNow,
    recordResult
  };
}

/* =========================
   Core API: snapshot + history
   ========================= */

/**
 * Fetch latest prices for ITEM_LIST (cached to save quota),
 * plus BTC/USD for sat conversion, WITHOUT writing to sheet.
 *
 * Resilience:
 * - RapidAPI first (batched with fetchAll)
 * - If RapidAPI provider down or errors, falls back to SerpAPI (batched)
 * - If SerpAPI fails or not configured, falls back to last-known prices in sheet (if ALLOW_STALE_FALLBACK)
 */
function fetchLatestSnapshot() {
  const props = getProps_();
  validateProps_(props);

  const cache = CacheService.getScriptCache();
  const cached = cache.get('LATEST_SNAPSHOT_V2');
  if (cached) return JSON.parse(cached);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(props.dataSheetName);

  const btcUsd = fetchBtcUsd_(props);
  const items = parseItems_(props.itemList);

  // 1) RapidAPI batch fetch (includes per-item cache reads/writes)
  const rapid = fetchItemsUsdRapidApiBatch_(items, props);

  // 2) SerpAPI batch fetch only for RapidAPI failures (if configured)
  let serpById = {};
  if (props.serpApiKey) {
    const needSerp = items.filter(it => {
      const rr = rapid.byId[it.id];
      return !rr || rr.error;
    });
    if (needSerp.length) {
      serpById = fetchItemsUsdSerpApiBatch_(needSerp, props);
    }
  }

  // 3) Finalize items with fallback to last-known
  const priced = items.map(it => {
    const rr = rapid.byId[it.id];

    if (rr && isFinite(rr.usd) && rr.usd > 0) {
      const sats = usdToSats_(rr.usd, btcUsd);
      return {
        id: it.id,
        name: it.name,
        query: it.query,
        item_description: rr.description || '',
        usd: rr.usd,
        sats: sats,
        price_source: rr.source,
        is_stale: false
      };
    }

    const sr = serpById[it.id];
    if (sr && isFinite(sr.usd) && sr.usd > 0) {
      const sats = usdToSats_(sr.usd, btcUsd);
      return {
        id: it.id,
        name: it.name,
        query: it.query,
        item_description: sr.description || '',
        usd: sr.usd,
        sats: sats,
        price_source: sr.source,
        is_stale: false
      };
    }

    if (props.allowStaleFallback && sheet) {
      const last = getLastKnownUsd_(it.id, sheet);
      if (isFinite(last) && last > 0) {
        const sats = usdToSats_(last, btcUsd);
        return {
          id: it.id,
          name: it.name,
          query: it.query,
          item_description: '',
          usd: last,
          sats: sats,
          price_source: 'last_known',
          is_stale: true
        };
      }
    }

    return {
      id: it.id,
      name: it.name,
      query: it.query,
      item_description: '',
      usd: 0,
      sats: 0,
      price_source: (rr && rr.error) ? ('error:' + rr.error) : 'error',
      is_stale: true
    };
  });

  const basketUsd = average_(priced.map(p => p.usd));
  const basketSats = average_(priced.map(p => p.sats));

  const out = {
    ts: new Date().toISOString(),
    btcUsd,
    items: priced,
    basketIndexUsd: basketUsd,
    basketIndexSats: basketSats
  };

  cache.put('LATEST_SNAPSHOT_V2', JSON.stringify(out), 60 * 10);
  return out;
}

/**
 * Write latest snapshot to history sheet (permanent).
 * Appends one row per item.
 */
function recordSnapshot() {
  const props = getProps_();
  validateProps_(props);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = getOrCreateHistorySheet_(ss, props.dataSheetName);

  const snap = fetchLatestSnapshot();
  const ts = new Date(snap.ts);

  const rows = snap.items.map(it => ([
    ts,                    // timestamp
    snap.btcUsd,           // btc_usd
    it.id,                 // item_id
    it.name,               // item_name
    it.query,              // query
    it.item_description,   // item_description
    it.usd,                // usd
    it.sats,               // sats
    snap.basketIndexUsd,   // basket_index_usd
    snap.basketIndexSats,  // basket_index_sats
    it.price_source,       // price_source
    it.is_stale            // is_stale
  ]));

  if (rows.length) {
    sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
  }

  return { ok: true, recordedAt: ts.toISOString(), rowsAppended: rows.length };
}

/* =========================
   UI data helpers
   ========================= */

function getConfig() {
  const props = getProps_();
  return {
    items: parseItems_(props.itemList).map(x => ({ id: x.id, name: x.name, query: x.query })),
    dataSheetName: props.dataSheetName
  };
}

function getItemHistory(itemId) {
  const props = getProps_();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(props.dataSheetName);
  if (!sheet) return [];

  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];

  const header = values[0];
  const idx = headerIndex_(header);
  const out = [];

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    if (String(row[idx.item_id]) !== String(itemId)) continue;
    out.push({
      ts: new Date(row[idx.timestamp]).toISOString(),
      usd: Number(row[idx.usd]),
      sats: Number(row[idx.sats]),
      btcUsd: Number(row[idx.btc_usd]),
      price_source: idx.price_source != null ? String(row[idx.price_source] || '') : '',
      is_stale: idx.is_stale != null ? Boolean(row[idx.is_stale]) : false
    });
  }
  out.sort((a, b) => new Date(a.ts) - new Date(b.ts));
  return out;
}

function getBasketHistory() {
  const props = getProps_();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(props.dataSheetName);
  if (!sheet) return [];

  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];

  const header = values[0];
  const idx = headerIndex_(header);
  const seen = {};
  const out = [];

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const tsIso = new Date(row[idx.timestamp]).toISOString();
    if (seen[tsIso]) continue;
    seen[tsIso] = true;
    out.push({
      ts: tsIso,
      basketIndexUsd: Number(row[idx.basket_index_usd]),
      basketIndexSats: Number(row[idx.basket_index_sats])
    });
  }
  out.sort((a, b) => new Date(a.ts) - new Date(b.ts));
  return out;
}

function getBasketInflation() {
  const hist = getBasketHistory();
  if (hist.length < 2) {
    return {
      baselineTs: null, baselineUsd: null,
      currentTs: null, currentUsd: null,
      inflationPct: 0
    };
  }
  const base = hist[0];
  const cur = hist[hist.length - 1];
  const inflationPct = (base.basketIndexUsd && cur.basketIndexUsd)
    ? ((cur.basketIndexUsd / base.basketIndexUsd) - 1) * 100 : 0;
  return {
    baselineTs: base.ts,
    baselineUsd: base.basketIndexUsd,
    currentTs: cur.ts,
    currentUsd: cur.basketIndexUsd,
    inflationPct: inflationPct
  };
}

/* =========================
   RapidAPI batch fetch
   ========================= */

/**
 * Batch RapidAPI fetch with per-item caching.
 * Returns: { byId: { [itemId]: { usd, source } OR { error, source } } }
 */
function fetchItemsUsdRapidApiBatch_(items, props) {
  const cache = CacheService.getScriptCache();
  const ttl = Math.max(60, Math.min(60 * 60 * 12, props.itemCacheHours * 60 * 60));

  const byId = {};
  const toFetch = [];

  items.forEach(it => {
    const key = cacheKeyForQuery_(it.query);
    const cached = cache.get(key);
    if (cached) {
      let cachedData = null;
      try {
        cachedData = JSON.parse(cached);
      } catch (e) {
        cachedData = null;
      }
      if (cachedData && isFinite(cachedData.usd)) {
        byId[it.id] = { usd: Number(cachedData.usd), description: cachedData.description || '', source: 'cache' };
      } else {
        byId[it.id] = { usd: Number(cached), source: 'cache' };
      }
    } else {
      toFetch.push({ it, key });
    }
  });

  if (!toFetch.length) return { byId };

  const requests = toFetch.map(({ it }) => {
    let url = addParam_(props.priceApiSearchUrl, 'product_title', it.query);
    if (props.countryCode) url = addParam_(url, 'country_code', props.countryCode);
    if (props.excludeDomains) url = addParam_(url, 'exclude_domains', props.excludeDomains);

    return {
      url,
      method: 'get',
      muteHttpExceptions: true,
      headers: {
        'X-RapidAPI-Key': props.rapidApiKey,
        'X-RapidAPI-Host': props.priceApiHost
      }
    };
  });

  const responses = UrlFetchApp.fetchAll(requests);

  for (let i = 0; i < toFetch.length; i++) {
    const { it, key } = toFetch[i];
    const resp = responses[i];

    try {
      const code = resp.getResponseCode();
      const text = resp.getContentText() || '';

      if (code === 502 || code === 503 || code === 504) {
        byId[it.id] = { error: `rapidapi_unreachable_${code}`, source: 'rapidapi' };
        continue;
      }
      if (code >= 400) {
        byId[it.id] = { error: `rapidapi_${code}`, source: 'rapidapi' };
        continue;
      }

      const data = JSON.parse(text);
      const price = extractLowestPrice_(data, props.maxResultsPerItem);
      const description = extractItemDescription_(data, props.maxResultsPerItem);

      if (isFinite(price) && price > 0) {
        cache.put(key, JSON.stringify({ usd: price, description: description }), ttl);
        byId[it.id] = { usd: price, description: description, source: 'rapidapi' };
      } else {
        byId[it.id] = { error: 'rapidapi_no_price', source: 'rapidapi' };
      }
    } catch (e) {
      byId[it.id] = { error: 'rapidapi_parse_error', source: 'rapidapi' };
    }
  }

  return { byId };
}

/* =========================
   SerpAPI batch fetch
   ========================= */

/**
 * Batch SerpAPI fetch for a subset of items (typically RapidAPI failures).
 * Returns: { [itemId]: { usd, source } OR { error, source } }
 */
function fetchItemsUsdSerpApiBatch_(items, props) {
  const cache = CacheService.getScriptCache();
  const ttl = Math.max(60, Math.min(60 * 60 * 12, props.itemCacheHours * 60 * 60));
  const engine = props.serpApiEngine || 'google_shopping';

  const byId = {};

  const requests = items.map(it => {
    const query = encodeURIComponent(it.query);
    let url = 'https://serpapi.com/search?engine=' + encodeURIComponent(engine) +
      '&q=' + query +
      '&api_key=' + encodeURIComponent(props.serpApiKey) +
      '&num=10';

    if (props.serpApiLocation) url += '&location=' + encodeURIComponent(props.serpApiLocation);
    if (String(props.serpApiNoCache).toLowerCase() === 'true') url += '&no_cache=true';

    return { url, method: 'get', muteHttpExceptions: true };
  });

  const responses = UrlFetchApp.fetchAll(requests);

  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    const resp = responses[i];
    const requestUrl = requests[i].url;

    try {
      const code = resp.getResponseCode();
      const text = resp.getContentText() || '';

      if (props.serpDebug) {
        logSerpDebug_(requestUrl, resp);
      }

      if (code >= 400) {
        byId[it.id] = { error: `serpapi_${code}`, source: 'serpapi' };
        continue;
      }

      const data = JSON.parse(text);
      const candidates = [];

      const results = Array.isArray(data.shopping_results) ? data.shopping_results :
        (Array.isArray(data.products) ? data.products :
          (Array.isArray(data.product_results) ? data.product_results : []));

      results.forEach(res => {
        let p = res.extracted_price;
        if (p == null && res.price) p = Number(String(res.price).replace(/[^0-9.]/g, ''));
        if (p != null && isFinite(p) && p > 0) candidates.push(p);

        if (Array.isArray(res.prices)) {
          res.prices.forEach(ofr => {
            let pp = ofr.extracted_price;
            if (pp == null && ofr.price) pp = Number(String(ofr.price).replace(/[^0-9.]/g, ''));
            if (pp != null && isFinite(pp) && pp > 0) candidates.push(pp);
          });
        }
      });

      if (!candidates.length) {
        byId[it.id] = { error: 'serpapi_no_price', source: 'serpapi' };
        continue;
      }

      const price = Math.min.apply(null, candidates);
      const description = extractItemDescription_(data, props.maxResultsPerItem);
      if (isFinite(price) && price > 0) {
        const key = cacheKeyForQuery_(it.query);
        cache.put(key, JSON.stringify({ usd: price, description: description }), ttl);
        byId[it.id] = { usd: price, description: description, source: 'serpapi' };
      } else {
        byId[it.id] = { error: 'serpapi_bad_price', source: 'serpapi' };
      }
    } catch (e) {
      byId[it.id] = { error: 'serpapi_parse_error', source: 'serpapi' };
    }
  }

  return byId;
}

/**
 * Run exactly one SerpAPI request for a query and log debug output if enabled.
 * Does not write to sheets.
 */
function runSerpDebugOnce(itemQuery) {
  const props = getProps_();
  if (!props.serpApiKey) throw new Error('Missing Script Property: SERPAPI_KEY');
  const engine = props.serpApiEngine || 'google_shopping';
  const query = encodeURIComponent(itemQuery || '');
  let url = 'https://serpapi.com/search?engine=' + encodeURIComponent(engine) +
    '&q=' + query +
    '&api_key=' + encodeURIComponent(props.serpApiKey) +
    '&num=10';

  if (props.serpApiLocation) url += '&location=' + encodeURIComponent(props.serpApiLocation);
  if (String(props.serpApiNoCache).toLowerCase() === 'true') url += '&no_cache=true';

  const resp = UrlFetchApp.fetch(url, { method: 'get', muteHttpExceptions: true });
  if (props.serpDebug) {
    logSerpDebug_(url, resp);
  } else {
    Logger.log('SERP_DEBUG is false; no SerpAPI debug output will be logged.');
  }
}

function logSerpDebug_(url, resp) {
  const code = resp.getResponseCode();
  const raw = resp.getContentText() || '';
  Logger.log('SERP_DEBUG status: %s', code);
  Logger.log('SERP_DEBUG url: %s', redactSecrets_(url));
  Logger.log('SERP_DEBUG raw(0-5000): %s', redactSecrets_(raw.slice(0, 5000)));
  try {
    const parsed = JSON.parse(raw);
    const pretty = JSON.stringify(parsed, null, 2);
    Logger.log('SERP_DEBUG json: %s', redactSecrets_(pretty));
  } catch (e) {
    Logger.log('SERP_DEBUG json parse error: %s', e && e.message ? e.message : String(e));
  }
}

/* =========================
   Last-known fallback
   ========================= */

/**
 * Find the last known USD for an itemId by searching upward. Used when providers are down.
 * Assumes your history sheet schema where:
 *   item_id is column 3, usd is column 7 (1-indexed)
 */
function getLastKnownUsd_(itemId, sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;

  const ITEM_ID_COL = 3;
  const USD_COL = 7;

  const chunkSize = 200;
  for (let end = lastRow; end >= 2; end -= chunkSize) {
    const start = Math.max(2, end - chunkSize + 1);
    const values = sheet.getRange(start, 1, end - start + 1, Math.max(USD_COL, ITEM_ID_COL)).getValues();
    for (let i = values.length - 1; i >= 0; i--) {
      const row = values[i];
      if (String(row[ITEM_ID_COL - 1]) === String(itemId)) {
        const usd = Number(row[USD_COL - 1]);
        if (isFinite(usd) && usd > 0) return usd;
      }
    }
  }
  return null;
}

/* =========================
   BTC/USD fetch (Coinbase + CoinGecko fallback)
   ========================= */
function fetchBtcUsd_(props) {
  const cache = CacheService.getScriptCache();
  const ttl = Math.max(60, Math.min(60 * 60, props.btcCacheMinutes * 60));
  const cached = cache.get('BTC_USD_V2');
  if (cached) return Number(cached);

  // Coinbase
  try {
    const resp = UrlFetchApp.fetch('https://api.coinbase.com/v2/prices/BTC-USD/spot', { muteHttpExceptions: true });
    if (resp.getResponseCode() === 200) {
      const data = JSON.parse(resp.getContentText());
      const rate = Number(data && data.data && data.data.amount);
      if (isFinite(rate) && rate > 0) {
        cache.put('BTC_USD_V2', String(rate), ttl);
        return rate;
      }
    }
  } catch (e) {}

  // CoinGecko
  try {
    const resp = UrlFetchApp.fetch('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd', { muteHttpExceptions: true });
    if (resp.getResponseCode() === 200) {
      const data = JSON.parse(resp.getContentText());
      const rate = Number(data && data.bitcoin && data.bitcoin.usd);
      if (isFinite(rate) && rate > 0) {
        cache.put('BTC_USD_V2', String(rate), ttl);
        return rate;
      }
    }
  } catch (e) {}

  throw new Error('BTC/USD fetch failed (Coinbase + CoinGecko).');
}

/* =========================
   Trigger installation
   ========================= */
function installDailyTrigger_() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'recordSnapshotDaily') ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger('recordSnapshotDaily').timeBased().everyDays(1).atHour(8).create();
}
function recordSnapshotDaily() { return recordSnapshot(); }

/* =========================
   Sheet helpers
   ========================= */
function getOrCreateHistorySheet_(ss, name) {
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);

  const desiredHeader = [
    'timestamp', 'btc_usd', 'item_id', 'item_name', 'query', 'item_description', 'usd', 'sats',
    'basket_index_usd', 'basket_index_sats', 'price_source', 'is_stale'
  ];

  if (sh.getLastRow() === 0) {
    sh.appendRow(desiredHeader);
    sh.setFrozenRows(1);
    return sh;
  }

  const lastCol = Math.max(sh.getLastColumn(), desiredHeader.length);
  const header = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(String);

  if (String(header[0]).trim().toLowerCase() !== 'timestamp') {
    sh.insertRowBefore(1);
    sh.getRange(1, 1, 1, desiredHeader.length).setValues([desiredHeader]);
    sh.setFrozenRows(1);
    return sh;
  }

  for (let c = 0; c < desiredHeader.length; c++) {
    const desired = desiredHeader[c];
    const existing = header[c] ? String(header[c]).trim() : '';
    if (existing !== desired) {
      sh.getRange(1, c + 1).setValue(desired);
    }
  }

  sh.setFrozenRows(1);
  return sh;
}

function headerIndex_(headerRow) {
  const m = {};
  headerRow.forEach((h, i) => m[String(h).trim()] = i);
  return {
    timestamp: m.timestamp,
    btc_usd: m.btc_usd,
    item_id: m.item_id,
    item_name: m.item_name,
    query: m.query,
    item_description: m.item_description,
    usd: m.usd,
    sats: m.sats,
    basket_index_usd: m.basket_index_usd,
    basket_index_sats: m.basket_index_sats,
    price_source: m.price_source !== undefined ? m.price_source : null,
    is_stale: m.is_stale !== undefined ? m.is_stale : null
  };
}

/* =========================
   Properties + utilities
   ========================= */
function getProps_() {
  const sp = PropertiesService.getScriptProperties();
  return {
    rapidApiKey: sp.getProperty('RAPIDAPI_KEY'),
    priceApiHost: sp.getProperty('PRICE_API_HOST'),
    priceApiSearchUrl: sp.getProperty('PRICE_API_SEARCH_URL'),
    dataSheetName: sp.getProperty('DATA_SHEET_NAME') || 'GroceryPriceHistory',
    itemList: sp.getProperty('ITEM_LIST') || '',
    countryCode: sp.getProperty('COUNTRY_CODE') || '',
    excludeDomains: sp.getProperty('EXCLUDE_DOMAINS') || '',
    itemCacheHours: Number(sp.getProperty('ITEM_CACHE_HOURS') || 6),
    btcCacheMinutes: Number(sp.getProperty('BTC_CACHE_MINUTES') || 30),
    maxResultsPerItem: Number(sp.getProperty('MAX_RESULTS_PER_ITEM') || 5),
    debug: String(sp.getProperty('DEBUG') || '').toLowerCase() === 'true',
    allowStaleFallback: String(sp.getProperty('ALLOW_STALE_FALLBACK') || 'true').toLowerCase() === 'true',
    apiRetryCount: Number(sp.getProperty('API_RETRY_COUNT') || 1),

    // SerpAPI-specific
    serpApiKey: sp.getProperty('SERPAPI_KEY') || '',
    serpApiEngine: sp.getProperty('SERPAPI_ENGINE') || '',
    serpApiLocation: sp.getProperty('SERPAPI_LOCATION') || '',
    serpApiNoCache: sp.getProperty('SERPAPI_NO_CACHE') || '',
    serpDebug: String(sp.getProperty('SERP_DEBUG') || 'false').toLowerCase() === 'true'
  };
}

function validateProps_(p) {
  const missing = [];
  if (!p.rapidApiKey) missing.push('RAPIDAPI_KEY');
  if (!p.priceApiHost) missing.push('PRICE_API_HOST');
  if (!p.priceApiSearchUrl) missing.push('PRICE_API_SEARCH_URL');
  if (!p.dataSheetName) missing.push('DATA_SHEET_NAME');
  if (!p.itemList) missing.push('ITEM_LIST');

  if (missing.length) {
    throw new Error('Missing Script Properties: ' + missing.join(', '));
  }
}

function parseItems_(itemList) {
  const raw = itemList.split(',').map(s => s.trim()).filter(Boolean);
  const items = [];
  raw.forEach(token => {
    if (token.includes('|')) {
      const parts = token.split('|').map(x => x.trim());
      items.push({ id: parts[0], name: parts[1] || parts[0], query: parts[2] || parts[0] });
    } else {
      items.push({ id: slug_(token), name: title_(token), query: token });
    }
  });
  return items;
}

function cacheKeyForQuery_(query) {
  return 'PC_ITEM_' + Utilities.base64EncodeWebSafe(String(query)).slice(0, 50);
}

function slug_(s) {
  return String(s).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
}
function title_(s) {
  const t = String(s).trim();
  return t.charAt(0).toUpperCase() + t.slice(1);
}
function addParam_(url, k, v) {
  const sep = url.includes('?') ? '&' : '?';
  return url + sep + encodeURIComponent(k) + '=' + encodeURIComponent(v);
}
function redactSecrets_(text) {
  if (text == null) return text;
  let out = String(text);
  out = out.replace(/([?&]api_key=)[^&\s]+/gi, '$1REDACTED');
  out = out.replace(/(api_key["']?\s*[:=]\s*["']?)([^"'\s,}]+)/gi, '$1REDACTED');
  out = out.replace(/(X-RapidAPI-Key["']?\s*[:=]\s*["']?)([^"'\s,}]+)/gi, '$1REDACTED');
  out = out.replace(/(Authorization["']?\s*[:=]\s*["']?)([^"'\s,}]+)/gi, '$1REDACTED');
  out = out.replace(/Bearer\s+[A-Za-z0-9\-._~+/]+=*/gi, 'Bearer REDACTED');
  out = out.replace(/(token["']?\s*[:=]\s*["']?)([^"'\s,}]+)/gi, '$1REDACTED');
  return out;
}
function average_(arr) {
  const nums = arr.map(Number).filter(n => isFinite(n));
  if (!nums.length) return 0;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}
function usdToSats_(usd, btcUsd) { return (usd / btcUsd) * 100000000; }
function num_(v) {
  if (v == null) return NaN;
  const n = Number(String(v).replace(/[^0-9.]/g, ''));
  return isFinite(n) ? n : NaN;
}

/**
 * Generic "lowest price" extraction. Works with many common RapidAPI shopping schemas.
 * If your provider schema differs, paste one response and we can tighten this.
 */
function extractLowestPrice_(data, maxInspect) {
  const candidates = [];
  const arr = (Array.isArray(data) && data) ||
    (Array.isArray(data.products) && data.products) ||
    (Array.isArray(data.results) && data.results) ||
    (Array.isArray(data.items) && data.items) ||
    [];

  for (let i = 0; i < Math.min(arr.length, maxInspect); i++) {
    const p = arr[i] || {};
    const direct = num_(p.price || p.min_price || p.lowest_price || p.sale_price || p.current_price);

    let offer = NaN;
    if (Array.isArray(p.offers) && p.offers.length) {
      offer = num_(p.offers[0].price || p.offers[0].amount || p.offers[0].value);
    }

    let nested = NaN;
    if (p.offer) nested = num_(p.offer.price || p.offer.amount || p.offer.value);

    [direct, offer, nested].forEach(v => { if (isFinite(v) && v > 0) candidates.push(v); });
  }

  if (!candidates.length) {
    const shallow = JSON.stringify(data).slice(0, 8000);
    const m = shallow.match(/"price"\s*:\s*"?([0-9]+(\.[0-9]+)?)"?/i);
    if (m) candidates.push(Number(m[1]));
  }

  if (!candidates.length) throw new Error('Could not extract price from response');
  return Math.min.apply(null, candidates);
}

function extractItemDescription_(data, maxInspect) {
  const arr = (Array.isArray(data) && data) ||
    (Array.isArray(data.products) && data.products) ||
    (Array.isArray(data.results) && data.results) ||
    (Array.isArray(data.items) && data.items) ||
    [];
  const fields = [
    'quantity', 'size', 'weight', 'unit', 'item_description', 'description',
    'product_description', 'title', 'name'
  ];

  for (let i = 0; i < Math.min(arr.length, maxInspect); i++) {
    const item = arr[i] || {};
    for (let f = 0; f < fields.length; f++) {
      const value = item[fields[f]];
      if (value == null) continue;
      const text = String(value).trim();
      if (text) return text;
    }
  }
  return '';
}
