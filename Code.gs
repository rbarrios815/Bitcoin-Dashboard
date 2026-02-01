/***********************
 * Grocery Price Tracker (RapidAPI + permanent Sheet history)
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
 ************************/

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

/**
 * Fetch latest prices for ITEM_LIST (cached to save quota),
 * plus BTC/USD for sat conversion, WITHOUT writing to sheet.
 *
 * Resilience:
 * - If RapidAPI provider is down (502/503/504) it falls back to last-known prices in the sheet.
 * - Stale values are flagged.
 */
function fetchLatestSnapshot() {
  const props = getProps_();
  validateProps_(props);

  const cache = CacheService.getScriptCache();
  const cached = cache.get('LATEST_SNAPSHOT_V2');
  if (cached) return JSON.parse(cached);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(props.dataSheetName); // may be null before setupOnce

  const btcUsd = fetchBtcUsd_(props);
  const items = parseItems_(props.itemList);

  const priced = items.map(it => {
    const got = fetchItemUsd_(it, props, sheet);
    const sats = usdToSats_(got.usd, btcUsd);
    return {
      id: it.id,
      name: it.name,
      query: it.query,
      description: got.description,
      usd: got.usd,
      sats: sats,
      price_source: got.source,
      is_stale: got.stale
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

  // Short cache: prevents reload spam from burning quota
  cache.put('LATEST_SNAPSHOT_V2', JSON.stringify(out), 60 * 10); // 10 min
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
    ts,                        // timestamp
    snap.btcUsd,               // btc_usd
    it.id,                     // item_id
    it.name,                   // item_name
    it.query,                  // query
    it.description,            // item_description (NEW)
    it.usd,                    // usd
    it.sats,                   // sats
    snap.basketIndexUsd,       // basket_index_usd
    snap.basketIndexSats,      // basket_index_sats
    it.price_source,           // price_source (NEW)
    it.is_stale                // is_stale (NEW)
  ]));

  sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);

  return { ok: true, recordedAt: ts.toISOString(), rowsAppended: rows.length };
}

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
      description: idx.item_description != null ? String(row[idx.item_description] || '') : '',
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
    ? ((cur.basketIndexUsd / base.basketIndexUsd) - 1) * 100
    : 0;

  return {
    baselineTs: base.ts,
    baselineUsd: base.basketIndexUsd,
    currentTs: cur.ts,
    currentUsd: cur.basketIndexUsd,
    inflationPct: inflationPct
  };
}

/* =========================
   RapidAPI price fetch (resilient)
   ========================= */

function fetchItemUsd_(item, props, sheetForFallback) {
  const cache = CacheService.getScriptCache();
  const hours = props.itemCacheHours;
  const ttl = Math.max(60, Math.min(60 * 60 * 12, hours * 60 * 60)); // 1m..12h

  const key = 'PC_ITEM_' + Utilities.base64EncodeWebSafe(item.query).slice(0, 50);
  const cached = cache.get(key);
  if (cached) {
    try {
      const parsed = JSON.parse(cached);
      if (parsed && typeof parsed === 'object') {
        return {
          usd: Number(parsed.usd),
          description: parsed.description || '',
          source: 'cache',
          stale: false
        };
      }
    } catch (e) {}

    return { usd: Number(cached), description: '', source: 'cache', stale: false };
  }

  let url = addParam_(props.priceApiSearchUrl, 'product_title', item.query);
  if (props.countryCode) url = addParam_(url, 'country_code', props.countryCode);
  if (props.excludeDomains) url = addParam_(url, 'exclude_domains', props.excludeDomains);

  const maxRetries = Math.max(0, Number(props.apiRetryCount || 0));
  let lastErr = null;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const resp = UrlFetchApp.fetch(url, {
        method: 'get',
        muteHttpExceptions: true,
        headers: {
          'X-RapidAPI-Key': props.rapidApiKey,
          'X-RapidAPI-Host': props.priceApiHost
        }
      });

      const code = resp.getResponseCode();

      // Provider down/unreachable: break into fallback
      if (code === 502 || code === 503 || code === 504) {
        lastErr = new Error(`Price API provider unreachable (${code}): ${resp.getContentText().slice(0, 250)}`);
        break;
      }

      if (code >= 400) {
        throw new Error(`Price API error (${code}): ${resp.getContentText().slice(0, 250)}`);
      }

      const data = JSON.parse(resp.getContentText());
      const result = extractPriceAndDescription_(data, props.maxResultsPerItem);

      cache.put(key, JSON.stringify({ usd: result.price, description: result.description }), ttl);
      return { usd: result.price, description: result.description, source: 'rapidapi', stale: false };

    } catch (e) {
      lastErr = e;
      if (attempt < maxRetries) Utilities.sleep(400 * (attempt + 1));
    }
  }

  // Fallback to last-known-good from your Sheet
  if (props.allowStaleFallback && sheetForFallback) {
    const last = getLastKnownUsd_(item.id, sheetForFallback);
    if (isFinite(last) && last > 0) {
      return { usd: last, description: '', source: 'last_known', stale: true };
    }
  }

  throw lastErr || new Error('Unknown price fetch failure');
}

function extractPriceAndDescription_(data, maxInspect) {
  const candidates = [];
  let description = '';
  const arr =
    (Array.isArray(data) && data) ||
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

    [direct, offer, nested].forEach(v => {
      if (isFinite(v) && v > 0) candidates.push(v);
    });

    if (!description) {
      description = extractItemDescription_(p);
    }
  }

  if (!candidates.length) {
    const shallow = JSON.stringify(data).slice(0, 8000);
    const m = shallow.match(/"price"\s*:\s*"?([0-9]+(\.[0-9]+)?)"?/i);
    if (m) candidates.push(Number(m[1]));
  }

  if (!candidates.length) throw new Error('Could not extract price from response');
  return { price: Math.min.apply(null, candidates), description };
}

function extractItemDescription_(item) {
  const candidates = [
    item.quantity,
    item.size,
    item.weight,
    item.volume,
    item.unit_size,
    item.unit_count,
    item.units,
    item.pack,
    item.description
  ];

  for (let i = 0; i < candidates.length; i++) {
    const value = candidates[i];
    if (value != null && String(value).trim()) {
      return String(value).trim();
    }
  }

  return '';
}

/**
 * Find the last known USD for an itemId by searching upward.
 * This keeps your UI working even when the provider is down.
 */
function getLastKnownUsd_(itemId, sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;

  // A timestamp, B btc_usd, C item_id, G usd
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
   BTC/USD fetch (resilient)
   ========================= */

function fetchBtcUsd_(props) {
  const cache = CacheService.getScriptCache();
  const ttl = Math.max(60, Math.min(60 * 60, props.btcCacheMinutes * 60)); // 1m..60m
  const cached = cache.get('BTC_USD_V2');
  if (cached) return Number(cached);

  // Coinbase primary
  try {
    const url = 'https://api.coinbase.com/v2/prices/BTC-USD/spot';
    const resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    if (resp.getResponseCode() === 200) {
      const data = JSON.parse(resp.getContentText());
      const rate = Number(data?.data?.amount);
      if (isFinite(rate) && rate > 0) {
        cache.put('BTC_USD_V2', String(rate), ttl);
        return rate;
      }
    }
  } catch (e) {}

  // CoinGecko fallback
  try {
    const url = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd';
    const resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    if (resp.getResponseCode() === 200) {
      const data = JSON.parse(resp.getContentText());
      const rate = Number(data?.bitcoin?.usd);
      if (isFinite(rate) && rate > 0) {
        cache.put('BTC_USD_V2', String(rate), ttl);
        return rate;
      }
    }
  } catch (e) {}

  throw new Error('BTC/USD fetch failed (Coinbase + CoinGecko).');
}

/* =========================
   Setup: daily trigger
   ========================= */

function installDailyTrigger_() {
  ScriptApp.getProjectTriggers().forEach(t => {
    const fn = t.getHandlerFunction();
    if (fn === 'recordSnapshotDaily') ScriptApp.deleteTrigger(t);
  });

  ScriptApp.newTrigger('recordSnapshotDaily')
    .timeBased()
    .everyDays(1)
    .atHour(8)
    .create();
}

function recordSnapshotDaily() {
  return recordSnapshot();
}

/* =========================
   Sheet helpers
   ========================= */

function getOrCreateHistorySheet_(ss, name) {
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);

  // Ensure header exists and includes new columns
  const desiredHeader = [
    'timestamp',
    'btc_usd',
    'item_id',
    'item_name',
    'query',
    'item_description',
    'usd',
    'sats',
    'basket_index_usd',
    'basket_index_sats',
    'price_source',
    'is_stale'
  ];

  if (sh.getLastRow() === 0) {
    sh.appendRow(desiredHeader);
    sh.setFrozenRows(1);
    return sh;
  }

  // Read first row (up to current last column)
  const lastCol = Math.max(sh.getLastColumn(), desiredHeader.length);
  const header = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(String);

  // If header doesn't start with timestamp, fix by inserting header row
  if (String(header[0]).trim().toLowerCase() !== 'timestamp') {
    sh.insertRowBefore(1);
    sh.getRange(1, 1, 1, desiredHeader.length).setValues([desiredHeader]);
    sh.setFrozenRows(1);
    return sh;
  }

  // If missing new columns, add them
  const headerMap = {};
  header.forEach((h, i) => headerMap[String(h).trim()] = i);

  // Ensure at least desiredHeader length and set any missing header cells
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
    item_description: (m.item_description !== undefined ? m.item_description : null),
    usd: m.usd,
    sats: m.sats,
    basket_index_usd: m.basket_index_usd,
    basket_index_sats: m.basket_index_sats,
    price_source: (m.price_source !== undefined ? m.price_source : null),
    is_stale: (m.is_stale !== undefined ? m.is_stale : null)
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
    apiRetryCount: Number(sp.getProperty('API_RETRY_COUNT') || 1)
  };
}

function validateProps_(p) {
  const missing = [];
  if (!p.rapidApiKey) missing.push('RAPIDAPI_KEY');
  if (!p.priceApiHost) missing.push('PRICE_API_HOST');
  if (!p.priceApiSearchUrl) missing.push('PRICE_API_SEARCH_URL');
  if (!p.dataSheetName) missing.push('DATA_SHEET_NAME');
  if (!p.itemList) missing.push('ITEM_LIST');

  if (missing.length) throw new Error('Missing Script Properties: ' + missing.join(', '));
}

function parseItems_(itemList) {
  // Supports:
  // - simple list: apples,bananas
  // - advanced tokens: id|Name|query
  const raw = itemList.split(',').map(s => s.trim()).filter(Boolean);
  const items = [];

  raw.forEach(token => {
    if (token.includes('|')) {
      const parts = token.split('|').map(x => x.trim());
      items.push({
        id: parts[0],
        name: parts[1] || parts[0],
        query: parts[2] || parts[0]
      });
    } else {
      items.push({ id: slug_(token), name: title_(token), query: token });
    }
  });

  return items;
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

function num_(v) {
  if (v == null) return NaN;
  const n = Number(String(v).replace(/[^0-9.]/g, ''));
  return isFinite(n) ? n : NaN;
}

function average_(arr) {
  const nums = arr.map(Number).filter(n => isFinite(n));
  if (!nums.length) return 0;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function usdToSats_(usd, btcUsd) {
  return (usd / btcUsd) * 100000000;
}
