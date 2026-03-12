/***********************
 * File: Code.gs
 * Project: Bitcoin Dashboard (Google Apps Script)
 * Purpose: Server-side data collection, normalization, caching, and web app API endpoints.
 * Notes: Keep secrets in Script Properties only; never hardcode API keys.
 * Maintenance: Validate property changes with setupOnce() and test web output after edits.
 * Version: 1.0.1
 * Body-Hash-SHA256: 92f4dde3685ea2100efd63dee8e1e2f6af86ddc700510743955d22f97d8f5722
 ************************/

const FIXED_BASKET_ITEMS_ = {
  cash10: { usd: 10 },
  sats10000: { sats: 10000 }
};

/* =========================
   Web app entry
   ========================= */
function doGet() {
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle('Commodity Price Tracker')
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

function basketWeightForItemId_(itemId) {
  return 1;
}

function getFixedBasketValue_(itemId, btcUsd) {
  const id = String(itemId || '').trim().toLowerCase();
  const fixed = FIXED_BASKET_ITEMS_[id];
  if (!fixed) return null;

  const usd = isFinite(fixed.usd)
    ? Number(fixed.usd)
    : (isFinite(fixed.sats) && isFinite(btcUsd) && btcUsd > 0)
      ? (Number(fixed.sats) / 100000000) * btcUsd
      : NaN;
  const sats = isFinite(fixed.sats)
    ? Number(fixed.sats)
    : (isFinite(fixed.usd) && isFinite(btcUsd) && btcUsd > 0)
      ? usdToSats_(Number(fixed.usd), btcUsd)
      : NaN;

  return {
    usd: isFinite(usd) ? usd : 0,
    sats: isFinite(sats) ? sats : 0
  };
}

function isFixedBasketItemId_(itemId) {
  const id = String(itemId || '').trim().toLowerCase();
  return Boolean(FIXED_BASKET_ITEMS_[id]);
}

function computeWeightedBasketIndex_(items) {
  let weightedUsdTotal = 0;
  let weightedSatsTotal = 0;
  let weightTotal = 0;

  (items || []).forEach(item => {
    const usd = Number(item && item.usd);
    const sats = Number(item && item.sats);
    const weight = basketWeightForItemId_(item && item.id);
    if (!isFinite(usd) || !isFinite(sats) || !isFinite(weight) || weight <= 0) return;
    weightedUsdTotal += usd * weight;
    weightedSatsTotal += sats * weight;
    weightTotal += weight;
  });

  return {
    usd: weightTotal ? (weightedUsdTotal / weightTotal) : NaN,
    sats: weightTotal ? (weightedSatsTotal / weightTotal) : NaN
  };
}

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

  const snapshotTs = new Date().toISOString();
  const btcUsd = fetchBtcUsd_(props);
  const items = parseItems_(props.itemList);
  const fetchableItems = items.filter(item => !isFixedBasketItemId_(item.id));

  // 1) RapidAPI batch fetch (includes per-item cache reads/writes)
  const rapid = fetchItemsUsdRapidApiBatch_(fetchableItems, props);

  // 2) SerpAPI batch fetch only for RapidAPI failures (if configured)
  let serpById = {};
  if (props.serpApiKey) {
    const needSerp = fetchableItems.filter(it => {
      const rr = rapid.byId[it.id];
      return !rr || rr.error;
    });
    if (needSerp.length) {
      serpById = fetchItemsUsdSerpApiBatch_(needSerp, props);
    }
  }

  // 3) Finalize items with fallback to last-known
  const priced = items.map(it => {
    if (isFixedBasketItemId_(it.id)) {
      const fixed = getFixedBasketValue_(it.id, btcUsd);
      return {
        id: it.id,
        name: it.name,
        query: it.query,
        item_description: applyItemDescription_(it.name, null, null),
        ts: snapshotTs,
        usd: fixed ? fixed.usd : 0,
        sats: fixed ? fixed.sats : 0,
        source_url: '',
        price_source: 'fixed',
        price_vendor: '',
        is_stale: false
      };
    }
    const rr = rapid.byId[it.id];

    if (rr && isFinite(rr.usd) && rr.usd > 0) {
      const description = applyItemDescription_(it.name, rr.description, null);
      const sats = usdToSats_(rr.usd, btcUsd);
      return {
        id: it.id,
        name: it.name,
        query: it.query,
        item_description: description,
        ts: snapshotTs,
        usd: rr.usd,
        sats: sats,
        source_url: rr.source_url || '',
        price_source: rr.source,
        price_vendor: rr.vendor || '',
        is_stale: false
      };
    }

    const sr = serpById[it.id];
    if (sr && isFinite(sr.usd) && sr.usd > 0) {
      const description = applyItemDescription_(it.name, sr.description, null);
      const sats = usdToSats_(sr.usd, btcUsd);
      return {
        id: it.id,
        name: it.name,
        query: it.query,
        item_description: description,
        ts: snapshotTs,
        usd: sr.usd,
        sats: sats,
        source_url: sr.source_url || '',
        price_source: sr.source,
        price_vendor: sr.vendor || '',
        is_stale: false
      };
    }

    if (props.allowStaleFallback && sheet) {
      const description = applyItemDescription_(it.name, null, null);
      const last = getLastKnownUsd_(description, sheet);
      if (isFinite(last) && last > 0) {
        const sats = usdToSats_(last, btcUsd);
        return {
          id: it.id,
          name: it.name,
          query: it.query,
          item_description: description,
          ts: snapshotTs,
          usd: last,
          sats: sats,
          source_url: '',
          price_source: 'last_known',
          price_vendor: '',
          is_stale: true
        };
      }
    }

    return {
      id: it.id,
      name: it.name,
      query: it.query,
      item_description: applyItemDescription_(it.name, null, null),
      ts: snapshotTs,
      usd: 0,
      sats: 0,
      source_url: '',
      price_source: (rr && rr.error) ? ('error:' + rr.error) : 'error',
      price_vendor: '',
      is_stale: true
    };
  });

  const firstAvailableByDescription = sheet
    ? getFirstAvailableByDescriptions_(priced.map(p => p.item_description), sheet)
    : {};
  const vendorCountsByDescription = sheet
    ? getVendorCountsByDescriptions_(priced.map(p => p.item_description), sheet)
    : {};

  const enriched = priced.map(item => {
    const normalized = normalizeDescription_(item.item_description);
    const firstAvailable = firstAvailableByDescription[normalized] || null;
    const vendorCount = vendorCountsByDescription[normalized] || 0;
    return Object.assign({}, item, { first_available: firstAvailable, vendor_count: vendorCount });
  });

  const weightedBasket = computeWeightedBasketIndex_(priced);
  const basketUsd = weightedBasket.usd;
  const basketSats = weightedBasket.sats;

  const out = {
    ts: snapshotTs,
    btcUsd,
    items: enriched,
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
    it.price_vendor,       // price_vendor
    it.source_url,         // source_url
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

function getLatestSnapshotFromSheet() {
  const props = getProps_();
  validateSheetProps_(props);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(props.dataSheetName);
  const items = parseItems_(props.itemList);
  const nameCounts = items.reduce((acc, item) => {
    const name = String(item.name || '').trim();
    if (!name) return acc;
    acc[name] = (acc[name] || 0) + 1;
    return acc;
  }, {});

  if (!sheet || sheet.getLastRow() < 2) {
    let fallbackBtcUsd = null;
    try {
      fallbackBtcUsd = fetchBtcUsd_(props);
    } catch (e) {
      fallbackBtcUsd = null;
    }
    return {
      ts: null,
      btcUsd: isFinite(fallbackBtcUsd) ? fallbackBtcUsd : null,
      items: items.map(it => ({
        id: it.id,
        name: it.name,
        query: it.query,
        item_description: applyItemDescription_(it.name, null, null),
        usd: isFixedBasketItemId_(it.id) ? (getFixedBasketValue_(it.id, fallbackBtcUsd) || { usd: 0 }).usd : 0,
        sats: isFixedBasketItemId_(it.id) ? (getFixedBasketValue_(it.id, fallbackBtcUsd) || { sats: 0 }).sats : 0,
        source_url: '',
        price_source: isFixedBasketItemId_(it.id) ? 'fixed' : '',
        price_vendor: '',
        is_stale: isFixedBasketItemId_(it.id) ? false : true,
        first_available: null,
        vendor_count: 0
      })),
      basketIndexUsd: 0,
      basketIndexSats: 0
    };
  }

  const values = sheet.getDataRange().getValues();
  const header = values[0];
  const idx = headerIndex_(header);

  const latestByDescription = {};
  const earliestByDescription = {};
  const vendorCountsByDescription = {};
  const latestById = {};
  const earliestById = {};
  const latestByName = {};
  const earliestByName = {};
  let latestTs = null;
  let latestBasketUsd = null;
  let latestBasketSats = null;
  let latestBtcUsd = null;

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const desc = String(row[idx.item_description] || '').trim();
    if (!desc) continue;

    const tsValue = row[idx.timestamp];
    const ts = tsValue ? new Date(tsValue) : null;
    if (!ts || Number.isNaN(ts.getTime())) continue;

    const normalized = normalizeDescription_(desc);
    const rowPriceSource = idx.price_source != null ? String(row[idx.price_source] || '').trim() : '';
    const rowPriceVendor = idx.price_vendor != null ? String(row[idx.price_vendor] || '').trim() : '';
    if (normalized) {
      if (!vendorCountsByDescription[normalized]) vendorCountsByDescription[normalized] = {};
      if (rowPriceVendor) {
        vendorCountsByDescription[normalized][rowPriceVendor] = true;
      }
    }
    const rowItemId = idx.item_id != null ? String(row[idx.item_id] || '').trim() : '';
    const rowItemName = idx.item_name != null ? String(row[idx.item_name] || '').trim() : '';
      const rowDetails = {
      ts,
      item_id: rowItemId,
      item_name: rowItemName,
      item_description: desc,
      usd: convertElectricityUsdToKwh_(row[idx.usd], rowItemId, rowItemName, desc),
      sats: Number(row[idx.sats]),
      price_source: idx.price_source != null ? String(row[idx.price_source] || '') : '',
      price_vendor: idx.price_vendor != null ? String(row[idx.price_vendor] || '') : '',
      source_url: idx.source_url != null ? String(row[idx.source_url] || '') : '',
      is_stale: idx.is_stale != null ? Boolean(row[idx.is_stale]) : false
    };
    if (!latestByDescription[normalized] || ts > latestByDescription[normalized].ts) {
      latestByDescription[normalized] = rowDetails;
    }
    if (rowItemId && (!latestById[rowItemId] || ts > latestById[rowItemId].ts)) {
      latestById[rowItemId] = rowDetails;
    }
    if (rowItemName && (!latestByName[rowItemName] || ts > latestByName[rowItemName].ts)) {
      latestByName[rowItemName] = rowDetails;
    }

    if (!earliestByDescription[normalized] || ts < earliestByDescription[normalized].ts) {
      earliestByDescription[normalized] = {
        ts,
        usd: convertElectricityUsdToKwh_(row[idx.usd], rowItemId, rowItemName, desc),
        sats: Number(row[idx.sats])
      };
    }
    if (rowItemId && (!earliestById[rowItemId] || ts < earliestById[rowItemId].ts)) {
      earliestById[rowItemId] = {
        ts,
        usd: convertElectricityUsdToKwh_(row[idx.usd], rowItemId, rowItemName, desc),
        sats: Number(row[idx.sats])
      };
    }
    if (rowItemName && (!earliestByName[rowItemName] || ts < earliestByName[rowItemName].ts)) {
      earliestByName[rowItemName] = {
        ts,
        usd: convertElectricityUsdToKwh_(row[idx.usd], rowItemId, rowItemName, desc),
        sats: Number(row[idx.sats])
      };
    }

    if (!latestTs || ts > latestTs) {
      latestTs = ts;
      latestBasketUsd = Number(row[idx.basket_index_usd]);
      latestBasketSats = Number(row[idx.basket_index_sats]);
      latestBtcUsd = Number(row[idx.btc_usd]);
    }
  }

  const payloadItems = items.map(it => {
    const fallbackDescription = applyItemDescription_(it.name, null, null);
    const normalized = normalizeDescription_(fallbackDescription);
    const nameKey = String(it.name || '').trim();
    const isNameUnique = nameKey && nameCounts[nameKey] === 1;
    const latestNameRow = latestByName[nameKey];
    const earliestNameRow = earliestByName[nameKey];
    const latestRow = latestById[it.id]
      || (isNameUnique ? latestNameRow : null)
      || latestByDescription[normalized]
      || null;
    const earliestRow = earliestById[it.id]
      || (isNameUnique ? earliestNameRow : null)
      || earliestByDescription[normalized]
      || null;
    const description = latestRow?.item_description || fallbackDescription;
    if (isFixedBasketItemId_(it.id)) {
      const fixed = getFixedBasketValue_(it.id, latestBtcUsd);
      return {
        id: it.id,
        name: it.name,
        query: it.query,
        item_description: description,
        ts: latestTs ? latestTs.toISOString() : null,
        usd: fixed ? fixed.usd : 0,
        sats: fixed ? fixed.sats : 0,
        source_url: '',
        price_source: 'fixed',
        price_vendor: '',
        is_stale: false,
        first_available: earliestRow
          ? { ts: new Date(earliestRow.ts).toISOString(), usd: earliestRow.usd, sats: earliestRow.sats }
          : null,
        vendor_count: vendorCountsByDescription[normalized]
          ? Object.keys(vendorCountsByDescription[normalized]).length
          : 0
      };
    }
    return {
      id: it.id,
      name: it.name,
      query: it.query,
      item_description: description,
      ts: latestRow ? new Date(latestRow.ts).toISOString() : null,
      usd: latestRow ? latestRow.usd : 0,
      sats: latestRow ? latestRow.sats : 0,
      source_url: latestRow ? latestRow.source_url : '',
      price_source: latestRow ? latestRow.price_source : '',
      price_vendor: latestRow ? latestRow.price_vendor : '',
      is_stale: latestRow ? latestRow.is_stale : true,
      first_available: earliestRow
        ? { ts: new Date(earliestRow.ts).toISOString(), usd: earliestRow.usd, sats: earliestRow.sats }
        : null,
      vendor_count: vendorCountsByDescription[normalized]
        ? Object.keys(vendorCountsByDescription[normalized]).length
        : 0
    };
  });

  return {
    ts: latestTs ? latestTs.toISOString() : null,
    btcUsd: isFinite(latestBtcUsd) ? latestBtcUsd : null,
    items: payloadItems,
    basketIndexUsd: isFinite(latestBasketUsd) ? latestBasketUsd : 0,
    basketIndexSats: isFinite(latestBasketSats) ? latestBasketSats : 0
  };
}


function buildFixedItemHistoryFromSheet_(sheet, fixedId, idx) {
  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];
  const out = [];
  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const ts = parseSheetDate_(idx.timestamp != null ? row[idx.timestamp] : null);
    if (!ts) continue;
    const btcUsd = safeNumber_(idx.btc_usd != null ? row[idx.btc_usd] : null);
    const fixed = getFixedBasketValue_(fixedId, btcUsd);
    if (!fixed) continue;
    out.push({
      ts: ts.toISOString(),
      usd: fixed.usd,
      sats: fixed.sats,
      btcUsd: btcUsd,
      price_source: 'fixed',
      price_vendor: '',
      source_url: '',
      is_stale: false
    });
  }
  out.sort((a,b) => new Date(a.ts) - new Date(b.ts));
  const dedup = [];
  let lastTs = '';
  out.forEach(row => {
    if (row.ts === lastTs) return;
    lastTs = row.ts;
    dedup.push(row);
  });
  return dedup;
}

function getItemHistory(itemDescription) {
  const props = getProps_();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(props.dataSheetName);
  if (!sheet) return [];

  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];

  const header = values[0];
  const idx = headerIndex_(header);
  const out = [];

  const targetDescription = String(itemDescription || '').trim();
  const normalizedTarget = normalizeDescription_(targetDescription);
  if (normalizedTarget === normalizeDescription_('10,000 satoshis')) {
    return buildFixedItemHistoryFromSheet_(sheet, 'sats10000', idx);
  }
  if (normalizedTarget === normalizeDescription_('$10')) {
    return buildFixedItemHistoryFromSheet_(sheet, 'cash10', idx);
  }
  const targetNormalized = normalizeDescription_(targetDescription);
  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const rowDescription = String(row[idx.item_description] || '').trim();
    if (!rowDescription) continue;
    if (normalizeDescription_(rowDescription) !== targetNormalized) continue;
    out.push({
      ts: new Date(row[idx.timestamp]).toISOString(),
      usd: convertElectricityUsdToKwh_(row[idx.usd], idx.item_id != null ? row[idx.item_id] : '', idx.item_name != null ? row[idx.item_name] : '', rowDescription),
      sats: Number(row[idx.sats]),
      btcUsd: Number(row[idx.btc_usd]),
      price_source: idx.price_source != null ? String(row[idx.price_source] || '') : '',
      price_vendor: idx.price_vendor != null ? String(row[idx.price_vendor] || '') : '',
      source_url: idx.source_url != null ? String(row[idx.source_url] || '') : '',
      is_stale: idx.is_stale != null ? Boolean(row[idx.is_stale]) : false
    });
  }
  out.sort((a, b) => new Date(a.ts) - new Date(b.ts));
  return out;
}

function getAllItemHistories(itemDescriptions) {
  const props = getProps_();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(props.dataSheetName);
  if (!sheet) return [];

  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];

  const header = values[0];
  const idx = headerIndex_(header);
  const outByNormalized = {};
  const targets = Array.isArray(itemDescriptions) ? itemDescriptions : [];
  const targetSet = {};
  const useFilter = targets.length > 0;

  targets.forEach(desc => {
    const cleaned = String(desc || '').trim();
    if (!cleaned) return;
    const normalized = normalizeDescription_(cleaned);
    if (!normalized) return;
    targetSet[normalized] = true;
    if (!outByNormalized[normalized]) {
      outByNormalized[normalized] = { description: cleaned, history: [] };
    }
  });

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const rowDescription = String(row[idx.item_description] || '').trim();
    if (!rowDescription) continue;
    const normalized = normalizeDescription_(rowDescription);
    if (useFilter && !targetSet[normalized]) continue;
    if (!outByNormalized[normalized]) {
      outByNormalized[normalized] = { description: rowDescription, history: [] };
    }
    outByNormalized[normalized].history.push({
      ts: new Date(row[idx.timestamp]).toISOString(),
      usd: convertElectricityUsdToKwh_(row[idx.usd], idx.item_id != null ? row[idx.item_id] : '', idx.item_name != null ? row[idx.item_name] : '', rowDescription),
      sats: Number(row[idx.sats]),
      btcUsd: Number(row[idx.btc_usd]),
      price_source: idx.price_source != null ? String(row[idx.price_source] || '') : '',
      price_vendor: idx.price_vendor != null ? String(row[idx.price_vendor] || '') : '',
      source_url: idx.source_url != null ? String(row[idx.source_url] || '') : '',
      is_stale: idx.is_stale != null ? Boolean(row[idx.is_stale]) : false
    });
  }

  const fixedTargets = [
    { normalized: normalizeDescription_('10,000 satoshis'), id: 'sats10000', label: '10,000 satoshis' },
    { normalized: normalizeDescription_('$10'), id: 'cash10', label: '$10' }
  ];
  fixedTargets.forEach(fixed => {
    if (!targetSet[fixed.normalized]) return;
    if (!outByNormalized[fixed.normalized]) outByNormalized[fixed.normalized] = { description: fixed.label, history: [] };
    if (!(outByNormalized[fixed.normalized].history || []).length) {
      outByNormalized[fixed.normalized].history = buildFixedItemHistoryFromSheet_(sheet, fixed.id, idx);
      if (!outByNormalized[fixed.normalized].description) outByNormalized[fixed.normalized].description = fixed.label;
    }
  });

  const out = Object.keys(outByNormalized).map(key => {
    const entry = outByNormalized[key];
    entry.history.sort((a, b) => new Date(a.ts) - new Date(b.ts));
    return entry;
  });

  out.sort((a, b) => a.description.localeCompare(b.description));
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
  const snapshots = {};

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const ts = row[idx.timestamp];
    if (!ts) continue;
    const tsIso = new Date(ts).toISOString();
    if (!snapshots[tsIso]) snapshots[tsIso] = [];
    snapshots[tsIso].push({
      id: idx.item_id != null ? String(row[idx.item_id] || '') : '',
      usd: convertElectricityUsdToKwh_(row[idx.usd], idx.item_id != null ? row[idx.item_id] : '', idx.item_name != null ? row[idx.item_name] : '', idx.item_description != null ? row[idx.item_description] : ''),
      sats: Number(row[idx.sats]),
      btcUsd: Number(row[idx.btc_usd])
    });
  }

  const out = Object.keys(snapshots).map(ts => {
    const rows = snapshots[ts];
    const weighted = computeWeightedBasketIndex_(rows);
    const btcValues = rows.map(item => Number(item.btcUsd)).filter(isFinite);
    return {
      ts,
      btcUsd: btcValues.length ? average_(btcValues) : NaN,
      basketIndexUsd: isFinite(weighted.usd) ? weighted.usd : 0,
      basketIndexSats: isFinite(weighted.sats) ? weighted.sats : 0
    };
  });

  out.sort((a, b) => new Date(a.ts) - new Date(b.ts));
  return out;
}

function getBasketInflation() {
  const hist = getBasketHistory();
  if (hist.length < 2) {
    return {
      baselineTs: null, baselineUsd: null, baselineSats: null,
      currentTs: null, currentUsd: null, currentSats: null,
      inflationPctUsd: 0,
      inflationPctSats: 0,
      inflationPct: 0
    };
  }
  const base = hist[0];
  const cur = hist[hist.length - 1];
  const inflationPctUsd = (base.basketIndexUsd && cur.basketIndexUsd)
    ? ((cur.basketIndexUsd / base.basketIndexUsd) - 1) * 100 : 0;
  const inflationPctSats = (base.basketIndexSats && cur.basketIndexSats)
    ? ((cur.basketIndexSats / base.basketIndexSats) - 1) * 100 : 0;
  return {
    baselineTs: base.ts,
    baselineUsd: base.basketIndexUsd,
    baselineSats: base.basketIndexSats,
    currentTs: cur.ts,
    currentUsd: cur.basketIndexUsd,
    currentSats: cur.basketIndexSats,
    inflationPctUsd: inflationPctUsd,
    inflationPctSats: inflationPctSats,
    inflationPct: inflationPctUsd
  };
}



function getPurchasingPowerDashboardData() {
  const props = getProps_();
  validateSheetProps_(props);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(props.dataSheetName);
  if (!sheet || sheet.getLastRow() < 2) {
    return {
      generatedAt: new Date().toISOString(),
      snapshots: [],
      items: [],
      quality: {
        currentItems: 0,
        staleItems: 0,
        missingItems: 0,
        vendorInconsistencyCount: 0,
        currentSnapshotHasStale: false
      },
      fieldsDetected: {}
    };
  }

  const values = sheet.getDataRange().getValues();
  const header = values[0].map(h => String(h || '').trim());
  const idx = buildDashboardHeaderIndex_(header);

  const configuredItems = parseItems_(props.itemList)
    .map(item => ({ id: item.id, name: item.name }));
  const configuredNameById = {};
  configuredItems.forEach(item => configuredNameById[item.id] = item.name);

  const snapshotsByTs = {};
  const itemHistoryByKey = {};
  const vendorSetByItem = {};

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const ts = parseSheetDate_(idx.timestamp != null ? row[idx.timestamp] : null);
    if (!ts) continue;

    const rowIso = ts.toISOString();
    if (!snapshotsByTs[rowIso]) {
      snapshotsByTs[rowIso] = {
        ts: rowIso,
        btcUsd: safeNumber_(idx.btc_usd != null ? row[idx.btc_usd] : null),
        basketUsd: safeNumber_(idx.basket_index_usd != null ? row[idx.basket_index_usd] : null),
        basketSats: safeNumber_(idx.basket_index_sats != null ? row[idx.basket_index_sats] : null),
        itemRows: []
      };
    }

    const itemId = idx.item_id != null ? String(row[idx.item_id] || '').trim() : '';
    const itemName = idx.item_name != null ? String(row[idx.item_name] || '').trim() : '';
    const description = idx.item_description != null ? String(row[idx.item_description] || '').trim() : '';
    const source = idx.price_source != null ? String(row[idx.price_source] || '').trim() : '';
    const vendor = idx.price_vendor != null ? String(row[idx.price_vendor] || '').trim() : '';
    const group = idx.group != null ? String(row[idx.group] || '').trim() : '';
    const isStale = idx.is_stale != null ? Boolean(row[idx.is_stale]) : /^last_known/i.test(source);
    const usd = deriveUsdValue_(row, idx);
    const btcUsd = safeNumber_(idx.btc_usd != null ? row[idx.btc_usd] : null);
    const sats = deriveSatsValue_(row, idx, usd, btcUsd);

    const fallbackKey = description || itemName || itemId || `row_${r}`;
    const itemKey = normalizeDescription_(fallbackKey);
    if (!itemHistoryByKey[itemKey]) {
      itemHistoryByKey[itemKey] = {
        key: itemKey,
        itemId: itemId || '',
        itemName: itemName || configuredNameById[itemId] || description || itemId || 'Unknown Item',
        description: description || itemName || itemId || 'Unknown Item',
        history: []
      };
    }
    itemHistoryByKey[itemKey].history.push({
      ts: rowIso,
      usd: usd,
      sats: sats,
      btcUsd: btcUsd,
      vendor: vendor,
      source: source,
      group: group,
      is_stale: isStale
    });

    if (!vendorSetByItem[itemKey]) vendorSetByItem[itemKey] = {};
    if (vendor) vendorSetByItem[itemKey][vendor] = true;

    snapshotsByTs[rowIso].itemRows.push({
      itemKey: itemKey,
      itemId: itemId,
      itemName: itemName || description || itemId || 'Unknown Item',
      description: description || itemName || itemId || 'Unknown Item',
      usd: usd,
      sats: sats,
      btcUsd: btcUsd,
      vendor: vendor,
      source: source,
      group: group,
      is_stale: isStale
    });
  }

  const snapshotKeys = Object.keys(snapshotsByTs).sort((a, b) => new Date(a) - new Date(b));
  const snapshots = snapshotKeys.map(tsIso => {
    const entry = snapshotsByTs[tsIso];
    const rows = entry.itemRows;
    const validUsd = rows.map(r => r.usd).filter(isFinite);
    const validSats = rows.map(r => r.sats).filter(isFinite);
    const staleCount = rows.filter(r => r.is_stale).length;
    const missingCount = rows.filter(r => !isFinite(r.usd) || !isFinite(r.sats)).length;

    return {
      ts: tsIso,
      btcUsd: isFinite(entry.btcUsd) ? entry.btcUsd : average_(rows.map(r => r.btcUsd)),
      basketUsd: isFinite(entry.basketUsd) ? entry.basketUsd : average_(validUsd),
      basketSats: isFinite(entry.basketSats) ? entry.basketSats : average_(validSats),
      itemCount: rows.length,
      staleCount: staleCount,
      missingCount: missingCount,
      groups: uniqueValues_(rows.map(r => r.group)),
      items: rows
    };
  });

  const itemList = Object.keys(itemHistoryByKey).map(key => {
    const entry = itemHistoryByKey[key];
    entry.history.sort((a, b) => new Date(a.ts) - new Date(b.ts));
    const latest = entry.history[entry.history.length - 1] || {};
    return {
      key: entry.key,
      itemId: entry.itemId,
      itemName: entry.itemName,
      description: entry.description,
      vendorCount: vendorSetByItem[key] ? Object.keys(vendorSetByItem[key]).length : 0,
      vendorChanged: vendorSetByItem[key] ? Object.keys(vendorSetByItem[key]).length > 1 : false,
      latestUsd: safeNumber_(latest.usd),
      latestSats: safeNumber_(latest.sats),
      latestVendor: latest.vendor || '',
      latestSource: latest.source || '',
      latestIsStale: Boolean(latest.is_stale),
      history: entry.history
    };
  }).sort((a, b) => a.description.localeCompare(b.description));

  const latestSnapshot = snapshots.length ? snapshots[snapshots.length - 1] : null;
  const quality = {
    currentItems: latestSnapshot ? latestSnapshot.itemCount : 0,
    staleItems: latestSnapshot ? latestSnapshot.staleCount : 0,
    missingItems: latestSnapshot ? latestSnapshot.missingCount : 0,
    vendorInconsistencyCount: itemList.filter(item => item.vendorChanged).length,
    currentSnapshotHasStale: latestSnapshot ? latestSnapshot.staleCount > 0 : false
  };

  return {
    generatedAt: new Date().toISOString(),
    snapshots: snapshots,
    items: itemList,
    quality: quality,
    fieldsDetected: {
      timestamp: idx.timestamp != null,
      item_name: idx.item_name != null,
      item_description: idx.item_description != null,
      price_vendor: idx.price_vendor != null,
      usd: idx.usd != null,
      btc_usd: idx.btc_usd != null,
      sats: idx.sats != null,
      basket_index_usd: idx.basket_index_usd != null,
      basket_index_sats: idx.basket_index_sats != null,
      is_stale: idx.is_stale != null,
      group: idx.group != null
    }
  };
}

function buildDashboardHeaderIndex_(headerRow) {
  return {
    timestamp: findHeaderIndex_(headerRow, ['timestamp', 'date', 'datetime', 'ts']),
    btc_usd: findHeaderIndex_(headerRow, ['btc_usd', 'btc usd', 'btc/usd', 'exchange_rate', 'btc_rate']),
    item_id: findHeaderIndex_(headerRow, ['item_id', 'item id', 'id']),
    item_name: findHeaderIndex_(headerRow, ['item_name', 'item name', 'name']),
    item_description: findHeaderIndex_(headerRow, ['item_description', 'item description', 'description']),
    usd: findHeaderIndex_(headerRow, ['usd', 'price_usd', 'usd_price', 'price']),
    sats: findHeaderIndex_(headerRow, ['sats', 'satoshis', 'sats_price']),
    basket_index_usd: findHeaderIndex_(headerRow, ['basket_index_usd', 'basket usd', 'basket_total_usd', 'basket_usd']),
    basket_index_sats: findHeaderIndex_(headerRow, ['basket_index_sats', 'basket sats', 'basket_total_sats', 'basket_sats']),
    price_source: findHeaderIndex_(headerRow, ['price_source', 'source', 'vendor_source']),
    price_vendor: findHeaderIndex_(headerRow, ['price_vendor', 'vendor', 'source_vendor']),
    is_stale: findHeaderIndex_(headerRow, ['is_stale', 'stale', 'carried_forward']),
    group: findHeaderIndex_(headerRow, ['category', 'group', 'product_group'])
  };
}

function findHeaderIndex_(headerRow, candidates) {
  const normalizedMap = {};
  headerRow.forEach((label, idx) => {
    normalizedMap[String(label || '').trim().toLowerCase()] = idx;
  });
  for (let i = 0; i < candidates.length; i++) {
    const found = normalizedMap[String(candidates[i]).toLowerCase()];
    if (found !== undefined) return found;
  }
  return null;
}

function deriveUsdValue_(row, idx) {
  const itemId = idx.item_id != null ? row[idx.item_id] : '';
  const itemName = idx.item_name != null ? row[idx.item_name] : '';
  const itemDescription = idx.item_description != null ? row[idx.item_description] : '';
  const direct = convertElectricityUsdToKwh_(idx.usd != null ? row[idx.usd] : null, itemId, itemName, itemDescription);
  if (isFinite(direct)) return direct;
  const sats = safeNumber_(idx.sats != null ? row[idx.sats] : null);
  const btcUsd = safeNumber_(idx.btc_usd != null ? row[idx.btc_usd] : null);
  if (isFinite(sats) && isFinite(btcUsd) && btcUsd > 0) {
    return (sats / 100000000) * btcUsd;
  }
  return NaN;
}

function deriveSatsValue_(row, idx, usd, btcUsd) {
  const direct = safeNumber_(idx.sats != null ? row[idx.sats] : null);
  if (isFinite(direct)) return direct;
  if (isFinite(usd) && isFinite(btcUsd) && btcUsd > 0) {
    return usdToSats_(usd, btcUsd);
  }
  return NaN;
}

function parseSheetDate_(value) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function safeNumber_(value) {
  const n = Number(value);
  return isFinite(n) ? n : NaN;
}

function isElectricityItemMeta_(itemId, itemName, itemDescription) {
  const id = String(itemId || '').trim().toLowerCase();
  if (id === 'mwh') return true;
  const name = String(itemName || '').trim().toLowerCase();
  const description = String(itemDescription || '').trim().toLowerCase();
  const combined = `${name} ${description}`;
  return combined.indexOf('electric') !== -1 || combined.indexOf('kwh') !== -1 || combined.indexOf('mwh') !== -1;
}

function convertElectricityUsdToKwh_(usd, itemId, itemName, itemDescription) {
  const numeric = safeNumber_(usd);
  if (!isFinite(numeric)) return NaN;
  return isElectricityItemMeta_(itemId, itemName, itemDescription) ? (numeric / 1000) : numeric;
}

function uniqueValues_(values) {
  const out = {};
  (values || []).forEach(v => {
    const cleaned = String(v || '').trim();
    if (!cleaned) return;
    out[cleaned] = true;
  });
  return Object.keys(out).sort();
}

function isReferenceItemId_(itemId) {
  const id = String(itemId || '').trim().toLowerCase();
  return id === 'gold' || id === 'silver' || id === 'mwh' || id === 'cash10' || id === 'sats10000';
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
        byId[it.id] = {
          usd: Number(cachedData.usd),
          description: cachedData.description || '',
          source_url: cachedData.source_url || '',
          vendor: cachedData.vendor || '',
          source: 'cache'
        };
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
      const extractedDescription = extractItemDescription_(data, props.maxResultsPerItem);
      const extractedSourceUrl = extractItemSourceUrl_(data, props.maxResultsPerItem);
      const extractedVendor = extractItemVendor_(data, props.maxResultsPerItem, extractedSourceUrl);
      const description = applyItemDescription_(it.name, extractedDescription, null);

      if (isFinite(price) && price > 0) {
        cache.put(
          key,
          JSON.stringify({
            usd: price,
            description: description,
            source_url: extractedSourceUrl,
            vendor: extractedVendor
          }),
          ttl
        );
        byId[it.id] = {
          usd: price,
          description: description,
          source_url: extractedSourceUrl,
          vendor: extractedVendor,
          source: 'rapidapi'
        };
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
    const rawQuery = getSerpQuery_(it.query, `SerpAPI batch item "${it.id}"`);
    const query = encodeURIComponent(rawQuery);
    let url = 'https://serpapi.com/search?engine=' + encodeURIComponent(engine) +
      '&q=' + query +
      '&api_key=' + encodeURIComponent(props.serpApiKey) +
      '&num=10';

    if (props.serpApiLocation) url += '&location=' + encodeURIComponent(props.serpApiLocation);
    if (String(props.serpApiNoCache).toLowerCase() === 'true') url += '&no_cache=true';

    return {
      url,
      method: 'get',
      muteHttpExceptions: true,
      serpQuery: rawQuery
    };
  });

  const responses = UrlFetchApp.fetchAll(requests);

  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    const resp = responses[i];
    const requestUrl = requests[i].url;
    const requestQuery = requests[i].serpQuery;

    try {
      const code = resp.getResponseCode();
      const text = resp.getContentText() || '';

      if (props.serpDebug) {
        logSerpDebug_(requestUrl, resp, requestQuery);
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

      const price = median_(candidates);
      const serpResultForDescription = getSerpResultForDescription_(results);
      const description = applyItemDescription_(it.name, null, serpResultForDescription);
      const sourceUrl = extractSerpSourceUrl_(results);
      const vendor = extractSerpVendor_(results, sourceUrl);
      if (isFinite(price) && price > 0) {
        const key = cacheKeyForQuery_(it.query);
        cache.put(
          key,
          JSON.stringify({
            usd: price,
            description: description,
            source_url: sourceUrl,
            vendor: vendor
          }),
          ttl
        );
        byId[it.id] = { usd: price, description: description, source_url: sourceUrl, vendor: vendor, source: 'serpapi' };
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
  const rawQuery = getSerpQuery_(
    itemQuery == null ? 'apples' : itemQuery,
    'SerpAPI debug'
  );
  const query = encodeURIComponent(rawQuery);
  let url = 'https://serpapi.com/search?engine=' + encodeURIComponent(engine) +
    '&q=' + query +
    '&api_key=' + encodeURIComponent(props.serpApiKey) +
    '&num=10';

  if (props.serpApiLocation) url += '&location=' + encodeURIComponent(props.serpApiLocation);
  if (String(props.serpApiNoCache).toLowerCase() === 'true') url += '&no_cache=true';

  const resp = UrlFetchApp.fetch(url, { method: 'get', muteHttpExceptions: true });
  if (props.serpDebug) {
    logSerpDebug_(url, resp, rawQuery);
  } else {
    Logger.log('SERP_DEBUG is false; no SerpAPI debug output will be logged.');
  }
}

function logSerpDebug_(url, resp, query) {
  const code = resp.getResponseCode();
  const raw = resp.getContentText() || '';
  Logger.log('SERP_DEBUG status: %s', code);
  Logger.log('SERP_DEBUG url: %s', redactSecrets_(url));
  if (query) Logger.log('SERP_DEBUG q: %s', query);
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

function getFirstAvailableByDescriptions_(descriptions, sheet) {
  if (!sheet || !descriptions || !descriptions.length) return {};

  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return {};

  const header = values[0];
  const idx = headerIndex_(header);
  const desired = {};
  descriptions.forEach(desc => {
    const key = normalizeDescription_(String(desc || '').trim());
    if (key) desired[key] = true;
  });

  const out = {};
  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const desc = String(row[idx.item_description] || '').trim();
    const normalized = normalizeDescription_(desc);
    if (!normalized || !desired[normalized]) continue;

    const tsValue = row[idx.timestamp];
    const ts = tsValue ? new Date(tsValue).toISOString() : null;
    const usd = Number(row[idx.usd]);
    const sats = Number(row[idx.sats]);
    if (!ts || !isFinite(usd) || !isFinite(sats)) continue;

    if (!out[normalized]) {
      out[normalized] = { ts, usd, sats, item_description: desc };
      continue;
    }

    if (new Date(ts) < new Date(out[normalized].ts)) {
      out[normalized] = { ts, usd, sats, item_description: desc };
    }
  }
  return out;
}

function getVendorCountsByDescriptions_(descriptions, sheet) {
  if (!sheet || !descriptions || !descriptions.length) return {};

  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return {};

  const header = values[0];
  const idx = headerIndex_(header);
  if (idx.price_vendor == null) return {};

  const desired = {};
  descriptions.forEach(desc => {
    const key = normalizeDescription_(String(desc || '').trim());
    if (key) desired[key] = true;
  });

  const out = {};

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const rowDescription = String(row[idx.item_description] || '').trim();
    if (!rowDescription) continue;
    const normalized = normalizeDescription_(rowDescription);
    if (!desired[normalized]) continue;
    const rowPriceVendor = String(row[idx.price_vendor] || '').trim();
    if (!rowPriceVendor) {
      continue;
    }
    if (!out[normalized]) out[normalized] = {};
    out[normalized][rowPriceVendor] = true;
  }

  const counts = {};
  Object.keys(out).forEach(key => {
    counts[key] = Object.keys(out[key]).length;
  });

  return counts;
}

/**
 * Find the last known USD for an item description by searching upward. Used when providers are down.
 * Assumes your history sheet schema where:
 *   item_description is column 6, usd is column 7 (1-indexed)
 */
function getLastKnownUsd_(itemId, sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;

  const ITEM_DESCRIPTION_COL = 6;
  const USD_COL = 7;

  const chunkSize = 200;
  for (let end = lastRow; end >= 2; end -= chunkSize) {
    const start = Math.max(2, end - chunkSize + 1);
    const values = sheet.getRange(start, 1, end - start + 1, Math.max(USD_COL, ITEM_DESCRIPTION_COL)).getValues();
    for (let i = values.length - 1; i >= 0; i--) {
      const row = values[i];
      if (String(row[ITEM_DESCRIPTION_COL - 1]) === String(itemId)) {
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
    'basket_index_usd', 'basket_index_sats', 'price_source', 'price_vendor', 'source_url', 'is_stale'
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

  const normalized = header.map(h => String(h).trim());
  desiredHeader.forEach((desired, idx) => {
    if (normalized.indexOf(desired) !== -1) return;
    const column = idx + 1;
    sh.insertColumnBefore(column);
    sh.getRange(1, column).setValue(desired);
    normalized.splice(idx, 0, desired);
  });

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
    price_vendor: m.price_vendor !== undefined ? m.price_vendor : null,
    source_url: m.source_url !== undefined ? m.source_url : null,
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

function validateSheetProps_(p) {
  const missing = [];
  if (!p.dataSheetName) missing.push('DATA_SHEET_NAME');
  if (!p.itemList) missing.push('ITEM_LIST');
  if (missing.length) {
    throw new Error('Missing Script Properties: ' + missing.join(', '));
  }
}

function parseItems_(itemList) {
  const raw = itemList.split(',').map(s => s.trim()).filter(Boolean);
  let items = [];
  raw.forEach(token => {
    if (token.includes('|')) {
      const parts = token.split('|').map(x => x.trim());
      const id = parts[0];
      const name = parts[1] || parts[0];
      const query = parts[2] || parts[0];
      items.push({ id, name, query });
    } else {
      items.push({ id: slug_(token), name: title_(token), query: defaultQueryForItem_(token) });
    }
  });
  items = normalizeBasketItems_(items);
  ensureCommodityItems_(items);
  validateBasketComposition_(items);
  return items;
}

function normalizeBasketItems_(items) {
  const normalized = [];
  const seenIds = {};

  (items || []).forEach(item => {
    const id = String(item && item.id || '').trim().toLowerCase();
    if (!id || seenIds[id]) return;
    seenIds[id] = true;
    normalized.push(item);
  });

  const grocery = normalized.filter(item => !isReferenceItemId_(item.id));
  const cappedGrocery = grocery.slice(0, 10);

  return cappedGrocery;
}


function validateBasketComposition_(items) {
  const groceryCount = (items || []).filter(item => !isReferenceItemId_(item.id)).length;
  const totalCount = (items || []).length;
  if (groceryCount !== 10 || totalCount !== 15) {
    throw new Error('Basket composition must include exactly 10 grocery items and 5 reference items (15 total). Update ITEM_LIST to include at least 10 groceries.');
  }
}

function ensureCommodityItems_(items) {
  const ids = new Set(items.map(item => String(item.id || '').trim().toLowerCase()));
  const addCommodity = (id, name) => {
    if (ids.has(id)) return;
    items.push({ id, name, query: defaultQueryForItem_(name) });
    ids.add(id);
  };
  addCommodity('gold', 'Gold');
  addCommodity('silver', 'Silver');
  addCommodity('mwh', '5 kWh');
  addCommodity('cash10', '$10');
  addCommodity('sats10000', '10,000 Satoshis');
}

function defaultQueryForItem_(itemName) {
  const normalized = String(itemName || '').trim().toLowerCase();
  const overrides = {
    gold: '0.1 gram gold bar',
    silver: '1 gram silver bar',
    mwh: '5 kWh electricity',
    'ground beef': 'ground beef 80/20 1 lb',
    salt: 'iodized table salt 26 oz'
  };
  return overrides[normalized] || itemName;
}

function getSerpQuery_(rawQuery, context) {
  const trimmed = String(rawQuery || '').trim();
  if (!trimmed) {
    const suffix = context ? ` for ${context}` : '';
    throw new Error(`Missing SerpAPI query${suffix}. Provide a non-empty q value.`);
  }
  return trimmed;
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
function normalizeDescription_(s) {
  return String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, '');
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
function median_(arr) {
  const nums = arr.map(Number).filter(n => isFinite(n)).sort((a, b) => a - b);
  if (!nums.length) return 0;
  const mid = Math.floor(nums.length / 2);
  if (nums.length % 2) return nums[mid];
  return (nums[mid - 1] + nums[mid]) / 2;
}
function usdToSats_(usd, btcUsd) { return (usd / btcUsd) * 100000000; }
function num_(v) {
  if (v == null) return NaN;
  const n = Number(String(v).replace(/[^0-9.]/g, ''));
  return isFinite(n) ? n : NaN;
}

/**
 * Generic "median price" extraction. Works with many common RapidAPI shopping schemas.
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
  return median_(candidates);
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

function extractItemSourceUrl_(data, maxInspect) {
  const arr = (Array.isArray(data) && data) ||
    (Array.isArray(data.products) && data.products) ||
    (Array.isArray(data.results) && data.results) ||
    (Array.isArray(data.items) && data.items) ||
    [];
  const fields = [
    'product_url', 'productUrl', 'url', 'product_link', 'productLink', 'link',
    'detail_url', 'detailUrl', 'item_url', 'itemUrl', 'product_page_url',
    'product_page_link', 'source_url', 'sourceUrl'
  ];

  for (let i = 0; i < Math.min(arr.length, maxInspect); i++) {
    const item = arr[i] || {};
    for (let f = 0; f < fields.length; f++) {
      const value = item[fields[f]];
      if (typeof value !== 'string') continue;
      const trimmed = value.trim();
      if (trimmed && /^https?:\/\//i.test(trimmed)) return trimmed;
    }
  }
  return '';
}

function extractItemVendor_(data, maxInspect, sourceUrl) {
  const arr = (Array.isArray(data) && data) ||
    (Array.isArray(data.products) && data.products) ||
    (Array.isArray(data.results) && data.results) ||
    (Array.isArray(data.items) && data.items) ||
    [];
  const fields = [
    'source', 'seller', 'merchant', 'merchant_name', 'seller_name',
    'store', 'store_name', 'shop', 'shop_name', 'retailer', 'retailer_name',
    'vendor', 'vendor_name', 'marketplace', 'provider'
  ];

  for (let i = 0; i < Math.min(arr.length, maxInspect); i++) {
    const item = arr[i] || {};
    for (let f = 0; f < fields.length; f++) {
      const value = item[fields[f]];
      if (value == null) continue;
      const text = String(value).trim();
      if (text) return text;
    }
    if (Array.isArray(item.offers)) {
      for (let o = 0; o < item.offers.length; o++) {
        const offer = item.offers[o] || {};
        for (let f = 0; f < fields.length; f++) {
          const value = offer[fields[f]];
          if (value == null) continue;
          const text = String(value).trim();
          if (text) return text;
        }
      }
    }
    if (item.offer && typeof item.offer === 'object') {
      for (let f = 0; f < fields.length; f++) {
        const value = item.offer[fields[f]];
        if (value == null) continue;
        const text = String(value).trim();
        if (text) return text;
      }
    }
  }

  return vendorFromUrl_(sourceUrl);
}

function getSerpResultForDescription_(results) {
  if (!Array.isArray(results)) return null;
  for (let i = 0; i < results.length; i++) {
    const res = results[i];
    if (res && res.snippet) return res;
  }
  return results.length ? results[0] : null;
}

function extractSerpSourceUrl_(results) {
  if (!Array.isArray(results)) return '';
  const fields = ['link', 'product_link', 'productLink', 'url', 'product_url', 'productUrl'];
  for (let i = 0; i < results.length; i++) {
    const res = results[i] || {};
    for (let f = 0; f < fields.length; f++) {
      const value = res[fields[f]];
      if (typeof value !== 'string') continue;
      const trimmed = value.trim();
      if (trimmed && /^https?:\/\//i.test(trimmed)) return trimmed;
    }
  }
  return '';
}

function extractSerpVendor_(results, sourceUrl) {
  if (Array.isArray(results)) {
    const fields = ['source', 'seller', 'merchant', 'store', 'shop', 'retailer', 'vendor'];
    for (let i = 0; i < results.length; i++) {
      const res = results[i] || {};
      for (let f = 0; f < fields.length; f++) {
        const value = res[fields[f]];
        if (value == null) continue;
        const text = String(value).trim();
        if (text) return text;
      }
    }
  }
  return vendorFromUrl_(sourceUrl);
}

function vendorFromUrl_(url) {
  if (!url) return '';
  const match = String(url).match(/^https?:\/\/([^/]+)/i);
  if (!match) return '';
  let host = match[1].toLowerCase();
  host = host.replace(/^www\./, '');
  if (!host) return '';
  const parts = host.split('.');
  if (parts.length <= 2) return host;
  return parts.slice(-2).join('.');
}

function getStandardItemDescription_(itemName) {
  const normalized = String(itemName || '').trim().toLowerCase();
  const descriptionOverrides = {
    apples: 'Honeycrisp apples 3 lb bag',
    bananas: '1 Banana',
    eggs: 'Grade A large eggs 12 count',
    milk: 'Whole milk 1 gallon',
    butter: 'Unsalted butter 16 oz',
    bread: 'Sandwich bread 20 oz loaf',
    rice: 'Long grain white rice 5 lb bag',
    chicken: 'Boneless skinless chicken breast 2 lb',
    'ground beef': 'Ground beef 80 20 1 lb',
    potatoes: 'Russet potatoes 5 lb bag',
    'yellow onions': 'Yellow onions 3 lb bag',
    salt: 'Iodized table salt 26 oz',
    gold: 'Gold 0.1 gram bar',
    silver: 'Silver 1 gram bar',
    mwh: 'Electricity 5 kWh',
    sats10000: '10,000 satoshis'
  };
  if (descriptionOverrides[normalized]) return descriptionOverrides[normalized];
  return '';
}

function getItemDescription_(itemName, serpResult) {
  const standard = getStandardItemDescription_(itemName);
  if (standard) return standard;

  if (serpResult && serpResult.snippet) {
    const snippet = String(serpResult.snippet).trim();
    if (snippet) return snippet;
  }

  const normalized = String(itemName || '').trim().toLowerCase();

  switch (normalized) {
    case 'honeycrisp apples 3 lb bag':
      return 'Fresh Honeycrisp apples, sweet and crisp, typically sold in 3 lb produce bags.';
    case 'grade a large eggs 12 count':
      return 'Grade A large chicken eggs, commonly sold in cartons of twelve.';
    case 'whole milk 1 gallon':
      return 'Pasteurized whole cow’s milk, one gallon container.';
    case 'unsalted butter 16 oz':
      return 'Unsalted butter made from cream, standard 16 oz package.';
    case 'sandwich bread 20 oz loaf':
      return 'Sliced sandwich bread loaf, approximately 20 oz.';
    case 'long grain white rice 5 lb bag':
      return 'Long grain white rice, dry uncooked grains, 5 lb bag.';
    case 'boneless skinless chicken breast 2 lb':
      return 'Boneless, skinless chicken breast meat, approximately 2 lb package.';
    case 'ground beef 80 20 1 lb':
      return 'Ground beef with 80% lean meat and 20% fat, 1 lb package.';
    case 'russet potatoes 5 lb bag':
      return 'Russet potatoes suitable for baking and frying, 5 lb bag.';
    case 'yellow onions 3 lb bag':
      return 'Yellow onions commonly used for cooking, 3 lb bag.';
    case 'iodized table salt 26 oz':
      return 'Iodized table salt, approximately 26 oz container.';
    case 'gold 0.1 gram bar':
      return 'Gold bullion bar weighing approximately 0.1 gram.';
    case 'silver 1 gram bar':
      return 'Silver bullion bar weighing approximately 1 gram.';
    case 'electricity 5 kwh':
      return 'Electricity energy quantity equivalent to 5 kilowatt-hours.';
    case '10,000 satoshis':
      return 'Bitcoin quantity equal to 10,000 satoshis (0.0001 BTC).';
    default: {
      const fallback = String(itemName || '').trim();
      return fallback || 'Grocery item.';
    }
  }
}

function applyItemDescription_(itemName, candidate, serpResult) {
  const standard = getStandardItemDescription_(itemName);
  if (standard) return standard;
  if (candidate) return candidate;
  return getItemDescription_(itemName, serpResult);
}
