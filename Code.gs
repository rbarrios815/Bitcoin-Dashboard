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
  const id = String(itemId || '').trim().toLowerCase();
  if (id === 'mwh') return 0;
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

function getTrackedQuantityMeta_(itemId) {
  const item = getCatalogItemById_(itemId);
  if (item) {
    return { quantity: item.target_quantity, unit: item.target_unit };
  }
  return { quantity: 1, unit: 'item' };
}

function computeWeightedBasketIndex_(items) {
  let weightedUsdTotal = 0;
  let weightedSatsTotal = 0;

  (items || []).forEach(item => {
    const usd = Number(item && item.usd);
    const sats = Number(item && item.sats);
    const weight = basketWeightForItemId_(item && item.id);
    const valid = String(item && item.validation_status || '').toLowerCase();
    if (valid && valid !== 'validated' && !isFixedBasketItemId_(item && item.id)) return;
    if (!isFinite(usd) || !isFinite(sats) || !isFinite(weight) || weight <= 0) return;
    weightedUsdTotal += usd * weight;
    weightedSatsTotal += sats * weight;
  });

  return {
    usd: weightedUsdTotal,
    sats: weightedSatsTotal
  };
}

function fetchLatestSnapshot(options) {
  const props = getProps_();
  validateProps_(props);

  const opts = Object.assign({ includeRawOffers: false, forceFresh: false }, options || {});
  const cache = CacheService.getScriptCache();
  const cacheKey = 'LATEST_SNAPSHOT_V3_COMPACT';
  if (!opts.forceFresh) {
    const cached = cache.get(cacheKey);
    if (cached) return JSON.parse(cached);
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const historySheet = ss.getSheetByName(props.dataSheetName);
  const snapshotTs = new Date().toISOString();
  const btcUsd = fetchBtcUsd_(props);
  const items = parseItems_(props.itemList);
  const fetchableItems = items.filter(item => !isFixedBasketItemId_(item.id));

  const rapid = fetchItemsUsdRapidApiBatch_(fetchableItems, props);
  const needSerp = fetchableItems.filter(item => {
    const result = rapid.byId[item.id];
    return !result || result.validation_status !== 'validated';
  });
  const serpById = props.serpApiKey && needSerp.length
    ? fetchItemsUsdSerpApiBatch_(needSerp, props)
    : {};

  const rawOffers = [];
  const priced = items.map(item => {
    if (isFixedBasketItemId_(item.id)) {
      const fixed = getFixedBasketValue_(item.id, btcUsd);
      return buildFixedSnapshotItem_(item, fixed, snapshotTs);
    }

    const chosen = selectBestFetchResult_(item, [rapid.byId[item.id], serpById[item.id]]);
    collectRawOffers_(rawOffers, snapshotTs, item, rapid.byId[item.id]);
    collectRawOffers_(rawOffers, snapshotTs, item, serpById[item.id]);

    if (chosen && chosen.validation_status === 'validated' && isFinite(chosen.usd) && chosen.usd > 0) {
      return buildValidatedSnapshotItem_(item, chosen, btcUsd, snapshotTs);
    }

    if (props.allowStaleFallback && historySheet) {
      const last = getLastKnownValidatedRow_(item.id, historySheet);
      if (last && isFinite(last.usd) && last.usd > 0) {
        return buildFallbackSnapshotItem_(item, last, btcUsd, snapshotTs);
      }
    }

    return buildErrorSnapshotItem_(item, chosen, snapshotTs);
  });

  const firstAvailableByDescription = historySheet
    ? getFirstAvailableByDescriptions_(priced.map(p => p.item_description), historySheet)
    : {};
  const vendorCountsByDescription = historySheet
    ? getVendorCountsByDescriptions_(priced.map(p => p.item_description), historySheet)
    : {};

  const enriched = priced.map(item => {
    const normalized = normalizeDescription_(item.item_description);
    const firstAvailable = firstAvailableByDescription[normalized] || null;
    const vendorCount = vendorCountsByDescription[normalized] || 0;
    return Object.assign({}, item, { first_available: firstAvailable, vendor_count: vendorCount });
  });

  const weightedBasket = computeWeightedBasketIndex_(enriched);
  const fullOut = {
    ts: snapshotTs,
    btcUsd,
    items: enriched,
    rawOffers,
    basketIndexUsd: weightedBasket.usd,
    basketIndexSats: weightedBasket.sats
  };
  const compactOut = buildCompactSnapshotForCache_(fullOut);

  safeCachePutJson_(cache, cacheKey, compactOut, 60 * 10);
  return opts.includeRawOffers ? fullOut : compactOut;
}

function recordSnapshot() {
  const props = getProps_();
  validateProps_(props);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const historySheet = getOrCreateHistorySheet_(ss, props.dataSheetName);
  const rawOffersSheet = getOrCreateRawOffersSheet_(ss, props.rawOffersSheetName);

  const snap = fetchLatestSnapshot({ includeRawOffers: true, forceFresh: true });
  const ts = new Date(snap.ts);

  const rows = snap.items
    .filter(it => !isFixedBasketItemId_(it.id))
    .map(it => ([
      ts,
      snap.btcUsd,
      it.id,
      it.name,
      it.query,
      it.canonical_query || it.query,
      it.item_description,
      it.raw_vendor_title || '',
      it.raw_vendor_title || '',
      it.usd,
      it.sats,
      it.normalized_price || '',
      it.normalized_unit || '',
      snap.basketIndexUsd,
      snap.basketIndexSats,
      it.price_source,
      it.price_vendor,
      it.source_url,
      it.is_stale,
      it.match_score || '',
      it.validation_status || '',
      it.fail_reason || ''
    ]));

  if (rows.length) {
    historySheet.getRange(historySheet.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
  }

  if (snap.rawOffers && snap.rawOffers.length) {
    const rawRows = snap.rawOffers.map(ofr => ([
      ts,
      ofr.item_id,
      ofr.vendor,
      ofr.raw_vendor_title,
      ofr.raw_price,
      ofr.parsed_quantity || '',
      ofr.parsed_unit || '',
      ofr.normalized_price || '',
      ofr.normalized_unit || '',
      ofr.pass ? 'pass' : 'fail',
      ofr.fail_reason || '',
      ofr.match_score || '',
      ofr.source_url || '',
      ofr.price_source || ''
    ]));
    rawOffersSheet.getRange(rawOffersSheet.getLastRow() + 1, 1, rawRows.length, rawRows[0].length).setValues(rawRows);
  }

  return { ok: true, recordedAt: ts.toISOString(), rowsAppended: rows.length, rawOffersAppended: snap.rawOffers ? snap.rawOffers.length : 0 };
}

function getConfig() {
  const props = getProps_();
  return {
    items: parseItems_(props.itemList).map(x => ({ id: x.id, name: x.name, query: x.query, canonical_description: x.canonical_description })),
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
    try { fallbackBtcUsd = fetchBtcUsd_(props); } catch (e) { fallbackBtcUsd = null; }
    return {
      ts: null,
      btcUsd: isFinite(fallbackBtcUsd) ? fallbackBtcUsd : null,
      items: items.map(it => buildEmptySheetSnapshotItem_(it, fallbackBtcUsd)),
      basketIndexUsd: 0,
      basketIndexSats: 0
    };
  }

  const values = sheet.getDataRange().getValues();
  const idx = headerIndex_(values[0]);
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
    const ts = parseSheetDate_(row[idx.timestamp]);
    if (!ts) continue;
    const normalized = normalizeDescription_(desc);
    const rowDetails = buildSheetRowDetails_(row, idx, desc);
    const rowItemId = rowDetails.item_id;
    const rowItemName = rowDetails.item_name;

    if (normalized) {
      if (!vendorCountsByDescription[normalized]) vendorCountsByDescription[normalized] = {};
      if (rowDetails.price_vendor) vendorCountsByDescription[normalized][rowDetails.price_vendor] = true;
      if (!latestByDescription[normalized] || ts > latestByDescription[normalized].ts) latestByDescription[normalized] = rowDetails;
      if (!earliestByDescription[normalized] || ts < earliestByDescription[normalized].ts) earliestByDescription[normalized] = { ts, usd: rowDetails.usd, sats: rowDetails.sats };
    }
    if (rowItemId && (!latestById[rowItemId] || ts > latestById[rowItemId].ts)) latestById[rowItemId] = rowDetails;
    if (rowItemId && (!earliestById[rowItemId] || ts < earliestById[rowItemId].ts)) earliestById[rowItemId] = { ts, usd: rowDetails.usd, sats: rowDetails.sats };
    if (rowItemName && (!latestByName[rowItemName] || ts > latestByName[rowItemName].ts)) latestByName[rowItemName] = rowDetails;
    if (rowItemName && (!earliestByName[rowItemName] || ts < earliestByName[rowItemName].ts)) earliestByName[rowItemName] = { ts, usd: rowDetails.usd, sats: rowDetails.sats };

    if (!latestTs || ts > latestTs) {
      latestTs = ts;
      latestBasketUsd = Number(row[idx.basket_index_usd]);
      latestBasketSats = Number(row[idx.basket_index_sats]);
      latestBtcUsd = Number(row[idx.btc_usd]);
    }
  }

  const payloadItems = items.map(it => {
    const fallbackDescription = it.canonical_description;
    const normalized = normalizeDescription_(fallbackDescription);
    const nameKey = String(it.name || '').trim();
    const isNameUnique = nameKey && nameCounts[nameKey] === 1;
    const latestRow = latestById[it.id] || (isNameUnique ? latestByName[nameKey] : null) || latestByDescription[normalized] || null;
    const earliestRow = earliestById[it.id] || (isNameUnique ? earliestByName[nameKey] : null) || earliestByDescription[normalized] || null;
    if (isFixedBasketItemId_(it.id)) {
      const fixed = getFixedBasketValue_(it.id, latestBtcUsd);
      return Object.assign({}, buildFixedSnapshotItem_(it, fixed, latestTs ? latestTs.toISOString() : null), {
        first_available: earliestRow ? { ts: new Date(earliestRow.ts).toISOString(), usd: earliestRow.usd, sats: earliestRow.sats } : null,
        vendor_count: vendorCountsByDescription[normalized] ? Object.keys(vendorCountsByDescription[normalized]).length : 0
      });
    }
    return Object.assign({}, buildSnapshotItemFromHistory_(it, latestRow, latestTs), {
      first_available: earliestRow ? { ts: new Date(earliestRow.ts).toISOString(), usd: earliestRow.usd, sats: earliestRow.sats } : null,
      vendor_count: vendorCountsByDescription[normalized] ? Object.keys(vendorCountsByDescription[normalized]).length : 0
    });
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
    out.push({ ts: ts.toISOString(), usd: fixed.usd, sats: fixed.sats, btcUsd: btcUsd, price_source: 'fixed', price_vendor: '', source_url: '', is_stale: false, validation_status: 'validated', match_score: 1 });
  }
  out.sort((a,b) => new Date(a.ts) - new Date(b.ts));
  return dedupeHistoryByTs_(out);
}

function getItemHistory(itemDescription) {
  const props = getProps_();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(props.dataSheetName);
  if (!sheet) return [];

  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];

  const idx = headerIndex_(values[0]);
  const out = [];
  const targetDescription = String(itemDescription || '').trim();
  const normalizedTarget = normalizeDescription_(targetDescription);
  if (normalizedTarget === normalizeDescription_('10,000 satoshis')) return buildFixedItemHistoryFromSheet_(sheet, 'sats10000', idx);
  if (normalizedTarget === normalizeDescription_('$10')) return buildFixedItemHistoryFromSheet_(sheet, 'cash10', idx);

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const rowDescription = String(row[idx.item_description] || '').trim();
    if (!rowDescription || normalizeDescription_(rowDescription) !== normalizedTarget) continue;
    out.push(buildHistoryPointFromRow_(row, idx, rowDescription));
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

  const idx = headerIndex_(values[0]);
  const outByNormalized = {};
  const targets = Array.isArray(itemDescriptions) ? itemDescriptions : [];
  const targetSet = {};
  const useFilter = targets.length > 0;
  targets.forEach(desc => {
    const cleaned = String(desc || '').trim();
    const normalized = normalizeDescription_(cleaned);
    if (!normalized) return;
    targetSet[normalized] = true;
    if (!outByNormalized[normalized]) outByNormalized[normalized] = { description: cleaned, history: [] };
  });

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const rowDescription = String(row[idx.item_description] || '').trim();
    if (!rowDescription) continue;
    const normalized = normalizeDescription_(rowDescription);
    if (useFilter && !targetSet[normalized]) continue;
    if (!outByNormalized[normalized]) outByNormalized[normalized] = { description: rowDescription, history: [] };
    outByNormalized[normalized].history.push(buildHistoryPointFromRow_(row, idx, rowDescription));
  }

  [
    { normalized: normalizeDescription_('10,000 satoshis'), id: 'sats10000', label: '10,000 satoshis' },
    { normalized: normalizeDescription_('$10'), id: 'cash10', label: '$10' }
  ].forEach(fixed => {
    if (!targetSet[fixed.normalized]) return;
    if (!outByNormalized[fixed.normalized]) outByNormalized[fixed.normalized] = { description: fixed.label, history: [] };
    const merged = {};
    (outByNormalized[fixed.normalized].history || []).forEach(point => { if (point && point.ts) merged[point.ts] = point; });
    buildFixedItemHistoryFromSheet_(sheet, fixed.id, idx).forEach(point => { if (point && point.ts) merged[point.ts] = point; });
    outByNormalized[fixed.normalized].history = Object.keys(merged).sort((a,b) => new Date(a) - new Date(b)).map(ts => merged[ts]);
  });

  return Object.keys(outByNormalized).map(key => {
    const entry = outByNormalized[key];
    entry.history.sort((a,b) => new Date(a.ts) - new Date(b.ts));
    return entry;
  }).sort((a,b) => a.description.localeCompare(b.description));
}

function getBasketHistory() {
  const props = getProps_();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(props.dataSheetName);
  if (!sheet) return [];
  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];
  const idx = headerIndex_(values[0]);
  const snapshots = {};

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const ts = row[idx.timestamp];
    if (!ts) continue;
    const tsIso = new Date(ts).toISOString();
    if (!snapshots[tsIso]) snapshots[tsIso] = [];
    const validationStatus = idx.validation_status != null ? String(row[idx.validation_status] || '') : 'validated';
    snapshots[tsIso].push({
      id: idx.item_id != null ? String(row[idx.item_id] || '') : '',
      usd: convertElectricityUsdToKwh_(row[idx.usd], idx.item_id != null ? row[idx.item_id] : '', idx.item_name != null ? row[idx.item_name] : '', idx.item_description != null ? row[idx.item_description] : ''),
      sats: Number(row[idx.sats]),
      btcUsd: Number(row[idx.btc_usd]),
      validation_status: validationStatus || 'validated'
    });
  }

  Object.keys(snapshots).forEach(ts => {
    const rows = snapshots[ts] || [];
    const hasById = {};
    rows.forEach(row => { const id = String(row.id || '').trim().toLowerCase(); if (id) hasById[id] = true; });
    const btcUsd = average_(rows.map(item => Number(item.btcUsd)).filter(isFinite));
    ['cash10', 'sats10000'].forEach(id => {
      if (hasById[id]) return;
      const fixed = getFixedBasketValue_(id, btcUsd);
      if (!fixed) return;
      rows.push({ id, usd: fixed.usd, sats: fixed.sats, btcUsd, validation_status: 'validated' });
    });
  });

  return Object.keys(snapshots).map(ts => {
    const rows = snapshots[ts];
    const weighted = computeWeightedBasketIndex_(rows);
    return { ts, btcUsd: average_(rows.map(item => Number(item.btcUsd)).filter(isFinite)), basketIndexUsd: isFinite(weighted.usd) ? weighted.usd : 0, basketIndexSats: isFinite(weighted.sats) ? weighted.sats : 0 };
  }).sort((a,b) => new Date(a.ts) - new Date(b.ts));
}

function getBasketInflation() {
  const hist = getBasketHistory();
  if (hist.length < 2) {
    return { baselineTs: null, baselineUsd: null, baselineSats: null, currentTs: null, currentUsd: null, currentSats: null, inflationPctUsd: 0, inflationPctSats: 0, inflationPct: 0 };
  }
  const base = hist[0];
  const cur = hist[hist.length - 1];
  const inflationPctUsd = (base.basketIndexUsd && cur.basketIndexUsd) ? ((cur.basketIndexUsd / base.basketIndexUsd) - 1) * 100 : 0;
  const inflationPctSats = (base.basketIndexSats && cur.basketIndexSats) ? ((cur.basketIndexSats / base.basketIndexSats) - 1) * 100 : 0;
  return { baselineTs: base.ts, baselineUsd: base.basketIndexUsd, baselineSats: base.basketIndexSats, currentTs: cur.ts, currentUsd: cur.basketIndexUsd, currentSats: cur.basketIndexSats, inflationPctUsd, inflationPctSats, inflationPct: inflationPctUsd };
}

function getPurchasingPowerDashboardData() {
  const props = getProps_();
  validateSheetProps_(props);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(props.dataSheetName);
  if (!sheet || sheet.getLastRow() < 2) {
    return { generatedAt: new Date().toISOString(), snapshots: [], items: [], quality: { currentItems: 0, staleItems: 0, missingItems: 0, vendorInconsistencyCount: 0, currentSnapshotHasStale: false }, fieldsDetected: {} };
  }

  const values = sheet.getDataRange().getValues();
  const header = values[0].map(h => String(h || '').trim());
  const idx = buildDashboardHeaderIndex_(header);
  const configuredItems = parseItems_(props.itemList).map(item => ({ id: item.id, name: item.name }));
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
      snapshotsByTs[rowIso] = { ts: rowIso, btcUsd: safeNumber_(idx.btc_usd != null ? row[idx.btc_usd] : null), basketUsd: safeNumber_(idx.basket_index_usd != null ? row[idx.basket_index_usd] : null), basketSats: safeNumber_(idx.basket_index_sats != null ? row[idx.basket_index_sats] : null), itemRows: [] };
    }
    const itemId = idx.item_id != null ? String(row[idx.item_id] || '').trim() : '';
    const itemName = idx.item_name != null ? String(row[idx.item_name] || '').trim() : '';
    const description = idx.item_description != null ? String(row[idx.item_description] || '').trim() : '';
    const rawVendorTitle = idx.raw_vendor_title != null ? String(row[idx.raw_vendor_title] || '').trim() : (idx.source_item_description != null ? String(row[idx.source_item_description] || '').trim() : '');
    const source = idx.price_source != null ? String(row[idx.price_source] || '').trim() : '';
    const vendor = idx.price_vendor != null ? String(row[idx.price_vendor] || '').trim() : '';
    const group = idx.group != null ? String(row[idx.group] || '').trim() : '';
    const isStale = idx.is_stale != null ? Boolean(row[idx.is_stale]) : /^last_known/i.test(source);
    const usd = deriveUsdValue_(row, idx);
    const btcUsd = safeNumber_(idx.btc_usd != null ? row[idx.btc_usd] : null);
    const sats = deriveSatsValue_(row, idx, usd, btcUsd);
    const fallbackKey = description || itemName || itemId || `row_${r}`;
    const itemKey = normalizeDescription_(fallbackKey);
    if (!itemHistoryByKey[itemKey]) itemHistoryByKey[itemKey] = { key: itemKey, itemId: itemId || '', itemName: itemName || configuredNameById[itemId] || description || itemId || 'Unknown Item', description: description || itemName || itemId || 'Unknown Item', source_item_description: rawVendorTitle, history: [] };
    itemHistoryByKey[itemKey].history.push({ ts: rowIso, usd, sats, btcUsd, vendor, source, group, is_stale: isStale });
    if (!vendorSetByItem[itemKey]) vendorSetByItem[itemKey] = {};
    if (vendor) vendorSetByItem[itemKey][vendor] = true;
    snapshotsByTs[rowIso].itemRows.push({ itemKey, itemId, itemName: itemName || description || itemId || 'Unknown Item', description: description || itemName || itemId || 'Unknown Item', source_item_description: rawVendorTitle, usd, sats, btcUsd, vendor, source, group, is_stale: isStale });
  }

  const snapshotKeys = Object.keys(snapshotsByTs).sort((a,b) => new Date(a) - new Date(b));
  snapshotKeys.forEach(tsIso => {
    const snapshot = snapshotsByTs[tsIso];
    const btcUsd = isFinite(snapshot.btcUsd) ? snapshot.btcUsd : average_(snapshot.itemRows.map(row => row.btcUsd));
    if (!isFinite(btcUsd) || btcUsd <= 0) return;
    [{ id: 'cash10', name: '$10', description: '$10' }, { id: 'sats10000', name: '10,000 Satoshis', description: '10,000 satoshis' }].forEach(fixedMeta => {
      if (snapshot.itemRows.some(row => String(row.itemId || '').trim().toLowerCase() === fixedMeta.id)) return;
      const fixed = getFixedBasketValue_(fixedMeta.id, btcUsd);
      if (!fixed) return;
      const key = normalizeDescription_(fixedMeta.description);
      snapshot.itemRows.push({ itemKey: key, itemId: fixedMeta.id, itemName: fixedMeta.name, description: fixedMeta.description, usd: fixed.usd, sats: fixed.sats, btcUsd, vendor: '', source: 'fixed', group: '', is_stale: false });
      if (!itemHistoryByKey[key]) itemHistoryByKey[key] = { key, itemId: fixedMeta.id, itemName: fixedMeta.name, description: fixedMeta.description, history: [] };
      itemHistoryByKey[key].history.push({ ts: tsIso, usd: fixed.usd, sats: fixed.sats, btcUsd, vendor: '', source: 'fixed', group: '', is_stale: false });
      if (!vendorSetByItem[key]) vendorSetByItem[key] = {};
    });
  });

  const snapshots = snapshotKeys.map(tsIso => {
    const entry = snapshotsByTs[tsIso];
    const rows = entry.itemRows;
    const validUsd = rows.map(r => r.usd).filter(isFinite);
    const validSats = rows.map(r => r.sats).filter(isFinite);
    const staleCount = rows.filter(r => r.is_stale).length;
    const missingCount = rows.filter(r => !isFinite(r.usd) || !isFinite(r.sats)).length;
    return { ts: tsIso, btcUsd: isFinite(entry.btcUsd) ? entry.btcUsd : average_(rows.map(r => r.btcUsd)), basketUsd: isFinite(entry.basketUsd) ? entry.basketUsd : average_(validUsd), basketSats: isFinite(entry.basketSats) ? entry.basketSats : average_(validSats), itemCount: rows.length, staleCount, missingCount, groups: uniqueValues_(rows.map(r => r.group)), items: rows };
  });

  const itemList = Object.keys(itemHistoryByKey).map(key => {
    const entry = itemHistoryByKey[key];
    entry.history.sort((a,b) => new Date(a.ts) - new Date(b.ts));
    const latest = entry.history[entry.history.length - 1] || {};
    return { key: entry.key, itemId: entry.itemId, itemName: entry.itemName, description: entry.description, vendorCount: vendorSetByItem[key] ? Object.keys(vendorSetByItem[key]).length : 0, vendorChanged: vendorSetByItem[key] ? Object.keys(vendorSetByItem[key]).length > 1 : false, latestUsd: safeNumber_(latest.usd), latestSats: safeNumber_(latest.sats), latestVendor: latest.vendor || '', latestSource: latest.source || '', latestIsStale: Boolean(latest.is_stale), history: entry.history };
  }).sort((a,b) => a.description.localeCompare(b.description));

  const latestSnapshot = snapshots.length ? snapshots[snapshots.length - 1] : null;
  const quality = { currentItems: latestSnapshot ? latestSnapshot.itemCount : 0, staleItems: latestSnapshot ? latestSnapshot.staleCount : 0, missingItems: latestSnapshot ? latestSnapshot.missingCount : 0, vendorInconsistencyCount: itemList.filter(item => item.vendorChanged).length, currentSnapshotHasStale: latestSnapshot ? latestSnapshot.staleCount > 0 : false };
  return { generatedAt: new Date().toISOString(), snapshots, items: itemList, quality, fieldsDetected: { timestamp: idx.timestamp != null, item_name: idx.item_name != null, item_description: idx.item_description != null, source_item_description: idx.source_item_description != null, raw_vendor_title: idx.raw_vendor_title != null, price_vendor: idx.price_vendor != null, usd: idx.usd != null, btc_usd: idx.btc_usd != null, sats: idx.sats != null, basket_index_usd: idx.basket_index_usd != null, basket_index_sats: idx.basket_index_sats != null, is_stale: idx.is_stale != null, group: idx.group != null, validation_status: idx.validation_status != null } };
}

function buildDashboardHeaderIndex_(headerRow) {
  return {
    timestamp: findHeaderIndex_(headerRow, ['timestamp', 'date', 'datetime', 'ts']),
    btc_usd: findHeaderIndex_(headerRow, ['btc_usd', 'btc usd', 'btc/usd', 'exchange_rate', 'btc_rate']),
    item_id: findHeaderIndex_(headerRow, ['item_id', 'item id', 'id']),
    item_name: findHeaderIndex_(headerRow, ['item_name', 'item name', 'name']),
    item_description: findHeaderIndex_(headerRow, ['item_description', 'item description', 'description']),
    source_item_description: findHeaderIndex_(headerRow, ['source_item_description', 'source item description', 'raw_description', 'product_description']),
    raw_vendor_title: findHeaderIndex_(headerRow, ['raw_vendor_title', 'raw vendor title']),
    usd: findHeaderIndex_(headerRow, ['usd', 'price_usd', 'usd_price', 'price']),
    sats: findHeaderIndex_(headerRow, ['sats', 'satoshis', 'sats_price']),
    basket_index_usd: findHeaderIndex_(headerRow, ['basket_index_usd', 'basket usd', 'basket_total_usd', 'basket_usd']),
    basket_index_sats: findHeaderIndex_(headerRow, ['basket_index_sats', 'basket sats', 'basket_total_sats', 'basket_sats']),
    price_source: findHeaderIndex_(headerRow, ['price_source', 'source', 'vendor_source']),
    price_vendor: findHeaderIndex_(headerRow, ['price_vendor', 'vendor', 'source_vendor']),
    is_stale: findHeaderIndex_(headerRow, ['is_stale', 'stale', 'carried_forward']),
    validation_status: findHeaderIndex_(headerRow, ['validation_status', 'status']),
    group: findHeaderIndex_(headerRow, ['category', 'group', 'product_group'])
  };
}

function findHeaderIndex_(headerRow, candidates) {
  const normalizedMap = {};
  headerRow.forEach((label, idx) => { normalizedMap[String(label || '').trim().toLowerCase()] = idx; });
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
  if (isFinite(sats) && isFinite(btcUsd) && btcUsd > 0) return (sats / 100000000) * btcUsd;
  return NaN;
}

function deriveSatsValue_(row, idx, usd, btcUsd) {
  const direct = safeNumber_(idx.sats != null ? row[idx.sats] : null);
  if (isFinite(direct)) return direct;
  if (isFinite(usd) && isFinite(btcUsd) && btcUsd > 0) return usdToSats_(usd, btcUsd);
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
  const combined = `${String(itemName || '').trim().toLowerCase()} ${String(itemDescription || '').trim().toLowerCase()}`;
  return combined.indexOf('electric') !== -1 || combined.indexOf('kwh') !== -1 || combined.indexOf('mwh') !== -1;
}

function convertElectricityUsdToKwh_(usd, itemId, itemName, itemDescription) {
  const numeric = safeNumber_(usd);
  if (!isFinite(numeric)) return NaN;
  return isElectricityItemMeta_(itemId, itemName, itemDescription) ? (numeric / 1000) : numeric;
}

function uniqueValues_(values) {
  const out = {};
  (values || []).forEach(v => { const cleaned = String(v || '').trim(); if (cleaned) out[cleaned] = true; });
  return Object.keys(out).sort();
}

function isReferenceItemId_(itemId) {
  const id = String(itemId || '').trim().toLowerCase();
  return id === 'gold' || id === 'silver' || id === 'mwh' || id === 'cash10' || id === 'sats10000';
}

function fetchItemsUsdRapidApiBatch_(items, props) {
  const byId = {};
  if (!items.length) return { byId };
  const requests = items.map(item => {
    let url = addParam_(props.priceApiSearchUrl, 'product_title', item.canonical_query || item.query);
    if (props.countryCode) url = addParam_(url, 'country_code', props.countryCode);
    if (props.excludeDomains) url = addParam_(url, 'exclude_domains', props.excludeDomains);
    return { url, method: 'get', muteHttpExceptions: true, headers: { 'X-RapidAPI-Key': props.rapidApiKey, 'X-RapidAPI-Host': props.priceApiHost } };
  });
  const responses = UrlFetchApp.fetchAll(requests);
  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const resp = responses[i];
    try {
      const code = resp.getResponseCode();
      if (code >= 400) { byId[item.id] = { validation_status: 'rejected', error: `rapidapi_${code}`, offers: [], source: 'rapidapi' }; continue; }
      const data = JSON.parse(resp.getContentText() || '{}');
      byId[item.id] = evaluateProviderCandidates_(item, extractRapidApiCandidates_(data, props.maxResultsPerItem), 'rapidapi');
    } catch (e) {
      byId[item.id] = { validation_status: 'rejected', error: 'rapidapi_parse_error', offers: [], source: 'rapidapi' };
    }
  }
  return { byId };
}

function fetchItemsUsdSerpApiBatch_(items, props) {
  const byId = {};
  if (!items.length) return byId;
  const engine = props.serpApiEngine || 'google_shopping';
  const requests = items.map(item => {
    const rawQuery = getSerpQuery_(item.canonical_query || item.query, `SerpAPI batch item "${item.id}"`);
    let url = 'https://serpapi.com/search?engine=' + encodeURIComponent(engine) + '&q=' + encodeURIComponent(rawQuery) + '&api_key=' + encodeURIComponent(props.serpApiKey) + '&num=10';
    if (props.serpApiLocation) url += '&location=' + encodeURIComponent(props.serpApiLocation);
    if (String(props.serpApiNoCache).toLowerCase() === 'true') url += '&no_cache=true';
    return { url, method: 'get', muteHttpExceptions: true, serpQuery: rawQuery };
  });
  const responses = UrlFetchApp.fetchAll(requests);
  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const resp = responses[i];
    try {
      if (props.serpDebug) logSerpDebug_(requests[i].url, resp, requests[i].serpQuery);
      const code = resp.getResponseCode();
      if (code >= 400) { byId[item.id] = { validation_status: 'rejected', error: `serpapi_${code}`, offers: [], source: 'serpapi' }; continue; }
      const data = JSON.parse(resp.getContentText() || '{}');
      byId[item.id] = evaluateProviderCandidates_(item, extractSerpApiCandidates_(data), 'serpapi');
    } catch (e) {
      byId[item.id] = { validation_status: 'rejected', error: 'serpapi_parse_error', offers: [], source: 'serpapi' };
    }
  }
  return byId;
}

function runSerpDebugOnce(itemQuery) {
  const props = getProps_();
  if (!props.serpApiKey) throw new Error('Missing Script Property: SERPAPI_KEY');
  const engine = props.serpApiEngine || 'google_shopping';
  const rawQuery = getSerpQuery_(itemQuery == null ? 'apples' : itemQuery, 'SerpAPI debug');
  let url = 'https://serpapi.com/search?engine=' + encodeURIComponent(engine) + '&q=' + encodeURIComponent(rawQuery) + '&api_key=' + encodeURIComponent(props.serpApiKey) + '&num=10';
  if (props.serpApiLocation) url += '&location=' + encodeURIComponent(props.serpApiLocation);
  if (String(props.serpApiNoCache).toLowerCase() === 'true') url += '&no_cache=true';
  const resp = UrlFetchApp.fetch(url, { method: 'get', muteHttpExceptions: true });
  if (props.serpDebug) logSerpDebug_(url, resp, rawQuery);
}

function logSerpDebug_(url, resp, query) {
  const code = resp.getResponseCode();
  const raw = resp.getContentText() || '';
  Logger.log('SERP_DEBUG status: %s', code);
  Logger.log('SERP_DEBUG url: %s', redactSecrets_(url));
  if (query) Logger.log('SERP_DEBUG q: %s', query);
  Logger.log('SERP_DEBUG raw(0-5000): %s', redactSecrets_(raw.slice(0, 5000)));
}

function getFirstAvailableByDescriptions_(descriptions, sheet) {
  if (!sheet || !descriptions || !descriptions.length) return {};
  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return {};
  const idx = headerIndex_(values[0]);
  const desired = {};
  descriptions.forEach(desc => { const key = normalizeDescription_(String(desc || '').trim()); if (key) desired[key] = true; });
  const out = {};
  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const desc = String(row[idx.item_description] || '').trim();
    const normalized = normalizeDescription_(desc);
    if (!normalized || !desired[normalized]) continue;
    const ts = parseSheetDate_(row[idx.timestamp]);
    const usd = Number(row[idx.usd]);
    const sats = Number(row[idx.sats]);
    if (!ts || !isFinite(usd) || !isFinite(sats)) continue;
    if (!out[normalized] || ts < new Date(out[normalized].ts)) out[normalized] = { ts: ts.toISOString(), usd, sats, item_description: desc };
  }
  return out;
}

function getVendorCountsByDescriptions_(descriptions, sheet) {
  if (!sheet || !descriptions || !descriptions.length) return {};
  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return {};
  const idx = headerIndex_(values[0]);
  if (idx.price_vendor == null) return {};
  const desired = {};
  descriptions.forEach(desc => { const key = normalizeDescription_(String(desc || '').trim()); if (key) desired[key] = true; });
  const out = {};
  for (let r = 1; r < values.length; r++) {
    const rowDescription = String(values[r][idx.item_description] || '').trim();
    if (!rowDescription) continue;
    const normalized = normalizeDescription_(rowDescription);
    if (!desired[normalized]) continue;
    const vendor = String(values[r][idx.price_vendor] || '').trim();
    if (!vendor) continue;
    if (!out[normalized]) out[normalized] = {};
    out[normalized][vendor] = true;
  }
  const counts = {};
  Object.keys(out).forEach(key => counts[key] = Object.keys(out[key]).length);
  return counts;
}

function getLastKnownValidatedRow_(itemId, sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return null;
  const values = sheet.getDataRange().getValues();
  const idx = headerIndex_(values[0]);
  for (let r = values.length - 1; r >= 1; r--) {
    const row = values[r];
    if (String(row[idx.item_id] || '').trim().toLowerCase() !== String(itemId || '').trim().toLowerCase()) continue;
    const validationStatus = idx.validation_status != null ? String(row[idx.validation_status] || '').trim().toLowerCase() : 'validated';
    if (validationStatus !== 'validated') continue;
    const usd = Number(row[idx.usd]);
    if (!isFinite(usd) || usd <= 0) continue;
    return {
      usd,
      raw_vendor_title: idx.raw_vendor_title != null ? String(row[idx.raw_vendor_title] || '') : (idx.source_item_description != null ? String(row[idx.source_item_description] || '') : ''),
      source_url: idx.source_url != null ? String(row[idx.source_url] || '') : '',
      price_source: idx.price_source != null ? String(row[idx.price_source] || '') : 'last_known',
      price_vendor: idx.price_vendor != null ? String(row[idx.price_vendor] || '') : '',
      match_score: idx.match_score != null ? Number(row[idx.match_score]) : 1,
      normalized_price: idx.normalized_price != null ? Number(row[idx.normalized_price]) : NaN,
      normalized_unit: idx.normalized_unit != null ? String(row[idx.normalized_unit] || '') : '',
      validation_status: 'validated'
    };
  }
  return null;
}

function getOrCreateCatalog_() { return buildTrackedCatalog_(); }
function getCatalogItemById_(itemId) {
  const id = String(itemId || '').trim().toLowerCase();
  const catalog = getOrCreateCatalog_();
  for (let i = 0; i < catalog.length; i++) if (catalog[i].id === id) return catalog[i];
  return null;
}

function buildTrackedCatalog_() {
  const base = [
    { id: 'apples', name: 'Apples', canonical_description: 'Honeycrisp apples 3 lb bag', canonical_query: 'honeycrisp apples 3 lb bag', required_keywords: ['honeycrisp', 'apple'], excluded_keywords: ['organic slices', 'juice', 'cider'], target_unit: 'lb', target_quantity: 3, category: 'produce', active: true },
    { id: 'bananas', name: 'Bananas', canonical_description: 'Bananas 1 lb', canonical_query: 'bananas 1 lb', required_keywords: ['banana'], excluded_keywords: ['chips', 'baby food'], target_unit: 'lb', target_quantity: 1, category: 'produce', active: true },
    { id: 'eggs', name: 'Eggs', canonical_description: 'Grade A large eggs 12 count', canonical_query: 'grade a large eggs 12 count', required_keywords: ['egg'], excluded_keywords: ['liquid', 'substitute'], target_unit: 'count', target_quantity: 12, category: 'dairy', active: true },
    { id: 'milk', name: 'Milk', canonical_description: 'Whole milk 1 gallon', canonical_query: 'whole milk 1 gallon', required_keywords: ['whole', 'milk'], excluded_keywords: ['almond', 'oat', 'soy', '2%', 'skim'], target_unit: 'gallon', target_quantity: 1, category: 'dairy', active: true },
    { id: 'butter', name: 'Butter', canonical_description: 'Unsalted butter 16 oz', canonical_query: 'unsalted butter 16 oz', required_keywords: ['unsalted', 'butter'], excluded_keywords: ['salted', 'spread', 'margarine'], target_unit: 'oz', target_quantity: 16, category: 'dairy', active: true },
    { id: 'bread', name: 'Bread', canonical_description: 'Sandwich bread 20 oz loaf', canonical_query: 'sandwich bread 20 oz loaf', required_keywords: ['bread', 'sandwich'], excluded_keywords: ['bun', 'bagel', 'roll', 'gluten free'], target_unit: 'oz', target_quantity: 20, category: 'bakery', active: true },
    { id: 'rice', name: 'Rice', canonical_description: 'Long grain white rice 5 lb bag', canonical_query: 'long grain white rice 5 lb bag', required_keywords: ['rice', 'long', 'white'], excluded_keywords: ['brown', 'cauliflower', 'minute'], target_unit: 'lb', target_quantity: 5, category: 'pantry', active: true },
    { id: 'chicken', name: 'Chicken', canonical_description: 'Boneless skinless chicken breast 2 lb', canonical_query: 'boneless skinless chicken breast 2 lb', required_keywords: ['chicken', 'breast', 'boneless', 'skinless'], excluded_keywords: ['whole', 'thigh', 'wing', 'drumstick', 'tender'], target_unit: 'lb', target_quantity: 2, category: 'meat', active: true },
    { id: 'ground_beef', name: 'Ground Beef', canonical_description: 'Ground beef 80 20 1 lb', canonical_query: 'ground beef 80/20 1 lb', required_keywords: ['ground', 'beef'], excluded_keywords: ['patty', 'wagyu'], target_unit: 'lb', target_quantity: 1, category: 'meat', active: true },
    { id: 'potatoes', name: 'Potatoes', canonical_description: 'Russet potatoes 5 lb bag', canonical_query: 'russet potatoes 5 lb bag', required_keywords: ['russet', 'potato'], excluded_keywords: ['yukon', 'gold', 'red', 'sweet'], target_unit: 'lb', target_quantity: 5, category: 'produce', active: true },
    { id: 'yellow_onions', name: 'Yellow Onions', canonical_description: 'Yellow onions 3 lb bag', canonical_query: 'yellow onions 3 lb bag', required_keywords: ['yellow', 'onion'], excluded_keywords: ['red', 'sweet', 'shallot'], target_unit: 'lb', target_quantity: 3, category: 'produce', active: true },
    { id: 'salt', name: 'Salt', canonical_description: 'Iodized table salt 26 oz', canonical_query: 'iodized table salt 26 oz', required_keywords: ['salt', 'iodized'], excluded_keywords: ['kosher', 'sea salt', 'himalayan'], target_unit: 'oz', target_quantity: 26, category: 'pantry', active: true },
    { id: 'gold', name: 'Gold', canonical_description: 'Gold 0.1 gram bar', canonical_query: '0.1 gram gold bar', required_keywords: ['gold'], excluded_keywords: [], target_unit: 'gram', target_quantity: 0.1, category: 'reference', active: true },
    { id: 'silver', name: 'Silver', canonical_description: 'Silver 1 gram bar', canonical_query: '1 gram silver bar', required_keywords: ['silver'], excluded_keywords: [], target_unit: 'gram', target_quantity: 1, category: 'reference', active: true },
    { id: 'mwh', name: '5 kWh', canonical_description: 'Electricity 5 kWh', canonical_query: 'electricity 5 kWh benchmark', required_keywords: ['electricity', 'kwh'], excluded_keywords: ['battery', 'generator', 'charger'], target_unit: 'kwh', target_quantity: 5, category: 'reference', active: true },
    { id: 'cash10', name: '$10', canonical_description: '$10', canonical_query: '$10 usd', required_keywords: [], excluded_keywords: [], target_unit: 'usd', target_quantity: 10, category: 'reference', active: true },
    { id: 'sats10000', name: '10,000 Satoshis', canonical_description: '10,000 satoshis', canonical_query: '10000 satoshis', required_keywords: [], excluded_keywords: [], target_unit: 'sats', target_quantity: 10000, category: 'reference', active: true }
  ];
  return base;
}

function parseItems_(itemList) {
  const configured = {};
  String(itemList || '').split(',').map(s => s.trim()).filter(Boolean).forEach(token => {
    const parts = token.includes('|') ? token.split('|').map(x => x.trim()) : [slug_(token), title_(token), token];
    configured[String(parts[0] || '').trim().toLowerCase()] = { id: String(parts[0] || '').trim().toLowerCase(), name: parts[1] || parts[0], query: parts[2] || parts[0] };
  });
  let items = buildTrackedCatalog_().filter(item => configured[item.id] || isReferenceItemId_(item.id)).map(item => {
    const override = configured[item.id] || {};
    return Object.assign({}, item, { query: item.canonical_query, canonical_query: item.canonical_query, name: override.name || item.name, display_name: override.name || item.name });
  });
  items = normalizeBasketItems_(items);
  ensureCommodityItems_(items);
  validateBasketComposition_(items);
  return items;
}

function normalizeBasketItems_(items) {
  const normalized = [];
  const seenIds = {};
  (items || []).forEach(item => { const id = String(item && item.id || '').trim().toLowerCase(); if (!id || seenIds[id]) return; seenIds[id] = true; normalized.push(item); });
  return normalized.filter(item => item.active !== false);
}

function validateBasketComposition_(items) {
  const groceryCount = (items || []).filter(item => !isReferenceItemId_(item.id)).length;
  const totalCount = (items || []).length;
  if (groceryCount !== 10 || totalCount !== 15) throw new Error('Basket composition must include exactly 10 grocery items and 5 reference items (15 total). Update ITEM_LIST to include the supported grocery item ids.');
}

function ensureCommodityItems_(items) {
  const ids = new Set(items.map(item => String(item.id || '').trim().toLowerCase()));
  ['gold','silver','mwh','cash10','sats10000'].forEach(id => {
    if (ids.has(id)) return;
    const catalogItem = getCatalogItemById_(id);
    if (catalogItem) items.push(catalogItem);
  });
}

function defaultQueryForItem_(itemName) {
  const item = getCatalogItemById_(slug_(itemName));
  return item ? item.canonical_query : itemName;
}

function getSerpQuery_(rawQuery, context) {
  const trimmed = String(rawQuery || '').trim();
  if (!trimmed) throw new Error(`Missing SerpAPI query${context ? ' for ' + context : ''}. Provide a non-empty q value.`);
  return trimmed;
}

function cacheKeyForQuery_(query) { return 'PC_ITEM_' + Utilities.base64EncodeWebSafe(String(query)).slice(0, 50); }
function slug_(s) { return String(s).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, ''); }
function title_(s) { const t = String(s).trim(); return t.charAt(0).toUpperCase() + t.slice(1); }
function normalizeDescription_(s) { return String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, ''); }
function addParam_(url, k, v) { const sep = url.includes('?') ? '&' : '?'; return url + sep + encodeURIComponent(k) + '=' + encodeURIComponent(v); }
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
function average_(arr) { const nums = arr.map(Number).filter(n => isFinite(n)); return nums.length ? nums.reduce((a,b) => a+b, 0) / nums.length : 0; }
function median_(arr) { const nums = arr.map(Number).filter(n => isFinite(n)).sort((a,b) => a-b); if (!nums.length) return 0; const mid = Math.floor(nums.length / 2); return nums.length % 2 ? nums[mid] : (nums[mid-1] + nums[mid]) / 2; }
function usdToSats_(usd, btcUsd) { return (usd / btcUsd) * 100000000; }
function num_(v) { if (v == null) return NaN; const n = Number(String(v).replace(/[^0-9.]/g, '')); return isFinite(n) ? n : NaN; }

function extractRapidApiCandidates_(data, maxInspect) {
  const arr = (Array.isArray(data) && data) || (Array.isArray(data.products) && data.products) || (Array.isArray(data.results) && data.results) || (Array.isArray(data.items) && data.items) || [];
  return arr.slice(0, Math.max(1, maxInspect || 5)).map(item => ({
    raw_vendor_title: String(item.title || item.name || item.description || item.product_description || '').trim(),
    raw_price: firstFiniteNumber_([item.price, item.min_price, item.lowest_price, item.sale_price, item.current_price]),
    source_url: extractCandidateUrl_(item),
    vendor: extractCandidateVendor_(item),
    raw: item
  })).filter(candidate => candidate.raw_vendor_title || isFinite(candidate.raw_price));
}

function extractSerpApiCandidates_(data) {
  const results = Array.isArray(data.shopping_results) ? data.shopping_results : (Array.isArray(data.products) ? data.products : (Array.isArray(data.product_results) ? data.product_results : []));
  return results.map(item => ({
    raw_vendor_title: String(item.title || item.snippet || '').trim(),
    raw_price: firstFiniteNumber_([item.extracted_price, item.price]),
    source_url: extractCandidateUrl_(item),
    vendor: extractCandidateVendor_(item),
    raw: item
  })).filter(candidate => candidate.raw_vendor_title || isFinite(candidate.raw_price));
}

function extractCandidateUrl_(item) { return String(item.product_url || item.productUrl || item.url || item.product_link || item.productLink || item.link || '').trim(); }
function extractCandidateVendor_(item) { return String(item.source || item.seller || item.merchant || item.store || item.vendor || '').trim() || vendorFromUrl_(extractCandidateUrl_(item)); }
function firstFiniteNumber_(values) { for (let i = 0; i < values.length; i++) { const n = num_(values[i]); if (isFinite(n) && n > 0) return n; } return NaN; }

function evaluateProviderCandidates_(item, candidates, source) {
  const evaluated = (candidates || []).map(candidate => validateCandidate_(item, candidate, source));
  const valid = evaluated.filter(candidate => candidate.pass && isFinite(candidate.normalized_price));
  let selected = null;
  if (valid.length) {
    valid.sort((a,b) => b.match_score - a.match_score || a.normalized_price - b.normalized_price || a.raw_price - b.raw_price);
    const topScore = valid[0].match_score;
    const topCandidates = valid.filter(candidate => candidate.match_score === topScore);
    topCandidates.sort((a,b) => a.normalized_price - b.normalized_price);
    const chosen = topCandidates[Math.floor(topCandidates.length / 2)];
    selected = Object.assign({}, chosen, { usd: computeAcceptedUsd_(item, chosen), validation_status: 'validated', source });
  }
  return { source, offers: evaluated, validation_status: selected ? 'validated' : 'rejected', selected, error: selected ? '' : ((evaluated[0] && evaluated[0].fail_reason) || 'no_valid_candidates') };
}

function validateCandidate_(item, candidate, source) {
  const title = String(candidate.raw_vendor_title || '').trim().toLowerCase();
  const parsed = parseSizeFromTitle_(candidate.raw_vendor_title || '');
  const requiredMissing = (item.required_keywords || []).filter(keyword => title.indexOf(String(keyword).toLowerCase()) === -1);
  const excludedHit = (item.excluded_keywords || []).find(keyword => title.indexOf(String(keyword).toLowerCase()) !== -1);
  const typeOk = validateProductTypeCompatibility_(item, title);
  const unitOk = validateUnitCompatibility_(item, parsed);
  const quantityCheck = compareQuantityToTarget_(item, parsed);
  const normalizedPrice = computeNormalizedPrice_(item, candidate.raw_price, parsed);
  const failReasons = [];
  if (!isFinite(candidate.raw_price) || candidate.raw_price <= 0) failReasons.push('missing_price');
  if (requiredMissing.length) failReasons.push('missing_keywords:' + requiredMissing.join('|'));
  if (excludedHit) failReasons.push('excluded_keyword:' + excludedHit);
  if (!typeOk.ok) failReasons.push(typeOk.reason);
  if (!unitOk.ok) failReasons.push(unitOk.reason);
  if (!quantityCheck.ok) failReasons.push(quantityCheck.reason);
  if (!isFinite(normalizedPrice) || normalizedPrice <= 0) failReasons.push('cannot_normalize_price');
  const score = computeMatchScore_(item, title, parsed, failReasons);
  return {
    item_id: item.id,
    vendor: candidate.vendor || '',
    raw_vendor_title: candidate.raw_vendor_title || '',
    raw_price: candidate.raw_price,
    parsed_quantity: parsed.quantity,
    parsed_unit: parsed.unit,
    normalized_price: normalizedPrice,
    normalized_unit: item.target_unit,
    pass: failReasons.length === 0,
    fail_reason: failReasons.join(';'),
    match_score: score,
    source_url: candidate.source_url || '',
    price_source: source,
    parsed
  };
}

function parseSizeFromTitle_(title) {
  const text = String(title || '').toLowerCase();
  const quantityPatterns = [
    { re: /(\d+(?:\.\d+)?)\s*(lb|lbs|pound|pounds)\b/, unit: 'lb' },
    { re: /(\d+(?:\.\d+)?)\s*(oz|ounce|ounces)\b/, unit: 'oz' },
    { re: /(\d+(?:\.\d+)?)\s*(gallon|gallons|gal)\b/, unit: 'gallon' },
    { re: /(\d+(?:\.\d+)?)\s*(gram|grams|g)\b/, unit: 'gram' },
    { re: /(\d+(?:\.\d+)?)\s*(count|ct)\b/, unit: 'count' },
    { re: /(\d+(?:\.\d+)?)\s*(kwh)\b/, unit: 'kwh' }
  ];
  for (let i = 0; i < quantityPatterns.length; i++) {
    const m = text.match(quantityPatterns[i].re);
    if (m) return { quantity: Number(m[1]), unit: quantityPatterns[i].unit };
  }
  if (/dozen/.test(text)) return { quantity: 12, unit: 'count' };
  return { quantity: NaN, unit: '' };
}

function validateUnitCompatibility_(item, parsed) {
  if (!item.target_unit) return { ok: true };
  if (!parsed || !parsed.unit) return { ok: item.target_unit === 'item', reason: 'missing_size' };
  const normalizedTarget = normalizeUnit_(item.target_unit);
  const normalizedParsed = normalizeUnit_(parsed.unit);
  return normalizedTarget === normalizedParsed ? { ok: true } : { ok: false, reason: 'unit_mismatch' };
}

function compareQuantityToTarget_(item, parsed) {
  if (!isFinite(item.target_quantity) || !parsed || !isFinite(parsed.quantity)) return { ok: true };
  const tolerance = item.id === 'bread' ? 0.15 : 0.25;
  const delta = Math.abs(parsed.quantity - item.target_quantity) / item.target_quantity;
  return delta <= tolerance ? { ok: true } : { ok: false, reason: 'quantity_mismatch' };
}

function validateProductTypeCompatibility_(item, title) {
  if (item.id === 'chicken' && !/breast/.test(title)) return { ok: false, reason: 'wrong_cut' };
  if (item.id === 'butter' && /salted/.test(title)) return { ok: false, reason: 'salted_butter' };
  if (item.id === 'potatoes' && /(yukon|gold|red|sweet)/.test(title)) return { ok: false, reason: 'wrong_potato_type' };
  if (item.id === 'mwh' && /(battery|generator|charger)/.test(title)) return { ok: false, reason: 'not_electricity_benchmark' };
  return { ok: true };
}

function computeNormalizedPrice_(item, rawPrice, parsed) {
  const price = Number(rawPrice);
  if (!isFinite(price) || price <= 0) return NaN;
  if (!parsed || !isFinite(parsed.quantity) || !parsed.unit) return price;
  const parsedUnit = normalizeUnit_(parsed.unit);
  const targetUnit = normalizeUnit_(item.target_unit);
  if (parsedUnit !== targetUnit || !item.target_quantity) return price;
  return price / parsed.quantity;
}

function computeAcceptedUsd_(item, candidate) {
  if (!isFinite(candidate.normalized_price) || !isFinite(item.target_quantity)) return Number(candidate.raw_price);
  return candidate.normalized_price * item.target_quantity;
}

function computeMatchScore_(item, title, parsed, failReasons) {
  let score = 0;
  (item.required_keywords || []).forEach(keyword => { if (title.indexOf(String(keyword).toLowerCase()) !== -1) score += 10; });
  if (parsed && parsed.unit && normalizeUnit_(parsed.unit) === normalizeUnit_(item.target_unit)) score += 15;
  if (parsed && isFinite(parsed.quantity) && isFinite(item.target_quantity) && item.target_quantity > 0) {
    const delta = Math.abs(parsed.quantity - item.target_quantity) / item.target_quantity;
    score += Math.max(0, 15 - Math.round(delta * 100));
  }
  score -= (failReasons || []).length * 20;
  return score;
}

function normalizeUnit_(unit) {
  const raw = String(unit || '').toLowerCase();
  if (/^lb|pound/.test(raw)) return 'lb';
  if (/^oz|ounce/.test(raw)) return 'oz';
  if (/^gram|^g$/.test(raw)) return 'gram';
  if (/gallon|gal/.test(raw)) return 'gallon';
  if (/count|ct/.test(raw)) return 'count';
  if (/kwh/.test(raw)) return 'kwh';
  return raw;
}

function selectBestFetchResult_(item, results) {
  const valid = (results || []).filter(Boolean).filter(result => result.selected && result.validation_status === 'validated');
  if (!valid.length) return (results || []).filter(Boolean)[0] || null;
  valid.sort((a,b) => b.selected.match_score - a.selected.match_score || a.selected.usd - b.selected.usd);
  return valid[0];
}

function buildCompactSnapshotForCache_(snapshot) {
  // CacheService payloads are size-limited, so keep the reusable snapshot lean and
  // intentionally strip raw offers, provider candidate arrays, and any debug/audit-only data.
  const items = Array.isArray(snapshot && snapshot.items)
    ? snapshot.items.map(item => ({
        id: item.id,
        name: item.name,
        query: item.query,
        canonical_query: item.canonical_query,
        item_description: item.item_description,
        raw_vendor_title: item.raw_vendor_title || '',
        source_item_description: item.source_item_description || '',
        ts: item.ts,
        usd: item.usd,
        sats: item.sats,
        tracked_quantity: item.tracked_quantity,
        tracked_unit: item.tracked_unit,
        source_url: item.source_url || '',
        price_source: item.price_source || '',
        price_vendor: item.price_vendor || '',
        is_stale: Boolean(item.is_stale),
        validation_status: item.validation_status || '',
        match_score: item.match_score || '',
        normalized_price: item.normalized_price || '',
        normalized_unit: item.normalized_unit || '',
        fail_reason: item.fail_reason || '',
        first_available: item.first_available || null,
        vendor_count: item.vendor_count || 0
      }))
    : [];
  return {
    ts: snapshot && snapshot.ts ? snapshot.ts : null,
    btcUsd: snapshot && isFinite(snapshot.btcUsd) ? snapshot.btcUsd : null,
    basketIndexUsd: snapshot && isFinite(snapshot.basketIndexUsd) ? snapshot.basketIndexUsd : 0,
    basketIndexSats: snapshot && isFinite(snapshot.basketIndexSats) ? snapshot.basketIndexSats : 0,
    items: items,
    rawOffers: []
  };
}

function safeCachePutJson_(cache, key, obj, ttlSeconds) {
  if (!cache || !key) return false;
  try {
    const payload = JSON.stringify(obj);
    const maxBytes = 90000;
    if (payload.length > maxBytes) {
      Logger.log('WARN safeCachePutJson_: skipping cache key %s (%s bytes exceeds %s byte guard)', key, payload.length, maxBytes);
      return false;
    }
    cache.put(key, payload, ttlSeconds);
    return true;
  } catch (err) {
    Logger.log('WARN safeCachePutJson_: unable to cache key %s: %s', key, err && err.message ? err.message : err);
    return false;
  }
}

function collectRawOffers_(bucket, snapshotTs, item, result) {
  if (!result || !Array.isArray(result.offers)) return;
  result.offers.forEach(offer => bucket.push(Object.assign({ timestamp: snapshotTs, item_id: item.id }, offer)));
}

function buildFixedSnapshotItem_(item, fixed, ts) {
  return { id: item.id, name: item.name, query: item.query, canonical_query: item.canonical_query, item_description: item.canonical_description, raw_vendor_title: '', source_item_description: '', ts, usd: fixed ? fixed.usd : 0, sats: fixed ? fixed.sats : 0, tracked_quantity: item.target_quantity, tracked_unit: item.target_unit, source_url: '', price_source: 'fixed', price_vendor: '', is_stale: false, validation_status: 'validated', match_score: 1, normalized_price: fixed ? fixed.usd : 0, normalized_unit: item.target_unit, fail_reason: '' };
}

function buildValidatedSnapshotItem_(item, chosenResult, btcUsd, ts) {
  const selected = chosenResult.selected;
  const usd = selected.usd;
  return { id: item.id, name: item.name, query: item.query, canonical_query: item.canonical_query, item_description: item.canonical_description, raw_vendor_title: selected.raw_vendor_title || '', source_item_description: selected.raw_vendor_title || '', ts, usd, sats: usdToSats_(usd, btcUsd), tracked_quantity: item.target_quantity, tracked_unit: item.target_unit, source_url: selected.source_url || '', price_source: chosenResult.source, price_vendor: selected.vendor || '', is_stale: false, validation_status: 'validated', match_score: selected.match_score || '', normalized_price: selected.normalized_price || '', normalized_unit: selected.normalized_unit || item.target_unit, fail_reason: '' };
}

function buildFallbackSnapshotItem_(item, last, btcUsd, ts) {
  return { id: item.id, name: item.name, query: item.query, canonical_query: item.canonical_query, item_description: item.canonical_description, raw_vendor_title: last.raw_vendor_title || '', source_item_description: last.raw_vendor_title || '', ts, usd: last.usd, sats: usdToSats_(last.usd, btcUsd), tracked_quantity: item.target_quantity, tracked_unit: item.target_unit, source_url: last.source_url || '', price_source: 'last_known_validated', price_vendor: last.price_vendor || '', is_stale: true, validation_status: 'validated', match_score: last.match_score || '', normalized_price: last.normalized_price || '', normalized_unit: last.normalized_unit || item.target_unit, fail_reason: '' };
}

function buildErrorSnapshotItem_(item, chosen, ts) {
  return { id: item.id, name: item.name, query: item.query, canonical_query: item.canonical_query, item_description: item.canonical_description, raw_vendor_title: '', source_item_description: '', ts, usd: 0, sats: 0, tracked_quantity: item.target_quantity, tracked_unit: item.target_unit, source_url: '', price_source: chosen && chosen.error ? 'error:' + chosen.error : 'error', price_vendor: '', is_stale: true, validation_status: 'rejected', match_score: '', normalized_price: '', normalized_unit: item.target_unit, fail_reason: chosen && chosen.error ? chosen.error : 'no_valid_candidates' };
}

function buildEmptySheetSnapshotItem_(it, fallbackBtcUsd) {
  if (isFixedBasketItemId_(it.id)) {
    const fixed = getFixedBasketValue_(it.id, fallbackBtcUsd);
    return buildFixedSnapshotItem_(it, fixed, null);
  }
  return { id: it.id, name: it.name, query: it.query, canonical_query: it.canonical_query, item_description: it.canonical_description, raw_vendor_title: '', source_item_description: '', usd: 0, sats: 0, tracked_quantity: it.target_quantity, tracked_unit: it.target_unit, source_url: '', price_source: '', price_vendor: '', is_stale: true, first_available: null, vendor_count: 0, validation_status: 'rejected', match_score: '', normalized_price: '', normalized_unit: it.target_unit, fail_reason: '' };
}

function buildSheetRowDetails_(row, idx, desc) {
  return { ts: parseSheetDate_(row[idx.timestamp]), item_id: idx.item_id != null ? String(row[idx.item_id] || '').trim() : '', item_name: idx.item_name != null ? String(row[idx.item_name] || '').trim() : '', item_description: desc, raw_vendor_title: idx.raw_vendor_title != null ? String(row[idx.raw_vendor_title] || '') : (idx.source_item_description != null ? String(row[idx.source_item_description] || '') : ''), source_item_description: idx.raw_vendor_title != null ? String(row[idx.raw_vendor_title] || '') : (idx.source_item_description != null ? String(row[idx.source_item_description] || '') : ''), usd: convertElectricityUsdToKwh_(row[idx.usd], idx.item_id != null ? row[idx.item_id] : '', idx.item_name != null ? row[idx.item_name] : '', desc), sats: Number(row[idx.sats]), price_source: idx.price_source != null ? String(row[idx.price_source] || '') : '', price_vendor: idx.price_vendor != null ? String(row[idx.price_vendor] || '') : '', source_url: idx.source_url != null ? String(row[idx.source_url] || '') : '', is_stale: idx.is_stale != null ? Boolean(row[idx.is_stale]) : false, validation_status: idx.validation_status != null ? String(row[idx.validation_status] || '') : 'validated', match_score: idx.match_score != null ? Number(row[idx.match_score]) : NaN, normalized_price: idx.normalized_price != null ? Number(row[idx.normalized_price]) : NaN, normalized_unit: idx.normalized_unit != null ? String(row[idx.normalized_unit] || '') : '' };
}

function buildSnapshotItemFromHistory_(item, latestRow, latestTs) {
  return { id: item.id, name: item.name, query: item.query, canonical_query: item.canonical_query, item_description: item.canonical_description, raw_vendor_title: latestRow ? latestRow.raw_vendor_title : '', source_item_description: latestRow ? latestRow.raw_vendor_title : '', ts: latestRow ? new Date(latestRow.ts).toISOString() : (latestTs ? latestTs.toISOString() : null), usd: latestRow ? latestRow.usd : 0, sats: latestRow ? latestRow.sats : 0, tracked_quantity: item.target_quantity, tracked_unit: item.target_unit, source_url: latestRow ? latestRow.source_url : '', price_source: latestRow ? latestRow.price_source : '', price_vendor: latestRow ? latestRow.price_vendor : '', is_stale: latestRow ? latestRow.is_stale : true, validation_status: latestRow ? latestRow.validation_status : 'rejected', match_score: latestRow ? latestRow.match_score : '', normalized_price: latestRow ? latestRow.normalized_price : '', normalized_unit: latestRow ? latestRow.normalized_unit : item.target_unit, fail_reason: '' };
}

function buildHistoryPointFromRow_(row, idx, rowDescription) {
  return { ts: new Date(row[idx.timestamp]).toISOString(), source_item_description: idx.raw_vendor_title != null ? String(row[idx.raw_vendor_title] || '') : (idx.source_item_description != null ? String(row[idx.source_item_description] || '') : ''), raw_vendor_title: idx.raw_vendor_title != null ? String(row[idx.raw_vendor_title] || '') : (idx.source_item_description != null ? String(row[idx.source_item_description] || '') : ''), usd: convertElectricityUsdToKwh_(row[idx.usd], idx.item_id != null ? row[idx.item_id] : '', idx.item_name != null ? row[idx.item_name] : '', rowDescription), sats: Number(row[idx.sats]), btcUsd: Number(row[idx.btc_usd]), price_source: idx.price_source != null ? String(row[idx.price_source] || '') : '', price_vendor: idx.price_vendor != null ? String(row[idx.price_vendor] || '') : '', source_url: idx.source_url != null ? String(row[idx.source_url] || '') : '', is_stale: idx.is_stale != null ? Boolean(row[idx.is_stale]) : false, validation_status: idx.validation_status != null ? String(row[idx.validation_status] || '') : 'validated' };
}

function dedupeHistoryByTs_(rows) { const seen = {}; return rows.filter(row => { if (!row.ts || seen[row.ts]) return false; seen[row.ts] = true; return true; }); }
function vendorFromUrl_(url) { if (!url) return ''; const match = String(url).match(/^https?:\/\/([^/]+)/i); if (!match) return ''; let host = match[1].toLowerCase().replace(/^www\./, ''); const parts = host.split('.'); return parts.length <= 2 ? host : parts.slice(-2).join('.'); }
function getStandardItemDescription_(itemName) { const item = getCatalogItemById_(slug_(itemName)) || getCatalogItemById_(String(itemName || '').trim().toLowerCase()); return item ? item.canonical_description : ''; }
function getItemDescription_(itemName) { return getStandardItemDescription_(itemName) || String(itemName || '').trim() || 'Grocery item.'; }
function applyItemDescription_(itemName) { return getItemDescription_(itemName); }
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
    'timestamp', 'btc_usd', 'item_id', 'item_name', 'query', 'canonical_query', 'item_description', 'raw_vendor_title', 'source_item_description', 'usd', 'sats',
    'normalized_price', 'normalized_unit', 'basket_index_usd', 'basket_index_sats', 'price_source', 'price_vendor', 'source_url', 'is_stale', 'match_score', 'validation_status', 'fail_reason'
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
    canonical_query: m.canonical_query !== undefined ? m.canonical_query : null,
    item_description: m.item_description,
    raw_vendor_title: m.raw_vendor_title !== undefined ? m.raw_vendor_title : null,
    source_item_description: m.source_item_description !== undefined ? m.source_item_description : null,
    usd: m.usd,
    sats: m.sats,
    normalized_price: m.normalized_price !== undefined ? m.normalized_price : null,
    normalized_unit: m.normalized_unit !== undefined ? m.normalized_unit : null,
    basket_index_usd: m.basket_index_usd,
    basket_index_sats: m.basket_index_sats,
    price_source: m.price_source !== undefined ? m.price_source : null,
    price_vendor: m.price_vendor !== undefined ? m.price_vendor : null,
    source_url: m.source_url !== undefined ? m.source_url : null,
    is_stale: m.is_stale !== undefined ? m.is_stale : null,
    match_score: m.match_score !== undefined ? m.match_score : null,
    validation_status: m.validation_status !== undefined ? m.validation_status : null,
    fail_reason: m.fail_reason !== undefined ? m.fail_reason : null
  };
}

function getOrCreateRawOffersSheet_(ss, name) {
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  const desiredHeader = ['timestamp', 'item_id', 'vendor', 'raw_vendor_title', 'raw_price', 'parsed_quantity', 'parsed_unit', 'normalized_price', 'normalized_unit', 'pass_fail', 'fail_reason', 'match_score', 'source_url', 'price_source'];
  if (sh.getLastRow() === 0) {
    sh.appendRow(desiredHeader);
    sh.setFrozenRows(1);
    return sh;
  }
  const lastCol = Math.max(sh.getLastColumn(), desiredHeader.length);
  const header = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(String);
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
    rawOffersSheetName: sp.getProperty('RAW_OFFERS_SHEET_NAME') || 'RawOffers',
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
