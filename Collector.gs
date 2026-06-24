function recordSnapshot() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) throw new Error('Another snapshot is already running.');
  try {
    const props = getProps_();
    validateCollectionProps_(props);
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const historySheet = ensureSheet_(ss, props.dataSheetName, HISTORY_HEADER);
    const rawSheet = ensureSheet_(ss, props.rawOffersSheetName, RAW_HEADER);
    const active = getActiveCatalog_(props);
    const btcUsd = fetchBtcUsd_(props);
    const ts = new Date();
    const prior = lastValidatedRows_(historySheet);
    const rawOffers = [];
    const results = [];

    const shoppingItems = active.filter(function(item){ return item.id !== 'mwh' && item.id !== 'cash10' && item.id !== 'sats10000'; });
    const providerResults = fetchShoppingCandidates_(shoppingItems, props);

    active.forEach(function(item) {
      if (item.id === 'cash10' || item.id === 'sats10000') return;
      let result;
      if (item.id === 'mwh') {
        try { result = electricityResult_(item, fetchElectricityBenchmark_(), btcUsd, ts); }
        catch (e) { result = null; }
      } else {
        const candidates = providerResults[item.id] || [];
        result = aggregateCandidates_(item, candidates, btcUsd, ts, rawOffers);
      }
      if (!result || !result.valid) {
        const old = prior[item.id];
        result = old && props.allowStaleFallback ? carriedResult_(item, old.usd, btcUsd, ts, old) : rejectedResult_(item, btcUsd, ts, result ? result.failReason : 'no_valid_candidates');
      }
      results.push(result);
    });

    const coreResults = results.filter(function(row){ return isCoreId_(row.itemId); });
    const basketUsd = coreResults.reduce(function(sum,row){ return row.valid && finitePositive_(row.usd) ? sum + row.usd : sum; },0);
    const basketSats = finitePositive_(btcUsd) ? usdToSats_(basketUsd, btcUsd) : 0;
    const rows = results.map(function(row){ return historyValues_(ts, btcUsd, row, basketUsd, basketSats); });
    if (rows.length) historySheet.getRange(historySheet.getLastRow()+1,1,rows.length,HISTORY_HEADER.length).setValues(rows);
    if (rawOffers.length) rawSheet.getRange(rawSheet.getLastRow()+1,1,rawOffers.length,RAW_HEADER.length).setValues(rawOffers.map(function(row){ return rawValues_(ts,row); }));
    CacheService.getScriptCache().remove('PP_DASHBOARD_V2');
    return {ok:true,recordedAt:ts.toISOString(),rowsAppended:rows.length,rawOffersAppended:rawOffers.length};
  } finally {
    lock.releaseLock();
  }
}
