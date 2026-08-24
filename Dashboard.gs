function getPurchasingPowerDashboardData() {
  const props = getProps_();
  const active = getActiveCatalog_(props);
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(props.dataSheetName);
  if (!sheet || sheet.getLastRow() < 2) return emptyDashboard_(active,props);

  const values = sheet.getDataRange().getValues();
  const header = indexHeader_(values[0]);
  const grouped = {};
  for (let r = 1; r < values.length; r++) {
    const row = historyRow_(values[r], header);
    if (!row.ts || !row.itemId) continue;
    if (!grouped[row.ts]) grouped[row.ts] = {ts:row.ts,btcUsd:row.btcUsd,rows:{}};
    grouped[row.ts].btcUsd = finitePositive_(row.btcUsd) ? row.btcUsd : grouped[row.ts].btcUsd;
    grouped[row.ts].rows[row.itemId] = row;
  }

  const activeById = {};
  active.forEach(function(item){ activeById[item.id] = item; });
  const core = active.filter(isCoreItem_);
  const references = active.filter(function(item){ return !isCoreItem_(item); });
  const lastValidUsd = {};
  const snapshots = Object.keys(grouped).sort().map(function(ts) {
    const group = grouped[ts];
    const btcUsd = Number(group.btcUsd);
    const items = [];
    core.forEach(function(item) {
      const current = group.rows[item.id];
      let normalized = normalizeHistoricalItem_(current, item, btcUsd);
      if (normalized && normalized.valid && !normalized.isStale) lastValidUsd[item.id] = normalized.usd;
      if ((!normalized || !normalized.valid) && finitePositive_(lastValidUsd[item.id]) && finitePositive_(btcUsd)) {
        normalized = carriedItem_(item, lastValidUsd[item.id], btcUsd, ts, current);
      }
      if (normalized) items.push(normalized);
    });
    return buildSnapshot_(ts, btcUsd, items, core.length);
  });

  const itemSeries = core.map(function(item) {
    return {
      id:item.id,name:item.name,description:item.description,category:item.category,
      history:snapshots.map(function(snap) {
        const found = snap.items.filter(function(row){ return row.itemId === item.id; })[0];
        return found ? {ts:snap.ts,usd:found.usd,sats:found.sats,isStale:found.isStale,vendor:found.vendor,source:found.source,sourceUrl:found.sourceUrl} : null;
      }).filter(Boolean)
    };
  });

  const referenceSeries = buildReferenceSeries_(Object.keys(grouped).sort(), grouped, references);
  const latest = snapshots.length ? snapshots[snapshots.length - 1] : null;
  const reliability = buildReliability_(snapshots,itemSeries,core.length,props.serpApiMaxSearchesPerDay,new Date(),props.reliabilityStartDate);
  return {
    version:PP_VERSION,
    generatedAt:new Date().toISOString(),
    location:props.serpApiLocation || 'United States',
    basketDefinition:{method:'fixed_quantity_total',itemCount:core.length,items:core.map(publicItem_),referencesExcluded:REFERENCE_IDS},
    snapshots:snapshots,
    itemSeries:itemSeries,
    references:referenceSeries,
    quality:latest ? latest.quality : emptyQuality_(core.length),
    reliability:reliability
  };
}
