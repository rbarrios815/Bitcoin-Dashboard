#!/usr/bin/env python3
from pathlib import Path

P = Path('Code.gs')
s = P.read_text()

def lit(old, new, name):
    global s
    n = s.count(old)
    if n != 1: raise RuntimeError(f'{name}: {n} matches')
    s = s.replace(old, new, 1)

helpers = """function confidenceGradeForSnapshot_(freshCount, staleCount, expectedCount) {
  if (!expectedCount) return 'F';
  const score = ((Number(freshCount || 0) + Number(staleCount || 0) * 0.35) / expectedCount) * 100;
  if (score >= 95) return 'A';
  if (score >= 80) return 'B';
  if (score >= 60) return 'C';
  if (score >= 40) return 'D';
  return 'F';
}

function computeSnapshotQuality_(rows, expectedItemIds) {
  const expected = (expectedItemIds || []).map(id => String(id || '').trim().toLowerCase()).filter(Boolean);
  const byId = {};
  (rows || []).forEach(row => {
    const id = String(row && (row.itemId || row.id) || '').trim().toLowerCase();
    if (id && !byId[id]) byId[id] = row;
  });
  let freshCount = 0, staleCount = 0, missingCount = 0, rejectedCount = 0;
  expected.forEach(id => {
    const row = byId[id];
    if (!row || !isFinite(Number(row.usd)) || Number(row.usd) <= 0 || !isFinite(Number(row.sats)) || Number(row.sats) <= 0) {
      missingCount += 1; return;
    }
    const status = String(row.validation_status || '').trim().toLowerCase();
    if (status && status !== 'validated') { rejectedCount += 1; return; }
    if (row.is_stale) staleCount += 1; else freshCount += 1;
  });
  const expectedItemCount = expected.length, observedCount = freshCount + staleCount;
  const coveragePct = expectedItemCount ? observedCount / expectedItemCount * 100 : 0;
  const freshPct = expectedItemCount ? freshCount / expectedItemCount * 100 : 0;
  const confidenceGrade = confidenceGradeForSnapshot_(freshCount, staleCount, expectedItemCount);
  return {
    expectedItemCount, freshCount, staleCount, missingCount, rejectedCount, observedCount, coveragePct, freshPct, confidenceGrade,
    confidenceStatus: confidenceGrade === 'A' || confidenceGrade === 'B' ? 'strong' : (confidenceGrade === 'C' ? 'limited' : 'insufficient'),
    usableForHeadline: observedCount === expectedItemCount && freshPct >= 80
  };
}

"""
lit('function getPurchasingPowerDashboardData() {', helpers + 'function getPurchasingPowerDashboardData() {', 'quality helpers')
lit("const configuredItems = parseItems_(props.itemList).map(item => ({ id: item.id, name: item.name }));", """const configuredItems = parseItems_(props.itemList).map(item => ({ id:item.id, name:item.name, category:item.category || '' }));
  const coreBasketItems = configuredItems.filter(item => basketWeightForItemId_(item.id) > 0);
  const coreBasketItemIds = coreBasketItems.map(item => item.id);""", 'basket metadata')
lit("const group = idx.group != null ? String(row[idx.group] || '').trim() : '';", """const catalogMeta = getCatalogItemById_(itemId);
    const group = idx.group != null && String(row[idx.group] || '').trim()
      ? String(row[idx.group] || '').trim()
      : (catalogMeta && catalogMeta.category ? catalogMeta.category : '');""", 'catalog group')
lit("""    const basket = computeCanonicalBasketForSnapshot_(rows, { btcUsd: btcUsd, injectFixedItems: true });
    const staleCount = rows.filter(r => r.is_stale).length;
    const missingCount = rows.filter(r => !isFinite(r.usd) || !isFinite(r.sats)).length;""", """    const basket = computeCanonicalBasketForSnapshot_(rows, { btcUsd:btcUsd, injectFixedItems:false });
    const snapshotQuality = computeSnapshotQuality_(rows, coreBasketItemIds);""", 'snapshot quality')
lit("""      itemCount: rows.length,
      staleCount,
      missingCount,
      groups: uniqueValues_(rows.map(r => r.group)),""", """      itemCount: snapshotQuality.expectedItemCount,
      coreItemCount: snapshotQuality.observedCount,
      freshCount: snapshotQuality.freshCount,
      staleCount: snapshotQuality.staleCount,
      missingCount: snapshotQuality.missingCount + snapshotQuality.rejectedCount,
      rejectedCount: snapshotQuality.rejectedCount,
      coveragePct: snapshotQuality.coveragePct,
      freshPct: snapshotQuality.freshPct,
      confidenceGrade: snapshotQuality.confidenceGrade,
      confidenceStatus: snapshotQuality.confidenceStatus,
      usableForHeadline: snapshotQuality.usableForHeadline,
      groups: uniqueValues_(rows.map(r => r.group)),""", 'snapshot fields')
lit("""  const latestSnapshot = snapshots.length ? snapshots[snapshots.length - 1] : null;
  const quality = { currentItems: latestSnapshot ? latestSnapshot.itemCount : 0, staleItems: latestSnapshot ? latestSnapshot.staleCount : 0, missingItems: latestSnapshot ? latestSnapshot.missingCount : 0, vendorInconsistencyCount: itemList.filter(item => item.vendorChanged).length, currentSnapshotHasStale: latestSnapshot ? latestSnapshot.staleCount > 0 : false };
  return { generatedAt: new Date().toISOString(), snapshots, items: itemList, quality, fieldsDetected: {""", """  const latestSnapshot = snapshots.length ? snapshots[snapshots.length - 1] : null;
  const quality = {
    currentItems: latestSnapshot ? latestSnapshot.itemCount : 0,
    freshItems: latestSnapshot ? latestSnapshot.freshCount : 0,
    staleItems: latestSnapshot ? latestSnapshot.staleCount : 0,
    missingItems: latestSnapshot ? latestSnapshot.missingCount : 0,
    coveragePct: latestSnapshot ? latestSnapshot.coveragePct : 0,
    confidenceGrade: latestSnapshot ? latestSnapshot.confidenceGrade : 'F',
    confidenceStatus: latestSnapshot ? latestSnapshot.confidenceStatus : 'insufficient',
    usableForHeadline: latestSnapshot ? latestSnapshot.usableForHeadline : false,
    vendorInconsistencyCount: itemList.filter(item => item.vendorChanged).length,
    currentSnapshotHasStale: latestSnapshot ? latestSnapshot.staleCount > 0 : false
  };
  const basketDefinition = {
    method:'fixed_quantity_total', itemCount:coreBasketItems.length, itemIds:coreBasketItemIds, items:coreBasketItems,
    referencesExcluded:['gold','silver','mwh','cash10','sats10000']
  };
  return { generatedAt:new Date().toISOString(), snapshots, items:itemList, quality, basketDefinition, fieldsDetected: {""", 'top quality')

P.write_text(s)
print('quality migrated')
