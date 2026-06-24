from pathlib import Path
code=Path('Code.gs').read_text()
page=Path('Index.html').read_text()
for term in ['APU000072610','fetchElectricityBenchmark_','filterOutlierCandidateOffers_','marketplace_not_allowed','implausible_unit_price','confidenceGradeForSnapshot_','fixed_quantity_total']:
 assert term in code,term
for term in ['Bitcoin Purchasing Power Dashboard','Include carried-forward values','Fresh observations only','Fixed basket total in dollars over time','Fixed basket total in sats over time','sats10000','usdSeries.push(weightedUsdTotal);','satsSeries.push(weightedSatsTotal);']:
 assert term in page,term
assert "excluded_keywords: ['salted', 'spread', 'margarine']" not in code
assert 'Basket cost is computed as the weighted average cost' not in page
print('Purchasing-power v2 contract checks passed.')
