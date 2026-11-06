# September 2026 grocery reliability audit

Audit scope: repository main `090b6cd1750c559c9ec00c7665dc233e98699ebb`, connected **Bitcoin Dashboard** Sheet, September 1–29, 2026. Currentness evaluated at September 29, 12:33 America/Chicago. No live observations, configuration or deployment were changed by this PR.

## Verified findings and evidence limits

- The history contains 377 September rows: 29 daily snapshots × 13 recorded series. Ten are groceries: apples, bananas, eggs, milk, butter, bread, rice, chicken, ground beef and potatoes. Gold, silver and electricity are references. Catalog onions and salt have no September observations; both remain inactive by default.
- Repository main defaults to six total SerpApi searches/day and a 220-search monthly budget. All twelve shopping items share that rotation. Normal odd September days query rice/chicken/beef/potatoes/gold/silver; even days query the other six groceries. Thus half the days permit only four grocery successes against an assumed six-opportunity denominator. Full 30-day maximum is 150/180 = 83.33% even with perfect validation, below 95%.
- There are **95 fresh validated grocery observations**. The old assumed denominator through September 29 is 29 × 6 = 174: **54.5977%**. Default start September 1 means the literal grade is still **Building** on September 29; 54.6% is the D score band once mature, not an earned September 29 D under that default.
- There are **110 distinct grocery item/date groups with raw offers**, plus 26 metal groups. They do not prove the total number of requests. Missing groups concentrate September 11–22, and September 14 has no raw offers at all despite having snapshot rows. Empty responses, transport/HTTP failures and locally skipped calls were not logged distinctly.
- Reconstructing the main-branch daily rotation gives **144 grocery opportunities** (14 × 6 + 15 × 4). That yields **95/144 = 65.9722%**. This is a labelled schedule reconstruction, not a recovered authoritative ledger. Counting only 110 nonempty groups gives 86.36%, an optimistic conditional rate that would conceal possible request failures and must not become the grade. With the six-per-day assumption, grocery denominator uncertainty is 110–144; either bound is below 95%.
- September raw groceries contain **4,398 offers: 683 passing and 3,715 failing**. Exactly 2,330 failures (62.72% of failures) are `missing_size;cannot_normalize_price`; 3,103 failures (83.53%) contain `missing_size` along with any other reasons. Bread's literal `sandwich`, rice's explicit `long` + `white`, and Honeycrisp spelling/package requirements are confirmed in code and raw failures.
- Read ranges: `GroceryPriceHistory!A2452:AS2828`; `RawOffers!A49700:O52000` and `A52001:O55088`, filtering the latter reads to September. Headers were separately verified. Unformatted late-September timestamps establish actual 48-hour ages. The old raw table has no saved extensions/snippets, so metadata-based improvement cannot be replayed.
- Access covered repository main and live Sheet observations, **not deployed Apps Script source/version or private Script Properties/provider billing logs**. The observed full-day pattern agrees with main, but deployment identity, hidden overrides, external quota usage and causes of missing requests remain unverified. No API keys or Sheet IDs are committed.

## Before / after accounting

| Concern | Before | Proposed v3 |
|---|---|---|
| Denominator | Calendar days × assumed min(groceries, six) | Durable unique scheduled core item/day entries |
| Numerator | Highest snapshot fresh count that day, capped at assumed daily budget | Successfully completed fresh validations for those entries |
| Empty/error/crashed request | Cannot distinguish from skipped rotation in saved history | Scheduled row remains failed or pending; counts as unsuccessful |
| Reference assets | Spend the same six slots without reducing assumed denominator | Independent weekly allocation; excluded from grocery numerator and denominator |
| Downtime / small schedule | Assumed denominator penalizes it, but for the wrong reason | Explicit schedule coverage gate: all 30 days and ≥15 opportunities per grocery |
| Maturity | Calendar age since configurable September 1 | Calendar age since first durable v3 schedule for current basket; date disclosed |
| Historical observations | Existing price history | Unchanged; legacy estimate separately returned |

Current coverage is calculated independently using fresh timestamps and valid latest basket values. All items must be ≤48 hours old, with zero missing, before A. The 95% threshold, canonical basket quantities, vendor exclusions, bounds, aggregation and index formulas are preserved.

## Per-item September performance

“Scheduled” is reconstructed from the old alternating rotation. “Nonempty” is the number of item/day groups actually found in RawOffers. “Replay” reruns revised title/size rules on saved offers only; it does not invent structured metadata or results for unobserved queries.

| Grocery | Scheduled | Nonempty | Actual successes | Actual / scheduled | Replay successes | Replay / scheduled | Last actual fresh | Last replay fresh |
|---|---:|---:|---:|---:|---:|---:|---|---|
| Apples | 14 | 11 | 8 | 57.14% | 8 | 57.14% | Sep 28 | Sep 28 |
| Bananas | 14 | 12 | 12 | 85.71% | 12 | 85.71% | Sep 28 | Sep 28 |
| Eggs | 14 | 10 | 10 | 71.43% | 10 | 71.43% | Sep 28 | Sep 28 |
| Milk | 14 | 12 | 11 | 78.57% | 11 | 78.57% | Sep 28 | Sep 28 |
| Butter | 14 | 12 | 8 | 57.14% | 12 | 85.71% | Sep 28 | Sep 28 |
| Bread | 14 | 8 | 6 | 42.86% | 8 | 57.14% | Sep 26 | Sep 28 |
| Rice | 15 | 10 | 8 | 53.33% | 9 | 60.00% | Sep 25 | Sep 27 |
| Chicken | 15 | 9 | 9 | 60.00% | 9 | 60.00% | Sep 29 | Sep 29 |
| Ground beef | 15 | 14 | 14 | 93.33% | 14 | 93.33% | Sep 29 | Sep 29 |
| Potatoes | 15 | 12 | 9 | 60.00% | 9 | 60.00% | Sep 29 | Sep 29 |
| **Total** | **144** | **110** | **95** | **65.97%** | **102** | **70.83%** | | |

Gold and silver each have 13 observed nonempty query days and 13 successful refreshes; neither belongs in this grocery table's totals.

## Evidence-backed validation changes

- **Butter:** safely converts `1 lb` and equivalent `16 oz (1 lb) 453 g` labels into ounces. The old parser preferred pounds and rejected otherwise correct butter. Four additional query days succeed. Explicit `4 x 4 oz` totals 16 oz; `16 oz, 10 per case` totals 160 oz and is rejected.
- **Bread:** literal `sandwich` is unnecessary in clear products such as `Kroger Classic White Bread, 20 oz` or `Bimbo White Bread 20 oz Soft Pre-Sliced White Loaf`. Require a whole-word bread identity plus loaf/sliced/sandwich/white/wheat/whole-grain form; retain or expand exclusions for rolls, buns, bagels, crumbs, flatbread and unsuitable sweet/prepared products. Two extra query days succeed. Multi-loaf packages are evaluated at their total mass and rejected when outside the target tolerance.
- **Rice:** `Uncle Ben's Original Converted Long Grain White Rice 80 oz Bag` now safely normalizes to five pounds; one additional query day succeeds. Keep explicit long-grain and white requirements. `Calrose`, unlabeled long-grain rice and products described only as enriched do not prove the same standardized product; dropping both words to increase success would weaken comparability.
- **Apples:** normalize `Honey Crisp`/`Honey-Crisp`, accepting `Michigan Fresh Apples, Honey Crisp, 48 oz (3 lb) 1.36 kg`. No additional successful September days, though the accepted vendor pool improves. Count-only, unknown-weight packages and other varieties remain rejected.
- **Potatoes and other items:** metric/ounce equivalents help explicit-size offers (e.g. `Potato Russet Bag Organic, 80 Ounce`). No extra potato query days are recovered. Russet identity remains required. Milk separates fluid ounces from mass ounces. Explicit multipacks, conflicting sizes, ambiguous ranges and mixed variants are handled conservatively, fixing some old false acceptances too.
- **Structured evidence:** SerpApi's [documented shopping fields](https://serpapi.com/shopping-results) include `extensions` and `snippet`, which the old collector discarded. The revised collector saves and uses only explicit same-offer package-size evidence. Unknown weight is never filled from the search query, canonical target, URL, serving size or shipping weight. Conflicting title/metadata sizes reject the offer. No extra billable product-detail request is added.

The replay changes which offers can participate in new aggregates; old stored prices and observations are never rewritten. Improved matching can change future vendor medians, so source and validation version remain inspectable.

## Revised September projection and exact gates

- **Literal Sep 29 grade: Building**, because only 29 September days have elapsed under the historical start. The new production method would also be Building until it has its own 30-day ledger.
- **Corrected mature score band, accounting only: C**, at 95/144 = 65.97%; actual current coverage is 8/10 (bread and rice older than 48 hours), zero missing.
- **Replayed mature score band: C**, at **102/144 = 70.8333%**, with 9/10 current and zero missing. Scheduled success is the lower metric and misses the unchanged 95% gate by **24.1667 percentage points**. At this denominator, ≥137 successes are required; the replay is **35 successes short**. Rice's latest replay success was Sep 27 about 08:34:44, approximately **51 h 58 m old** at audit time, so it independently fails the 48-hour gate.
- Even treating all unobserved requests as unscheduled (the most favorable possible denominator), replay success is **102/110 = 92.7273%**, still below 95%. That conditional calculation is diagnostic only.
- If all six scheduled groceries succeed on Sep 30, the reconstruction would be **108/150 = 72.0%**, still C. This is an upper-bound month-end scenario under the historical rotation, not an observed Sep 30 result. The new six-core rotation's missing counterfactual searches cannot be fabricated from the old data.

## Ten-item sustainability and reduction scenarios

**Ten fits the search budget. ≥95% real-world validation has not yet been demonstrated.** Default allocation uses six core calls/day and two reference calls no more often than weekly: 176 maximum in 28 days, 184 in 29 days, 190 in 30 days, and 196 in 31 days. Monthly ceilings stay 220, leaving 24–44 dashboard searches unused; the separate 30-search provider reserve remains intact if the provider plan is 250. An explicit total daily cap of six produces 180/186 grocery calls in 30/31 days and no metal refreshes. No property was changed live.

The following sensitivity analysis assumes the replayed per-item rates persist, equal long-run item frequencies under the new rotation, and selection of the strongest N items for each column. It is **not a causal prediction that removing an item repairs transport failures**, and the two columns may choose different subsets. Small samples, correlated missing request days and absent structured metadata limit inference.

| Basket size | Expected scheduled success if September request gaps persist | Expected validation if every scheduled query returns comparable nonempty offers |
|---|---:|---:|
| 10 | 70.90% | 92.94% |
| 9 | 72.43% | 95.19% |
| 8 | 74.35% | 97.71% |
| 7 | 76.39% | 98.81% |
| 6 | 79.13% | 100.00% |

Calculation: arithmetic mean of the best N item-level replay successes/scheduled counts in column 2; best N replay successes/nonempty counts in column 3. The optimistic nine-item column excludes apples; eight excludes apples and potatoes. These are diagnostics, not approved composition changes. Even six would not fix the historical operational gaps. Nine's optimistic 95.19% has essentially no headroom for request failures. Eight would offer more headroom **if** later evidence requires a smaller basket, but the current evidence does not yet justify dropping either item after the new structured parser and collector are measured.

**Recommendation: retain ten**, run a complete v3 month, and diagnose request-empty/transport/validation outcomes before considering a separately versioned basket change. An A is mathematically and budget-feasible (tested at exactly 171/180 with ten current items), but no honest repair can promise that Google Shopping will supply sufficient unambiguous offers. The unused budget is headroom, not assumed successful refreshes or hidden retries.

## Tests, deployment and rollback

`node tests/run.js` runs all existing `testMeasurementContract()` tests and 36 new regression groups. They cover the requested A boundaries, actual denominators, reference-only failure isolation, 48-hour boundary, latest missing values, future data, duplicate and sparse schedules, pre-ledger reset, Chicago dates, 28/29/30/31-day budget/rotation simulations, explicit low caps, quota blocks, month resets/cursor persistence, pre-request durable writes, Sheet write failures, normalization, conservative metadata and unchanged basket arithmetic. All pass. Apps Script backend sources and frontend JavaScript compile in Node's VM; manifest JSON parses; `git diff --check` passes. No billable live SerpApi calls were made and no Apps Script deployment was exercised.

Reproduce title-only replay from bounded connector range exports (JSON objects with a `values` array):

```sh
node scripts/replay-september.js HISTORY.json RAW_PART1.json RAW_PART2.json
```

Use the ranges listed above, with formatted history dates and raw columns A:O; filter is September 2026. The replay is read-only and prints per-item outcomes and changed offer examples. Source files stay outside the public repository. A compact outcome fixture in the repository records the audited totals for review.

After PR review/merge, deploy all `.gs` and HTML changes together. Preserve API keys and existing Sheet properties. Set the total daily cap to 8 only if an explicit legacy cap of 6 currently exists and weekly metal collection is desired; retain core 6/monthly 220. Verify the first `RefreshSchedule` rows precede requests, planned outcomes complete, the displayed start date is correct, reference failures do not affect grocery counts, and new raw rows retain size evidence. Check provider account usage separately. The v3 record starts with that first real schedule; do not backfill successful rows.

Rollback: redeploy the prior Apps Script version. Leave the additive schedule tab and appended raw columns intact for audit; the old history columns and basket data contract remain available. Do not delete history to alter a grade. Keep the first v3 record if rolling forward again; outages remain visible.

## Files changed

| File | Purpose |
|---|---|
| `Reliability.gs` (new) | Durable schedule ledger, actual core-only accounting, maturity/coverage/currentness gates |
| `SerpApiBudget.gs` | Independent fair core/reference cursors, weekly surplus allocation, Chicago days, configured budget caps |
| `Collector.gs` | Schedule persistence before requests and outcome completion after history writes |
| `ShoppingSources.gs` | Structured size evidence, independent reference transport batch, conservative validation and failure diagnostics/fallback |
| `PackageParsing.gs` (new) | Unit conversion, multipacks, labelled metadata, conflict and ambiguity checks |
| `Config.gs` | Core/reference/ledger settings; total daily default eight; separate legacy calculation cap |
| `Code.gs` | Version 2.2, appended raw evidence columns, bread rule |
| `Dashboard.gs` | v3 grade plus explicitly separate legacy calculation |
| `Models.gs` | Append validation evidence/version to new raw rows; basket/index math unchanged |
| `RuntimeUtils.gs` | Empty-dashboard v3 reliability contract |
| `App.html`, `Index.html` | Dynamic grocery counts, visible actual numerator/denominator and methodology/coverage wording |
| `tests/run.js` (new) | Run legacy tests and 36 targeted regression groups without paid/provider access |
| `scripts/replay-september.js` (new) | Read-only historical replay utility |
| `docs/september-2026-replay-summary.json` (new) | Compact per-item audit results, without Sheet IDs or offer URLs |
| `README.md`, `MEASUREMENT_SPEC.md`, this audit | Budget/methodology transition, evidence, exact outcomes, limitations and deployment/rollback |

## PR review follow-up — September 29, 2026

The automated review identified that an empty SerpApi plan inadvertently suppressed an otherwise configured RapidAPI provider. This is fixed: the backup continues the shared rotation when the primary reaches its budget or quota block, with a separate daily reservation/cap and no additional SerpApi usage. If both providers run, they contribute to one pre-recorded item/day opportunity. Same-day retries cannot inflate success or create duplicate ledger rows. Reference transport isolation now also applies to RapidAPI.

The original bug was reproduced with a failing regression before the fix. Six additional regression groups cover monthly exhaustion, provider quota blocks, successful same-run backup validation with a single ledger entry, fair rotation and handoff back to SerpApi, backup caps and duplicate prevention, failure to persist a fallback schedule, and isolated reference failure. All existing tests and 36 regression groups now pass. September validation replay, basket math, item count, 95% threshold and SerpApi budget projections are unchanged. This follow-up does not establish that RapidAPI is configured in the deployed app; private properties remain uninspected. No live collection, merge or deployment was performed.
