# Bitcoin Basket-of-Goods Index

## Project identity

This repository is the **basket-of-goods purchasing-power index project**.

It compares the cost of a fixed, standardized shopping basket in:

- U.S. dollars
- satoshis
- an indexed series with a visible baseline of 100

It also reports basket composition, item-level changes, carried-forward values, data confidence, and reference comparisons.

**This repository does not mine Bitcoin.**

## SerpApi query budget (collector 2.2 / reliability methodology v3)

The default basket remains **10 groceries**. Onions and salt remain inactive. Gold and silver remain references, with a separate weekly rotation.

- `SERPAPI_MONTHLY_BUDGET=220` remains the dashboard ceiling.
- `SERPAPI_CORE_SEARCHES_PER_DAY=6` reserves six rotating grocery opportunities daily.
- `SERPAPI_MAX_SEARCHES_PER_DAY=8` is the new total ceiling: six groceries plus up to two references on a weekly reference day. Ordinary days use six searches. An existing explicit property of `6` is honored; it prevents reference refreshes rather than displacing groceries. Set that property to `8` during deployment if weekly metal updates are desired.
- `SERPAPI_REFERENCE_INTERVAL_DAYS=7` limits reference frequency. References can use only surplus after reserving the rest of the month's core allocation.
- Budget reservation and independent persistent cursors prevent repeated same-day requests and unfair rotation when allowance changes. Both cursors survive month boundaries.
- Default totals are at most 190 searches in a 30-day month and 196 in a 31-day month (180/186 grocery searches plus at most ten reference searches). Depending on the prior reference date, usage may be two searches lower.
- Local reservations conservatively include transport errors/crashes, even if the provider did not bill them. A provider quota-exhausted response blocks later calls that month.
- Unselected, failed, or blocked items retain explicitly stale fallback values. No inferred package sizes or fabricated fresh observations are used.

Run `getSerpApiBudgetStatus()` to inspect local reservations. External account usage is not visible to that counter.

## Reliability grade

The collector creates `RefreshSchedule` (override with `REFRESH_LOG_SHEET_NAME`) and flushes one durable row per scheduled item/day **before** issuing provider requests. Pending/crashed, empty, failed, or rejected grocery requests remain denominator entries; only successful fresh validated results count in the numerator. Gold and silver never enter either total. Raw-offer count is not a request ledger.

An A requires all of:

- A full 30-day track record under the new method.
- Every configured grocery current within 48 hours, with zero missing latest values.
- At least **95% of actual scheduled grocery opportunities** successful.
- A grocery schedule on all 30 days and at least 15 opportunities per grocery in that window. This separate coverage gate prevents sparse schedules, budget exhaustion or collector outages from earning A through a small denominator.

With six groceries scheduled each day, the usual denominator is 180 and the minimum success count is 171. It is counted from the ledger, never assumed. Duplicate item/day entries and unscheduled observations cannot inflate success.

**Explicit methodology boundary:** v3 tracking begins on the first persisted grocery schedule for the current basket, not September 1 and not an arbitrary hard-coded deployment date. The dashboard discloses this date and shows `Building` until day 30. `RELIABILITY_START_DATE` continues to control the separately returned legacy estimate only. Historical prices, raw offers and index arithmetic are not rewritten. Legacy September data cannot retrospectively prove which zero-result requests were scheduled; see [September audit](docs/SEPTEMBER_2026_RELIABILITY_AUDIT.md).

## Validation and verification

Package parsing converts equivalent mass/volume units, handles explicit multipacks, and rejects conflicting sizes, ambiguous ranges, bundles and unknown quantities. SerpApi `extensions` and explicitly labelled package-size `snippet` content can supplement the title; search queries/URLs, serving sizes and arbitrary snippets cannot. Evidence and parser version are appended to new raw-offer rows.

Bread no longer requires the literal word `sandwich`, but must still describe loaf/sliced/white/wheat bread and exclude buns, rolls, bagels and other unsuitable products. `Honey Crisp` and `Honey-Crisp` normalize to Honeycrisp. Rice retains explicit long-grain and white identity: missing variety evidence is not safely recoverable from the historical titles.

Run all existing Apps Script contract tests plus the new regression suite locally:

```sh
node tests/run.js
```

See [measurement specification](MEASUREMENT_SPEC.md) for the exact contract and [September audit](docs/SEPTEMBER_2026_RELIABILITY_AUDIT.md) for replay results, deployment checks, risks and rollback. This change is proposed through a PR; merging and Apps Script deployment are separate actions.

## Separate Bitcoin projects

| Project | Purpose | Location |
|---|---|---|
| **Bitcoin Basket-of-Goods Index** | Measures how Bitcoin purchasing power changes against a fixed shopping basket. | This `Bitcoin-Dashboard` repository and its Google Apps Script web app. |
| **v6 Solo Mining Dashboard** | Runs or monitors solo SHA-256 mining attempts, including hashrate, shares, best difficulty, and block candidates. | Separate local mining app at `http://127.0.0.1:8791/?version=v6`. |

## Naming rule

Use **Basket-of-Goods Index** when discussing prices, purchasing power, basket composition, item histories, or data confidence.

Use **v6 Solo Mining Dashboard** when discussing hashes, hashrate, shares, best difficulty, Stratum, block candidates, or mining rewards.

Keeping these names separate prevents the index dashboard from being mistaken for software that performs Bitcoin mining.

