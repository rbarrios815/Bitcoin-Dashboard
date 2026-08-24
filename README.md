# Bitcoin Basket-of-Goods Index

## Project identity

This repository is the **basket-of-goods purchasing-power index project**.

It compares the cost of a fixed, standardized shopping basket in:

- U.S. dollars
- satoshis
- an indexed series with a visible baseline of 100

It also reports basket composition, item-level changes, carried-forward values, data confidence, and reference comparisons.

**This repository does not mine Bitcoin.**

## SerpApi query budget

The daily collector refreshes a rotating subset of shopping items so a 250-search monthly plan lasts the full month. Defaults:

- `SERPAPI_MONTHLY_BUDGET=220` reserves 30 provider searches outside the dashboard.
- `SERPAPI_MAX_SEARCHES_PER_DAY=6` refreshes all 12 default SerpApi-backed items every two days.
- A second run on the same calendar day does not consume more SerpApi searches.
- A provider quota-exhausted response blocks additional SerpApi calls for the rest of that month.
- Items not selected for the day's rotation continue to use visibly marked stale fallback values.

Run `getSerpApiBudgetStatus()` in Apps Script to inspect the dashboard's locally tracked usage. The usage counter resets automatically when the calendar month changes.

## Reliability grade

Current coverage and long-term reliability are separate:

- Current coverage counts validated basket items whose most recent fresh observation is no more than 48 hours old.
- The reliability grade uses the rolling scheduled-refresh record, not a single snapshot.
- An A requires a full 30-day track record, 12/12 current basket items, zero missing latest items, and at least 95% scheduled-refresh success.
- With six planned searches per day, 95% means at least 171 successful validations out of 180 opportunities in a 30-day window.
- `RELIABILITY_START_DATE` defaults to `2026-09-01`, the first full provider cycle after the quota-safe collector was introduced.

## Separate Bitcoin projects

| Project | Purpose | Location |
|---|---|---|
| **Bitcoin Basket-of-Goods Index** | Measures how Bitcoin purchasing power changes against a fixed shopping basket. | This `Bitcoin-Dashboard` repository and its Google Apps Script web app. |
| **v6 Solo Mining Dashboard** | Runs or monitors solo SHA-256 mining attempts, including hashrate, shares, best difficulty, and block candidates. | Separate local mining app at `http://127.0.0.1:8791/?version=v6`. |

## Naming rule

Use **Basket-of-Goods Index** when discussing prices, purchasing power, basket composition, item histories, or data confidence.

Use **v6 Solo Mining Dashboard** when discussing hashes, hashrate, shares, best difficulty, Stratum, block candidates, or mining rewards.

Keeping these names separate prevents the index dashboard from being mistaken for software that performs Bitcoin mining.
