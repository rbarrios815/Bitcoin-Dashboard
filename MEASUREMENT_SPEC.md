# Bitcoin Purchasing Power Dashboard — Measurement Specification v2 (reliability methodology v3)

## Purpose

The dashboard measures how much of the same standardized real-world shopping basket can be purchased with dollars and satoshis over time. It is an evidence system, not a promise that Bitcoin purchasing power always rises.

## Canonical basket

The core basket contains exactly the configured grocery items. Each item represents one fixed target package, such as a 3 lb bag of Honeycrisp apples or one dozen large Grade A eggs.

The basket total is the **sum of valid standardized package prices**. It is not an average. Gold, silver, electricity, $10, and 10,000 sats are reference series and never contribute to grocery-basket math.

## Comparable ranges

A range comparison must use the same item identities at both boundaries. Missing products must be disclosed as partial coverage rather than silently changing the composition of the basket. A missing interior observation should appear as a chart gap.

## Product-price validation

1. Product identity must match required keywords and avoid excluded keywords.
2. Package unit and quantity must match the canonical target within tolerance.
3. Grocery marketplace offers from eBay, Etsy, Whatnot, Shop LC, Alibaba, AliExpress, or Temu are rejected.
4. Broad item-specific normalized-unit plausibility rails reject obvious mismatches.
5. Up to five distinct vendors are selected.
6. A median is computed after a median-absolute-deviation outlier screen.
7. Failed current retrievals may use the last validated price, but the observation is marked carried forward.

## Electricity

Electricity is not a shopping product. The reference uses BLS series `APU000072610`, Electricity per KWH in U.S. city average, multiplied by the tracked 5 kWh quantity. This is a monthly national benchmark, not a Houston utility-bill quote.

## Current coverage and reliability grade

Current coverage is not a letter grade. An item is current when its latest usable basket value is backed by a validated fresh observation collected within the previous 48 hours. The interface reports current, older, and missing counts separately.

The reliability grade measures sustained operation over the rolling scheduled-refresh window. An A is permitted only when all of these conditions hold:

- At least 30 calendar days of the current basket methodology have elapsed since its first durable grocery schedule. The start date is disclosed and is derived from the ledger, not a configurable backdate.
- Every configured grocery item is current within 48 hours.
- No latest basket item is missing.
- At least 95% of scheduled refresh opportunities succeeded during the rolling window.

The numerator counts successful fresh grocery validations in `RefreshSchedule`; the denominator counts actual scheduled grocery item/day opportunities, including pending/crashed requests and all failures. Neither references nor unscheduled carry-forwards enter this fraction. The default six-grocery daily plan normally produces 180 opportunities and requires 171 successful validations, but 180 is never substituted for the actual ledger count. An A also requires 30 distinct scheduled days and at least 15 opportunities for each configured grocery, independently of the percentage. Missing schedule days therefore cannot improve the grade. Lower grades use the weaker of current-coverage percentage and scheduled-refresh success: B at 80, C at 60, D at 40, and F below 40. Before 30 days, the grade is `Building`.

Short-window sats changes with older grocery prices remain provisional because BTC/USD can move while merchandise observations remain unchanged.

## Primary headline

`10,000 sats basket share = 10,000 / basket cost in sats`

The interface must show the basket coverage and confidence beside this result.

## Interpretation

- Falling basket sats means Bitcoin purchasing power improved against the measured basket.
- Rising basket sats means it declined.
- The dashboard must report unfavorable outcomes as plainly as favorable ones.
- The sats change should be decomposed into the basket USD-price factor and BTC/USD factor.

## Limitations

Shopping-search observations are not equivalent to retailer scanner data. Vendor and geographic changes remain visible in the quality section. Legacy history remains available, but v2 recomputes basket totals from row-level data instead of trusting stored legacy basket totals.


## Methodology v3 transition and historical comparability

Collector version 2.2 introduces `RefreshSchedule` with day, item ID, provider, status, scheduled/completed timestamps, reason, methodology and sorted core-basket IDs. It is written and flushed before network requests; result status is completed only after observations are written. Interrupted work remains `scheduled` and counts as unsuccessful. Same-day retries cannot replace or multiply an opportunity. References are filtered by core ID even if a reference ledger row says `success` or `failed`. An active-basket fingerprint prevents mixing incompatible basket schedules; changing the configured composition requires its own full record before A.

The old September 1 start and old assumed-denominator calculation remain available as `legacyReliability`, explicitly separate from the live v3 grade. No history migration or raw-offer revalidation is applied to stored observations. The replay in the September audit is labelled counterfactual. Its missing request outcomes cannot be recovered from offer rows. Consequently v3 must build a new record beginning with the first durable schedule after deployment. This is a disclosed measurement boundary, not deletion of unfavorable performance.

Calendar days use the Apps Script timezone (America/Chicago by default). Currentness uses actual timestamps and an inclusive 48-hour limit; future observations cannot make an item current. The rolling window includes today's scheduled opportunities. A crash before the schedule is written leaves a coverage gap, which independently blocks A even though an unscheduled request is not fabricated in the denominator.

## Query allocation and package validation changes

The default ten groceries receive six searches daily using a persistent circular cursor. Every consecutive pair of full collection days covers all ten. Gold and silver have their own cursor, up to two searches every seven days, with a total daily cap of eight and monthly cap of 220. References may use only capacity left after reserving remaining grocery days. Explicit lower daily caps are honored. Quota blocks, interruptions and depleted budgets are disclosed through missing schedules, failed opportunities and stale values; an A cannot bypass the schedule-coverage gate. A six-search explicit total cap keeps groceries first and carries references forward until capacity is made available.

Mass conversion uses 16 oz/lb and 453.59237 g/lb; volume conversion distinguishes fluid ounces from weight ounces. Explicit multipack counts multiply package mass/count before comparison. Equivalent dual labels must agree within 2% (to accommodate rounded metric labels). Conflicting sizes/ranges, ambiguous multipacks and multiple product variants are rejected. Canonical package quantities, tolerance (bread 15%, others 25%), price bounds, vendor exclusions, median/MAD aggregation and USD/sats/basket arithmetic remain unchanged.

Size evidence can come from the same offer's title, numeric or package-size-labelled SerpApi extensions, or explicitly labelled net/package weight in a snippet. Arbitrary descriptive snippets, serving/shipping data, search queries and URLs never supply missing package size. New `RawOffers` fields append `size_evidence`, `size_source` and `validation_version`; legacy rows remain unchanged. SerpApi documents `extensions` and `snippet` in the [Shopping Results API](https://serpapi.com/shopping-results). Historical raw rows discarded these fields, so their prospective benefit cannot be quantified retrospectively.

Bread permits ordinary loaf identity without the literal `sandwich` keyword and excludes buns, bagels, rolls, crumbs, garlic bread and other unsuitable products. Honeycrisp spelling variants are equivalent; other apple varieties remain excluded. Rice still requires explicit long and white identity; `80 oz` is safely equivalent to `5 lb`. Butter can use `1 lb` or `4 x 4 oz`; a case of ten one-pound packages is rejected. Potatoes can use equivalent metric/ounce mass, but russet identity remains necessary.
