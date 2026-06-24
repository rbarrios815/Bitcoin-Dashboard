# Bitcoin Purchasing Power Dashboard — Measurement Specification v2

## Purpose

The dashboard measures how much of the same standardized real-world shopping basket can be purchased with dollars and satoshis over time. It is an evidence system, not a promise that Bitcoin purchasing power always rises.

## Canonical basket

The core basket contains exactly the configured ten grocery items. Each item represents one fixed target package, such as a 3 lb bag of Honeycrisp apples or one dozen large Grade A eggs.

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

## Confidence grade

Fresh observations receive full credit. Carried-forward observations receive 35% credit. Missing or rejected items receive no credit.

- A: score at least 95
- B: at least 80
- C: at least 60
- D: at least 40
- F: below 40

Short-window sats changes with carried-forward grocery prices are provisional because BTC/USD can move while merchandise observations remain unchanged.

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
