# Migration notes

The new application reads the existing `GroceryPriceHistory` and `RawOffers` tabs and retains their current field contracts. Legacy stored basket totals are treated as audit fields; v2 rebuilds purchasing-power totals from row-level package prices.
