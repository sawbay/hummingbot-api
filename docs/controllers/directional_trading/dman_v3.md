# DMan V3 Controller

The `DManV3Controller` is a sophisticated mean-reversion strategy that combines Bollinger Bands with dynamic DCA (Dollar Cost Averaging) execution.

## Overview
It uses Bollinger Band width to dynamically adjust order spreads and shift mid-prices, allowing the strategy to adapt to varying market volatility.

## Logic
- **Indicator:** Uses Bollinger Bands to measure market volatility (BB Width).
- **Dynamic Spreads:** The `dca_spreads` are multiplied by a `spread_multiplier` derived from the current Bollinger Band Width (if `dynamic_order_spread` is enabled).
- **DCA Execution:**
  - Creates a `DCAExecutor` instead of a simple position executor.
  - Supports multiple levels of entry with configurable spreads and amounts.
  - Can make the take-profit and trailing-stop targets dynamic based on volatility.
- **Signal:** Generates entry signals based on Bollinger Band Percentage (%b) thresholds.

## Best For
Trading in volatile markets where a single entry point might be risky, and using DCA helps to achieve a better average entry price.
