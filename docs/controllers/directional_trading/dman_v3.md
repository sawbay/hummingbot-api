# DMan V3 Controller

The `DManV3Controller` (Dynamic Market Maker v3) is an advanced mean-reversion strategy that uses Bollinger Bands for timing and DCA (Dollar Cost Averaging) for execution.

## Overview
DMan V3 is designed to handle market "overshoots" by planning multiple entry levels. It is highly dynamic, adjusting its entry spreads and exit targets based on real-time volatility.

## Logic
- **Signal:** Standard Bollinger Band extremes (%b).
- **Execution:** Uses a `DCAExecutor` to place multiple limit orders.
- **Dynamic Adjustments:**
  - **Spreads:** DCA levels can be spaced based on a multiplier of the current Bollinger Band Width.
  - **Volatility Scaling:** Both Stop Loss and Trailing Stop can be dynamically adjusted based on volatility.
- **DCA Customization:** Supports configurable spreads and amount distributions (e.g., equally weighted or martingaling).

## Best For
Professional-grade mean reversion. It is particularly effective at capturing reversals in high-volatility environments while managing risk through multi-level entries and trailing stops.
