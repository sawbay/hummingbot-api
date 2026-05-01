# Bollinger V2 Controller

The `BollingerV2Controller` is a mean-reversion strategy based on the Bollinger Band Percentage (%b) indicator, using TALib for indicator calculation.

## Overview
It monitors the price relative to the upper and lower Bollinger Bands and generates signals when the price goes beyond specified thresholds.

## Logic
- **Indicator:** Uses Bollinger Bands calculated via TALib for high performance.
- **Signal Calculation:**
  - Calculates Bollinger Band Percentage (`bbp`): `(close - lower_band) / (upper_band - lower_band)`.
  - Includes a `non_zero_range` utility to prevent division by zero errors during high volatility.
  - **Long Signal:** When `bbp < bb_long_threshold`.
  - **Short Signal:** When `bbp > bb_short_threshold`.
- **Execution:** Uses a standard position executor.

## Best For
Mean-reversion trading where high-performance indicator calculation (TALib) is preferred.
