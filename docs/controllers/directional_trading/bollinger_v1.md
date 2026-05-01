# Bollinger V1 Controller

The `BollingerV1Controller` is a simple mean-reversion strategy based on the Bollinger Band Percentage (%b) indicator.

## Overview
It monitors the price relative to the upper and lower Bollinger Bands and generates signals when the price goes beyond specified thresholds.

## Logic
- **Indicator:** Uses standard Bollinger Bands (BB) with configurable length and standard deviation.
- **Signal Calculation:** 
  - Calculates Bollinger Band Percentage (`bbp`): `(close - lower_band) / (upper_band - lower_band)`.
  - **Long Signal:** When `bbp < bb_long_threshold` (default 0.0, i.e., touching the lower band).
  - **Short Signal:** When `bbp > bb_short_threshold` (default 1.0, i.e., touching the upper band).
- **Execution:** Uses a standard position executor.

## Best For
Simple mean-reversion trading in sideways or range-bound markets where price extremes are likely to be followed by a reversal.
