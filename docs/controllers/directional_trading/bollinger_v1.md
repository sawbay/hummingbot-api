# Bollinger V1 Controller

The `BollingerV1Controller` is a simple mean-reversion strategy based on the Bollinger Band Percentage (%b) indicator.

## Overview
It monitors the price relative to the upper and lower Bollinger Bands and assumes that prices will eventually revert to the mean after touching the extremes.

## Logic
- **Indicator:** Uses Bollinger Bands with a configurable length (default 100) and standard deviation (default 2.0).
- **Signal Calculation:** It uses `%b` (Bollinger Band Percentage):
  - **Long:** Triggered when `%b` is below `bb_long_threshold` (default 0.0, i.e., touching the lower band).
  - **Short:** Triggered when `%b` is above `bb_short_threshold` (default 1.0, i.e., touching the upper band).
- **Execution:** Uses a standard position executor.

## Best For
Simple mean-reversion trading in sideways or range-bound markets where price extremes are likely to be followed by a reversal.
