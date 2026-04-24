# SuperTrend V1 Controller

The `SuperTrendV1Controller` is a trend-following strategy based on the popular SuperTrend indicator.

## Overview
It uses the SuperTrend indicator to determine the current market direction and enters trades when the price is near the SuperTrend line but still moving in the trending direction.

## Logic
- **Indicator:** SuperTrend (calculates trend based on ATR and Median Price).
- **Entry Logic:**
  - **Long:** Triggered when the SuperTrend signal is bullish (1) AND the price is within a `percentage_threshold` of the SuperTrend line.
  - **Short:** Triggered when the SuperTrend signal is bearish (-1) AND the price is within a `percentage_threshold` of the SuperTrend line.
- **Goal:** To enter a trend during a "pullback" to the support/resistance level defined by the SuperTrend line.

## Best For
Pure trend-following strategies in trending markets. It helps traders join an existing trend at a more favorable price point by waiting for the price to come close to the indicator's trend line.
