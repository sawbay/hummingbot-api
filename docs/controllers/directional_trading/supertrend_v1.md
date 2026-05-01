# SuperTrend V1 Controller

The `SuperTrendV1Controller` is a trend-following strategy based on the popular SuperTrend indicator.

## Overview
It follows the direction of the market trend as defined by the SuperTrend indicator, with an additional filter for the distance between the price and the indicator line.

## Logic
- **Indicator:** SuperTrend, which uses ATR (Average True Range) to calculate potential trend reversals.
- **Signal Generation:**
  - **Long Signal:** SuperTrend indicates an uptrend AND the current price is within a `percentage_threshold` distance from the SuperTrend line.
  - **Short Signal:** SuperTrend indicates a downtrend AND the current price is within a `percentage_threshold` distance from the SuperTrend line.
- **Percentage Filter:** This filter ensures that the strategy doesn't enter a trade if the price has already moved too far from the trend reversal point.
- **Execution:** Uses a standard position executor.

## Best For
Following strong market trends and entering shortly after a trend reversal has been confirmed.
