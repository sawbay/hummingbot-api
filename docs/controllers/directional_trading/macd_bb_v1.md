# MACD BB V1 Controller

The `MACDBBV1Controller` is a trend-following and mean-reversion strategy that combines Bollinger Bands with the MACD indicator for signal confirmation.

## Overview
It uses Bollinger Bands to identify price extremes and MACD to confirm that the momentum is shifting in the desired direction before entering a trade.

## Logic
- **Indicators:**
  - **Bollinger Bands:** Identifies overbought/oversold conditions.
  - **MACD (Moving Average Convergence Divergence):** Measures momentum and trend direction.
- **Signal Confirmation:**
  - **Long Signal:** Price is near/below the lower Bollinger Band (`bbp < threshold`) AND MACD histogram is positive (`macdh > 0`) AND MACD line is below zero (`macd < 0`).
  - **Short Signal:** Price is near/above the upper Bollinger Band (`bbp > threshold`) AND MACD histogram is negative (`macdh < 0`) AND MACD line is above zero (`macd > 0`).
- **Execution:** Uses a standard position executor.

## Best For
Capturing reversals with momentum confirmation, helping to filter out false signals that might occur in a strongly trending market.
