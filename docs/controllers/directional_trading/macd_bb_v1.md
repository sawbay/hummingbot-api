# MACD BB V1 Controller

The `MACDBBV1Controller` combines Bollinger Bands with the MACD (Moving Average Convergence Divergence) indicator to create a multi-confirmed entry signal.

## Overview
This controller uses the Bollinger Bands to identify potential reversal zones and the MACD Histogram/Signal to confirm momentum before entering a trade.

## Logic
- **Indicators:**
  - Bollinger Bands (%b).
  - MACD (Fast, Slow, and Signal periods).
- **Confirmation Logic:**
  - **Long:** Price is at the lower Bollinger Band AND MACD Histogram is positive AND MACD line is below zero.
  - **Short:** Price is at the upper Bollinger Band AND MACD Histogram is negative AND MACD line is above zero.
- **Goal:** To ensure that mean-reversion trades are taken only when there is evidence of momentum shifting back toward the mean.

## Best For
Traders who find Bollinger Bands alone too "noisy" and want a secondary momentum-based confirmation to avoid entering a reversal trade too early.
