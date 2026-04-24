# Bollingrid Controller

The `BollingridController` is a mean-reversion strategy that combines Bollinger Bands with Grid execution.

## Overview
It uses Bollinger Band signals to trigger the creation of a grid of orders, aiming to capture the price movement as it reverts towards the middle band or continues a breakout.

## Logic
- **Indicator:** Bollinger Bands (BB) with configurable length and standard deviation.
- **Signal:** Standard Bollinger Band Percentage (%b) triggers.
- **Dynamic Grid Parameters:**
  - When a signal is triggered, it calculates `start_price`, `end_price`, and `limit_price` as coefficients of the current Bollinger Band Width.
  - This allows the grid to automatically scale its range and order spacing based on market volatility.
- **Execution:**
  - Uses `GridExecutor` to manage multiple limit orders within the calculated range.
  - Supports `max_open_orders`, `order_frequency`, and `min_spread_between_orders`.
- **Risk Management:** Uses `TripleBarrierConfig` for the overall grid performance.

## Best For
Trading mean-reversions or breakouts in volatile markets where using a grid of orders is preferred over a single entry point.
