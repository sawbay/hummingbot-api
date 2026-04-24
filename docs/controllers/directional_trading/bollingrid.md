# BollinGrid Controller

The `BollinGridController` is a hybrid strategy that uses Bollinger Bands to identify entry zones and a Grid execution strategy to manage the trade.

## Overview
Instead of a single entry, BollinGrid uses a grid of orders. The placement and range of this grid are dynamically calculated based on market volatility (Bollinger Band Width).

## Logic
- **Signal:** Based on Bollinger Band Percentage (%b), similar to V1.
- **Dynamic Grid Range:**
  - Uses the **Bollinger Band Width** to determine the spread between grid orders.
  - Higher volatility = wider grid.
- **Configurable Coefficients:** Allows setting `grid_start_price`, `grid_end_price`, and `grid_limit_price` as multipliers of the current band width.
- **Execution:** Uses a `GridExecutor` to place multiple orders, allowing for "averaging in" to a position.

## Best For
Mean reversion in volatile markets. It excels when the price overshoots a Bollinger Band and oscillates before returning to the mean, allowing the grid to capture multiple small moves or improve the average entry price.
