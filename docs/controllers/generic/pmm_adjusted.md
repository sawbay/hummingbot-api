# PMM Adjusted Controller

The `PMMAdjustedController` is a Pure Market Making strategy that adjusts its spreads and amounts based on external signals (like candles) and inventory levels.

## Overview
It aims to maintain a target inventory balance while providing liquidity through multiple buy and sell levels.

## Logic
- **Spreads & Amounts:** Supports multiple levels with configurable spreads and percentage-based amounts.
- **Inventory Skew:** Adjusts order sizes to drive the inventory back towards `target_base_pct`. 
  - If base asset exceeds target, it scales down buys and scales up sells (skew).
- **Signal Integration:** Can use candle data to adjust reference prices or spreads.
- **Risk Management:**
  - Global Take Profit and Stop Loss at the controller level.
  - Cooldown periods after trades.
  - Position-aware execution that stops providing liquidity if inventory bounds (`min_base_pct`, `max_base_pct`) are reached.
- **Visualization:** Provides rich ASCII visualizations of inventory skew and PnL.

## Best For
Providing liquidity in active markets while managing inventory risk through sophisticated skewing and signal-based adjustments.
