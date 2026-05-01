# PMM Mister Controller

The `PMMisterController` is an advanced Pure Market Making controller with sophisticated position management, featuring hanging executors and price distance requirements.

## Overview
It provides liquidity with enhanced control over order placement and refresh logic, specifically designed for markets where traditional PMM might be too simplistic.

## Logic
- **Hanging Executors:** Supports "hanging" orders that stay open even after the mid-price moves, allowing for better fill rates in volatile markets.
- **Price Distance Tolerance:** Prevents placing new orders if existing ones are too close to the current price, reducing noise and unnecessary fees.
- **Effectivization:** Implements a delay after a fill before an order is considered "effective," helping to manage rapid price movements.
- **Tolerance Scaling:** Dynamically adjusts refresh tolerances based on market conditions.
- **Inventory Management:** Similar to other PMM controllers, it targets a `target_base_pct` and uses skewing to maintain balance.
- **Profit Protection:** Includes optional position profit protection based on break-even prices.

## Best For
Professional market making where fine-tuned control over order behavior, refresh logic, and hanging levels is required.
