# Multi-Grid Strike Controller

The `MultiGridStrikeController` is an advanced version of the Grid Strike controller that supports managing multiple independent grids simultaneously.

## Overview
It allows for complex grid strategies by defining multiple `GridConfig` objects, each with its own range, side, and capital allocation.

## Logic
- **Configuration:** Users define a list of grids, each with its own `start_price`, `end_price`, `side`, and `amount_quote_pct`.
- **Dynamic Allocation:** Distributes the `total_amount_quote` among active grids based on their percentage allocation.
- **Execution:**
  - Manages multiple `GridExecutor`s.
  - Automatically handles configuration changes (adding/removing/disabling grids) at runtime.
  - Monitors mid-price against each grid's boundaries independently.
- **Monitoring:** Provides a detailed status report for each individual grid's performance and state.

## Best For
Advanced traders who want to run multiple grid strategies on the same pair (e.g., overlapping grids or different ranges) with unified capital management.
