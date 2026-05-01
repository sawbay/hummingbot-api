# Quantum Grid Allocator Controller

The `QuantumGridAllocator` is a sophisticated portfolio management and grid trading controller that dynamically allocates capital across multiple assets.

## Overview
It maintains a target portfolio distribution by creating grid strategies for each asset based on its deviation from the target allocation.

## Logic
- **Portfolio Allocation:** Users define a target weight for multiple assets (e.g., 50% SOL, 50% FDUSD).
- **Zone-Based Trading:**
  - **Long-Only Zone:** If an asset is significantly under-allocated, it only creates buy grids.
  - **Short-Only Zone:** If an asset is significantly over-allocated, it only creates sell grids.
  - **Neutral Zone:** Creates both buy and sell grids to capture volatility while maintaining balance.
- **Dynamic Sizing:** The size and range of the grids are adjusted based on how far the asset has deviated from its target.
- **Indicator Integration:** Can use Bollinger Bands (BB) to determine dynamic grid ranges based on market volatility.
- **Capital Efficiency:** Only creates grids when capital is available and needed to rebalance the portfolio.

## Best For
Managing a diversified portfolio of assets while simultaneously running grid strategies to earn from market volatility.
