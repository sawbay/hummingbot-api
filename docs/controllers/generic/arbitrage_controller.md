# Arbitrage Controller

The `ArbitrageController` is designed to exploit price discrepancies between two different exchange pairs. It monitors the price difference and executes trades when the profitability exceeds a specified threshold.

## Overview
It coordinates two `ArbitrageExecutor`s (one for buying on exchange A and selling on exchange B, and vice versa) to capture arbitrage opportunities.

## Logic
- **Exchanges:** Operates on two user-defined connector pairs.
- **Profitability:** Monitors real-time prices and calculates potential profit after accounting for gas (if using AMMs).
- **Execution:** 
  - Creates an `ArbitrageExecutor` when the profit is above `min_profitability`.
  - Implements an imbalance check (`max_executors_imbalance`) to prevent excessive exposure in one direction.
  - Includes a cooldown period (`delay_between_executors`) between consecutive arbitrage attempts.
- **Conversion:** Handles quote asset conversion and gas token price fetching for accurate profitability calculation on AMMs.

## Best For
Capturing market inefficiencies between centralized and decentralized exchanges or between different CEXs.
