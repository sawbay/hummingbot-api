# AI Livestream Controller

The `AILivestreamController` is a specialized strategy that receives trading signals from an external AI/ML model via MQTT.

## Overview
It listens to a specific MQTT topic for prediction probabilities and enters trades when the confidence level for long or short positions exceeds user-defined thresholds.

## Logic
- **MQTT Listener:** Connects to an external MQTT broker and subscribes to `hbot/predictions/{pair}/ML_SIGNALS`.
- **Signal Processing:** 
  - Receives a message with probabilities for `[short, neutral, long]`.
  - **Long Signal:** If `long_probability > long_threshold`.
  - **Short Signal:** If `short_probability > short_threshold`.
- **Volatility-Adjusted TP/SL:** Automatically adjusts the triple barrier (Take Profit / Stop Loss) based on a `target_pct` (volatility estimate) provided by the AI model.
- **Execution:** Uses a standard position executor.

## Best For
Integrating external machine learning models or signal providers that can stream real-time predictions via MQTT.
