# AI Livestream Controller

The `AILivestreamController` is designed to consume external machine learning (ML) signals via MQTT and translate them into trading actions.

## Overview
This controller doesn't generate its own technical signals. Instead, it listens to a specific MQTT topic for predictions (probabilities) and acts based on them.

## Logic
- **Signal Source:** Listens to an MQTT topic (default: `hbot/predictions/{trading_pair}/ML_SIGNALS`).
- **Probabilities:** It expects a dictionary containing probabilities for `short`, `neutral`, and `long`.
- **Thresholds:**
  - **Long:** Triggered if the `long` probability exceeds the `long_threshold`.
  - **Short:** Triggered if the `short` probability exceeds the `short_threshold`.
- **Dynamic Risk:** It can adjust the triple-barrier configuration (Take Profit/Stop Loss) based on a `target_pct` provided in the ML signal.

## Best For
Integrating external ML models or real-time signal providers into Hummingbot without needing to implement the ML logic within the bot itself.
