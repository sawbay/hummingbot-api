# Bollinger V2 Controller

The `BollingerV2Controller` is an enhanced version of the Bollinger strategy that adds trend filtering to improve signal reliability.

## Overview
Unlike V1, which trades every touch of the bands, V2 uses moving averages and volatility indicators to ensure that trades are taken in the direction of the broader trend or when volatility conditions are favorable.

## Logic
- **Advanced Filtering:** It incorporates "Fast" and "Slow" moving averages (typically EMA) to confirm trend direction.
- **Trend-Following:**
  - It generally restricts Longs to bullish trends and Shorts to bearish trends.
- **Volatility Awareness:** Can monitor Bollinger Band Width to avoid trading during "squeezes" or to target high-volatility expansions.
- **Execution:** Uses a standard position executor but with more "cautious" entry criteria than V1.

## Best For
Trend-following or mean-reversion with a trend filter. It helps avoid "catching a falling knife" in strong trending markets where V1 might produce many losing contrarian signals.
