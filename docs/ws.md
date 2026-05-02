# WebSocket API

The API exposes two WebSocket endpoints for real-time data streaming.  
Both use the same subscribe/unsubscribe/ping protocol and support Basic Auth.

**Base URL:** `ws://localhost:8000`

---

## Authentication

All WebSocket connections require authentication via one of three methods:

| Method | Example |
|---|---|
| `Authorization` header | `Authorization: Basic base64(user:pass)` |
| `?token=` query param | `?token=base64(user:pass)` |
| Separate query params | `?username=admin&password=admin` |

Debug mode (`DEBUG_MODE=true` in `.env`) bypasses auth.

---

## Protocol

Every message is a JSON object with an `action` field.

### Actions (client → server)

```json
{ "action": "subscribe",   "type": "<sub_type>", ...params }
{ "action": "unsubscribe", "subscription_id": "<id>" }
{ "action": "ping" }
```

### Responses (server → client)

```json
{ "type": "connected",    "connection_id": "...", "timestamp": 1234567890.0 }
{ "type": "subscribed",   "subscription_id": "...", "subscription_type": "...", "update_interval": 2.0 }
{ "type": "unsubscribed", "subscription_id": "..." }
{ "type": "heartbeat",    "timestamp": 1234567890.0 }
{ "type": "pong",         "timestamp": 1234567890.0 }
{ "type": "error",        "message": "..." }
```

Data is only pushed when the payload changes (hash-based change detection).

---

## `/ws/market-data`

Market data streaming: candles, order books, and trades.

### `candles`

Stream OHLCV candle data for a trading pair.

```json
{
  "action": "subscribe",
  "type": "candles",
  "connector": "binance",
  "trading_pair": "BTC-USDT",
  "interval": "1m",
  "update_interval": 1.0
}
```

**Push payload:**
```json
{
  "type": "candles",
  "subscription_id": "candles_binance_BTC-USDT_1m",
  "data": [
    { "timestamp": 1234567890, "open": 30000, "high": 30100, "low": 29900, "close": 30050, "volume": 12.5 }
  ],
  "timestamp": 1234567890.0
}
```

### `order_book`

Stream order book snapshots.

```json
{
  "action": "subscribe",
  "type": "order_book",
  "connector": "binance",
  "trading_pair": "BTC-USDT",
  "depth": 20,
  "update_interval": 0.5
}
```

**Push payload:**
```json
{
  "type": "order_book",
  "subscription_id": "order_book_binance_BTC-USDT",
  "data": { "bids": [[30000, 1.2], ...], "asks": [[30010, 0.8], ...] },
  "timestamp": 1234567890.0
}
```

### `trades`

Stream real-time trade events.

```json
{
  "action": "subscribe",
  "type": "trades",
  "connector": "binance",
  "trading_pair": "BTC-USDT",
  "update_interval": 0.5
}
```

**Push payload:**
```json
{
  "type": "trades",
  "subscription_id": "trades_binance_BTC-USDT",
  "data": [
    { "price": 30050, "amount": 0.1, "side": "buy", "timestamp": 1234567890 }
  ],
  "timestamp": 1234567890.0
}
```

---

## `/ws/executors`

Executor, bot status, and deployment data streaming.

### `executors`

Filtered list of executors.

```json
{
  "action": "subscribe",
  "type": "executors",
  "filters": {
    "account_name": "master_account",
    "connector_name": "binance",
    "trading_pair": "BTC-USDT",
    "status": "RUNNING"
  },
  "update_interval": 2.0
}
```

### `executor_detail`

Single executor with full detail.

```json
{
  "action": "subscribe",
  "type": "executor_detail",
  "executor_id": "abc123",
  "update_interval": 1.0
}
```

### `executor_summary`

Aggregate summary of all active executors.

```json
{ "action": "subscribe", "type": "executor_summary", "update_interval": 2.0 }
```

### `performance`

Performance report, optionally scoped to a controller.

```json
{
  "action": "subscribe",
  "type": "performance",
  "controller_id": "my-controller",
  "update_interval": 5.0
}
```

### `positions`

Held positions with live unrealized PnL.

```json
{
  "action": "subscribe",
  "type": "positions",
  "controller_id": "my-controller",
  "update_interval": 2.0
}
```

**Push payload:**
```json
{
  "type": "positions",
  "subscription_id": "positions_my-controller",
  "data": {
    "total_positions": 1,
    "total_realized_pnl": 12.5,
    "total_unrealized_pnl": -3.2,
    "positions": [
      {
        "trading_pair": "BTC-USDT",
        "connector_name": "binance",
        "realized_pnl_quote": 12.5,
        "unrealized_pnl_quote": -3.2,
        "net_amount_base": 0.05
      }
    ]
  },
  "timestamp": 1234567890.0
}
```

### `executor_logs`

Streaming log entries for a specific executor.

```json
{
  "action": "subscribe",
  "type": "executor_logs",
  "executor_id": "abc123",
  "level": "ERROR",
  "limit": 100,
  "update_interval": 1.0
}
```

Only new log entries are sent after the initial batch (delta push).

### `bot_status`

Real-time status for a single named bot (requires MQTT connection).

```json
{
  "action": "subscribe",
  "type": "bot_status",
  "bot_name": "my-bot-20260502-090000",
  "update_interval": 2.0
}
```

**Push payload:**
```json
{
  "type": "bot_status",
  "subscription_id": "bot_status_my-bot-20260502-090000",
  "data": {
    "bot_name": "my-bot-20260502-090000",
    "status": "running",
    "performance": { "my-controller": { "status": "running", "performance": { ... } } },
    "recently_active": true
  },
  "timestamp": 1234567890.0
}
```

### `all_bots_status`

Status for all active and pending bots. Includes bots that are still deploying (not yet discovered via MQTT).

```json
{ "action": "subscribe", "type": "all_bots_status", "update_interval": 2.0 }
```

**Push payload:**
```json
{
  "type": "all_bots_status",
  "subscription_id": "all_bots_status",
  "data": {
    "my-bot-20260502-090000": {
      "status": "running",
      "source": "docker",
      "performance": { ... },
      "recently_active": true
    },
    "my-bot-20260502-091500": {
      "status": "deploying",
      "source": "pending",
      "performance": {},
      "recently_active": false
    }
  },
  "bot_count": 2,
  "timestamp": 1234567890.0
}
```

---

### `bot_deployment` ⭐ New

Tracks a single bot instance through its deployment lifecycle — from the moment the Docker container is created until it is fully running or has failed.

**Use this immediately after calling `/bot-orchestration/deploy-v2-controllers` or `/bot-orchestration/deploy-v2-script`.** It replaces the need to poll the REST endpoint `/bot-orchestration/deployment-status/{instance_name}`.

**Sources checked on every interval:**
1. In-memory `pending_bots` registry (instant — no network call)
2. `active_bots` dict (MQTT/Docker discovery)
3. Docker container health — exit code + crash logs

**The subscription automatically terminates** once a terminal state (`running` or `failed`) is reached, sending a `bot_deployment_resolved` message before stopping.

#### Subscribe

```json
{
  "action": "subscribe",
  "type": "bot_deployment",
  "instance_name": "my-bot-20260502-090000",
  "update_interval": 2.0
}
```

| Field | Required | Description |
|---|---|---|
| `instance_name` | ✅ | The `unique_instance_name` returned by the deploy endpoint |
| `update_interval` | ❌ | Push interval in seconds (default: 2.0, min: 0.5, max: 60) |

#### Push payload — status update

Sent on every state change:

```json
{
  "type": "bot_deployment",
  "subscription_id": "bot_deployment_my-bot-20260502-090000",
  "data": {
    "instance_name": "my-bot-20260502-090000",
    "overall_status": "deploying",
    "is_active": false,
    "pending_status": "deploying",
    "pending_error": null,
    "container": {
      "found": true,
      "status": "running",
      "running": true,
      "exit_code": 0,
      "error": null
    }
  },
  "timestamp": 1234567890.0
}
```

#### `overall_status` values

| Value | Meaning |
|---|---|
| `"deploying"` | Container created, waiting for MQTT discovery |
| `"running"` | Bot discovered via Docker/MQTT — terminal ✅ |
| `"failed"` | Container exited with non-zero code or was not found — terminal ❌ |

#### Push payload — terminal event (`bot_deployment_resolved`)

Sent **once** when a terminal state is reached. After this, the push loop exits and no further messages are sent for this subscription.

```json
{
  "type": "bot_deployment_resolved",
  "subscription_id": "bot_deployment_my-bot-20260502-090000",
  "instance_name": "my-bot-20260502-090000",
  "final_status": "running",
  "timestamp": 1234567890.0
}
```

#### Failure example

When a container exits immediately (e.g. wrong config or exchange credentials):

```json
{
  "type": "bot_deployment",
  "subscription_id": "bot_deployment_my-bot-20260502-090000",
  "data": {
    "instance_name": "my-bot-20260502-090000",
    "overall_status": "failed",
    "is_active": false,
    "pending_status": "failed",
    "pending_error": "Container exited with code 1",
    "container": {
      "found": true,
      "status": "exited",
      "running": false,
      "exit_code": 1,
      "error": "Container exited with code 1"
    }
  },
  "timestamp": 1234567890.0
}
```

> **Note:** Container logs on failure are captured server-side and stored in the `BotRun.error_message` DB field. Fetch them via `GET /bot-orchestration/bot-runs?bot_name=<instance_name>` or the `GET /bot-orchestration/deployment-status/<instance_name>` REST endpoint.

#### Complete deployment flow (recommended)

```
POST /bot-orchestration/deploy-v2-controllers
  ← { "success": true, "unique_instance_name": "my-bot-20260502-090000" }

WS connect → /ws/executors
  ← { "type": "connected" }

→ { "action": "subscribe", "type": "bot_deployment",
    "instance_name": "my-bot-20260502-090000", "update_interval": 2 }
  ← { "type": "subscribed", "subscription_id": "bot_deployment_my-bot-..." }

  ← { "type": "bot_deployment", "data": { "overall_status": "deploying" } }
  ← { "type": "bot_deployment", "data": { "overall_status": "running" } }
  ← { "type": "bot_deployment_resolved", "final_status": "running" }

→ { "action": "subscribe", "type": "bot_status",
    "bot_name": "my-bot-20260502-090000" }   ← switch to ongoing monitoring
```

---

## Update Interval Limits

| | Min | Default | Max |
|---|---|---|---|
| `update_interval` | `0.5s` | `2.0s` | `60.0s` |
