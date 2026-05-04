# Manual Test Plan: Controller Management

This test plan verifies the deep controller management capabilities, allowing start, stop, and config hot-reloading for individual controllers inside a running Hummingbot instance.

## Prerequisites

1.  Hummingbot API is running (`make run`).
2.  A Hummingbot V2 instance with controllers is active (e.g., deployed via `POST /bot-orchestration/deploy-v2-controllers`).
3.  The bot name is known (e.g., `my-bot`).
4.  A controller ID is known (e.g., `directional_strategy_1`).

---

## 1. Stop Controller

### REST API
**Command:**
```bash
curl -X POST http://localhost:8000/bot-orchestration/my-bot/controllers/directional_strategy_1/stop \
     -u admin:admin
```

**Expected Response:**
```json
{
  "status": "success",
  "data": {
    "success": true,
    "controller_id": "directional_strategy_1"
  }
}
```

### WebSocket
**Message:**
```json
{
  "action": "command",
  "command": "stop_controller",
  "bot_name": "my-bot",
  "params": {
    "controller_id": "directional_strategy_1"
  },
  "request_id": "req_stop_1"
}
```

**Verification:**
1.  Check `bot_status` WebSocket subscription. The controller status should eventually reflect that it's stopped.
2.  The bot container should still be running (check `docker ps`).

---

## 2. Start Controller

### REST API
**Command:**
```bash
curl -X POST http://localhost:8000/bot-orchestration/my-bot/controllers/directional_strategy_1/start \
     -u admin:admin
```

**Expected Response:**
```json
{
  "status": "success",
  "data": {
    "success": true,
    "controller_id": "directional_strategy_1"
  }
}
```

### WebSocket
**Message:**
```json
{
  "action": "command",
  "command": "start_controller",
  "bot_name": "my-bot",
  "params": {
    "controller_id": "directional_strategy_1"
  },
  "request_id": "req_start_1"
}
```

**Verification:**
1.  Check `bot_status` WebSocket subscription. The controller status should reflect it's running.

---

## 3. Update Controller Config

### REST API
**Command:**
```bash
curl -X PUT http://localhost:8000/bot-orchestration/my-bot/controllers/directional_strategy_1/config \
     -u admin:admin \
     -H "Content-Type: application/json" \
     -d '{
       "params": {
         "connector_name": "binance",
         "trading_pair": "ETH-USDT",
         "max_executors_per_side": 3
       }
     }'
```

**Expected Response:**
```json
{
  "status": "success",
  "data": {
    "success": true,
    "controller_id": "directional_strategy_1"
  }
}
```

### WebSocket
**Message:**
```json
{
  "action": "command",
  "command": "update_controller_config",
  "bot_name": "my-bot",
  "params": {
    "controller_id": "directional_strategy_1",
    "params": {
      "connector_name": "binance",
      "trading_pair": "ETH-USDT"
    }
  },
  "request_id": "req_config_1"
}
```

**Verification:**
1.  Check the bot's logs to see if the configuration was reloaded.
2.  The bot's container uptime/start time should NOT change (no restart).

---

## 4. Error Cases

### Non-existent Bot
**Command:**
```bash
curl -X POST http://localhost:8000/bot-orchestration/non-existent-bot/controllers/ctrl_1/stop \
     -u admin:admin
```
**Expected Response:** `404 Not Found` with message "Bot 'non-existent-bot' not found in active bots".

### Missing Controller ID (WS)
**Message:**
```json
{
  "action": "command",
  "command": "stop_controller",
  "bot_name": "my-bot",
  "request_id": "req_fail_1"
}
```
**Expected Response:** `command_result` with `success: false` and message "stop_controller requires 'controller_id'".
