# Test Plan: Bot Deployment Lifecycle

This document outlines the testing procedures for the Hummingbot deployment flow, including health checks, database state transitions, and real-time WebSocket updates.

---

## Prerequisites

See [Test Setup Prerequisites](setup.md) for environment requirements.

---

## Phase 1: Happy Path (Successful Deployment)

**Goal**: Verify that a healthy container transitions from `CREATED` to `RUNNING` and notifies via WebSocket.

### 1. Monitor All Bots Status
Open a WebSocket connection to observe the `pending_bots` registry.
```bash
wscat -c "ws://localhost:8000/ws/executors?username=admin&password=admin"
# Send:
{"action":"subscribe","type":"all_bots_status","update_interval":2}
```

### 2. Deploy Bot
```bash
curl -s -u admin:admin -X POST http://localhost:8000/bot-orchestration/deploy-v2-controllers \
  -H "Content-Type: application/json" \
  -d '{
    "instance_name": "test-grid",
    "credentials_profile": "master_account",
    "controllers_config": ["btc-usdt-grid"],
    "image": "hummingbot/hummingbot:latest",
    "headless": true
  }' | jq .
```
*Note the `unique_instance_name` in the response.*

### 3. Track Deployment (WebSocket)
In the same or a new `wscat` window:
```json
{"action":"subscribe","type":"bot_deployment","instance_name":"test-grid-YYYYMMDD-HHMMSS","update_interval":2}
```
**Expected Behavior**:
- Status: `deploying` → `running`.
- Terminal Event: `bot_deployment_resolved` with `final_status: "running"`.

### 4. Verify Persistence
```bash
curl -s -u admin:admin "http://localhost:8000/bot-orchestration/bot-runs?bot_name=test-grid-YYYYMMDD-HHMMSS" | jq '.data[0].run_status'
```
**Expected**: `"RUNNING"`

---

## Phase 2: Failure Path (Container Crash)

**Goal**: Verify that startup failures (e.g., missing config) are captured in the DB and notified via WS.

### 1. Deploy with Invalid Config
```bash
curl -s -u admin:admin -X POST http://localhost:8000/bot-orchestration/deploy-v2-controllers \
  -H "Content-Type: application/json" \
  -d '{
    "instance_name": "test-fail",
    "credentials_profile": "master_account",
    "controllers_config": ["non-existent-config"],
    "image": "hummingbot/hummingbot:latest",
    "headless": true
  }' | jq .
```

### 2. Track Deployment (WebSocket)
**Expected Behavior**:
- Status: `deploying` → `failed`.
- Error Message: `Container exited with code 1`.

### 3. Verify Error Logs in DB
```bash
curl -s -u admin:admin "http://localhost:8000/bot-orchestration/bot-runs?bot_name=test-fail-YYYYMMDD-HHMMSS" | jq '.data[0]'
```
**Expected**:
- `run_status`: `"ERROR"`
- `deployment_status`: `"FAILED"`
- `error_message`: Contains Docker container logs.

---

## Phase 3: REST Polling

Verify the fallback polling endpoint provides the same data as the WebSocket.
```bash
curl -s -u admin:admin http://localhost:8000/bot-orchestration/deployment-status/test-grid-YYYYMMDD-HHMMSS | jq .
```

---

## Phase 4: Cleanup
```bash
curl -s -u admin:admin -X POST "http://localhost:8000/bot-orchestration/stop-and-archive-bot/test-grid-YYYYMMDD-HHMMSS"
```
