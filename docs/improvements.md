# Improvement Roadmap for hummingbot-api

This document identifies concrete bottlenecks and architectural weaknesses in the current codebase and proposes prioritized, actionable improvements to make `hummingbot-api` production-grade and suitable for a low-latency **Terminal Trading** UI.

---

## Current Architecture Summary

Before diving into improvements, here is what the current system does well and where it falls short:

| Area | Current Approach | Bottleneck |
|---|---|---|
| Status delivery | `while True` + `asyncio.sleep(N)` polling | Fixed `N`-second latency on every update |
| Docker events | `get_active_containers()` called every 1s in `update_active_bots` | Misses fast transient events (crash → restart) |
| WS push | Hash comparison per poll cycle | CPU wasteful; misses sub-interval changes |
| Bot commands | REST `POST /stop-bot` etc. | Extra HTTP handshake overhead |
| Controller management | No per-controller enable/disable API | Full bot restart required for config changes |
| DB sessions | Per-request `get_session_context()` calls | Fine for now; may become a bottleneck under load |
| Auth | HTTP Basic Auth hardcoded in `main.py` | Not suitable for multi-user or token-based access |

---

## Improvement 1 — Event-Driven Internal Bus (Replace Polling Loops)

**Problem**: Every push loop in `executor_ws_manager.py` (`_bot_status_push_loop`, `_performance_push_loop`, `_positions_push_loop`, etc.) uses `while True: ... await asyncio.sleep(interval)`. This creates a fixed latency equal to `update_interval` (default 2s). Data arriving between polls is silently delayed.

**Root Cause**: `MQTTManager` (in `utils/mqtt_manager.py`) receives messages on MQTT callbacks but only stores them in memory dicts. The WS push loops must periodically "pull" from these dicts.

**Proposed Fix — Internal `asyncio.Queue`-Based Event Bus**:

1. Add an `EventBus` singleton (or pass as constructor arg) that each `MQTTManager` callback publishes to.
2. Each WS push loop replaces its `asyncio.sleep` with `await bus.get()` — zero artificial latency.
3. Retain the hash-comparison logic to suppress duplicate events.

```python
# utils/event_bus.py  (new file)
import asyncio
from dataclasses import dataclass, field
from typing import Any

@dataclass
class BotEvent:
    bot_name: str
    event_type: str   # "performance", "heartbeat", "log", "status"
    payload: Any

class EventBus:
    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}

    def subscribe(self, subscriber_id: str) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=1000)
        self._queues[subscriber_id] = q
        return q

    def unsubscribe(self, subscriber_id: str):
        self._queues.pop(subscriber_id, None)

    def publish(self, event: BotEvent):
        for q in self._queues.values():
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Drop; consumer is too slow
```

**Impact**: Reduces effective latency from `2000 ms → ~0 ms` for MQTT-sourced events like performance, logs, and heartbeats.

**Effort**: Medium. Requires refactoring `MQTTManager` callbacks to call `bus.publish()`, and replacing each push loop's sleep with a queue wait.

---

## Improvement 2 — Docker Event Stream (Replace `get_active_containers` Polling)

**Problem**: `BotsOrchestrator.update_active_bots()` calls `docker_client.containers.list()` every second. This is a blocking SDK call wrapped in `run_in_executor`, meaning:
- 1-second floor latency for container state changes.
- A crashed container that restarts within 1s might be missed entirely.
- Under heavy load, many parallel `containers.list()` calls accumulate.

**Proposed Fix — Docker Event Stream**:

Use `docker.client.events()` (an infinite generator that yields Docker daemon events in real time) as a background `asyncio` task:

```python
# In BotsOrchestrator or DockerService

async def _docker_event_listener(self):
    """Stream Docker lifecycle events and update state immediately."""
    loop = asyncio.get_event_loop()
    try:
        for event in await loop.run_in_executor(None, self.docker_client.events, {"type": "container"}):
            action = event.get("Action")          # "start", "die", "stop", "kill"
            name = event.get("Actor", {}).get("Attributes", {}).get("name")
            if not name:
                continue
            if action == "start":
                await self._on_container_started(name)
            elif action in ("die", "stop", "kill"):
                exit_code = int(event["Actor"]["Attributes"].get("exitCode", 0))
                await self._on_container_stopped(name, exit_code)
    except Exception as e:
        logger.error(f"Docker event stream error: {e}")
```

**Impact**: Container `CREATED → RUNNING → FAILED` transitions are pushed to clients within milliseconds rather than the current 1-5s polling window. Eliminates the "ghost bot" scenario where a crashed container lingers as "deploying" until the next poll.

**Effort**: Medium. The existing `update_active_bots` loop can be kept as a periodic reconciliation fallback (every 30s) while Docker events drive real-time updates.

---

## Improvement 3 — WebSocket Delta Protocol (Snapshots + Incremental Updates)

**Problem**: The current WS protocol sends a full JSON payload on every detected hash change. For `all_bots_status` with 20+ bots, or `performance` with many controllers, this is a large repeated payload.

**Proposed Protocol**:

| Message Type | When Sent | Payload |
|---|---|---|
| `snapshot` | On initial subscribe | Full data |
| `delta` | On subsequent changes | Only changed fields |
| `heartbeat` | Every 30s if no delta | `{ "type": "heartbeat", "timestamp": ... }` |

Example for `bot_status`:
```json
// Initial snapshot
{"type": "bot_status", "mode": "snapshot", "data": {...full status...}}

// Subsequent delta (only changed fields)
{"type": "bot_status", "mode": "delta", "data": {"status": "running", "recently_active": true}}
```

**Fine-Grained Subscription Channels** (split from current monolithic `all_bots_status`):

| Channel | Purpose | Update Frequency |
|---|---|---|
| `bot_heartbeat` | Online/offline LED indicator | Event-driven (MQTT LWT) |
| `bot_logs` | Raw log stream | Event-driven (MQTT log topic) |
| `bot_trades` | Fill events (OrderFilledEvent) | Event-driven |
| `bot_performance` | PnL, volume, position deltas | MQTT performance topic |
| `all_bots_status` | Summary table of all bots | Keep as periodic snapshot |

**Implementation Note**: Add a `mode` field to `ExecutorSubscription` and track previous state to compute diffs. Libraries like `deepdiff` or a simple field-by-field comparison work well here.

**Impact**: Reduces WS bandwidth by 60-80% for active sessions. Allows UI to subscribe only to the specific channels for the currently-visible panel.

---

## Improvement 4 — WebSocket RPC for Bot Commands

**Problem**: To send a command (start, stop, configure bot), the Terminal UI must:
1. Open a new HTTP connection.
2. Send `POST /bot-orchestration/stop-bot` with Basic Auth headers.
3. Wait for REST response.
4. Then the existing WS subscription updates status.

This is redundant when a WS connection is already open.

**Proposed Fix — WS Command Messages**:

Extend the existing WS JSON-RPC message pattern (already used for `subscribe`/`unsubscribe`) to support commands:

```json
// Client → Server
{
  "action": "command",
  "command": "stop_bot",
  "bot_name": "bot_001",
  "params": {"skip_order_cancellation": false},
  "request_id": "cmd_abc123"
}

// Server → Client (ack)
{
  "type": "command_ack",
  "request_id": "cmd_abc123",
  "status": "sent",          // or "error"
  "message": "Stop command dispatched to bot_001"
}

// Server → Client (result, async via MQTT response)
{
  "type": "command_result",
  "request_id": "cmd_abc123",
  "bot_name": "bot_001",
  "result": {...}
}
```

**Supported Commands via WS**:
- `start_bot` / `stop_bot`
- `configure_bot` (hot config update)
- `import_strategy`
- `enable_controller` / `disable_controller` (see Improvement 5)

**Impact**: Removes HTTP round-trip overhead for interactive commands. Makes the Terminal feel like a native SSH session — near-instantaneous command feedback.

**Effort**: Low-to-Medium. The router `routers/websocket.py` already handles JSON dispatch; add a `command` branch and delegate to `BotsOrchestrator`.

---

## Improvement 5 — Deep Controller Management (Hot-Reload Without Bot Restart)

**Problem**: Currently, modifying a controller's config requires:
1. Stopping the bot.
2. Editing the YAML.
3. Redeploying.

This interrupts all other controllers running inside the same bot process.

**Proposed Fix**:

The Hummingbot `v2_with_controllers.py` script already listens on the `hbot/{bot_id}/config` MQTT topic. Extend the API to publish a structured controller-targeting payload:

```python
# In MQTTManager or BotsOrchestrator
async def set_controller_config(self, bot_name: str, controller_id: str, params: dict):
    """Hot-reload a single controller's config without restarting the bot."""
    payload = {
        "controller_id": controller_id,
        "params": params
    }
    await self.publish_command(bot_name, "config", payload)

async def stop_controller(self, bot_name: str, controller_id: str):
    payload = {"controller_id": controller_id, "action": "stop"}
    await self.publish_command(bot_name, "controller", payload)

async def start_controller(self, bot_name: str, controller_id: str):
    payload = {"controller_id": controller_id, "action": "start"}
    await self.publish_command(bot_name, "controller", payload)
```

**New REST Endpoints**:
```
POST /bot-orchestration/{bot_name}/controllers/{controller_id}/start
POST /bot-orchestration/{bot_name}/controllers/{controller_id}/stop
PUT  /bot-orchestration/{bot_name}/controllers/{controller_id}/config
```

**Impact**: Enables traders to tune parameters or switch strategies on a single controller while all others remain unaffected. Critical for multi-strategy bots.

**Effort**: Medium. Requires verifying Hummingbot's MQTT `config` handler accepts controller-scoped payloads. May need a corresponding Hummingbot-side patch.

---

## Improvement 6 — Structured Authentication & Authorization

**Problem**: `main.py` uses a single global `username`/`password` from `.env` checked via HTTP Basic Auth. This means:
- All users share the same credentials.
- WebSockets have no auth (`# WebSocket router (handles its own auth)` — but `routers/websocket.py` needs review).
- No per-bot or per-account access control.

**Proposed Fix — JWT Bearer Tokens**:

1. Add a `POST /auth/token` endpoint: validates Basic credentials, returns a signed JWT (e.g., 1-hour expiry).
2. Replace `Depends(auth_user)` with `Depends(verify_jwt_token)`.
3. For WebSockets, accept the JWT as a query param: `ws://host/ws/executors?token=<jwt>`.

```python
# utils/auth.py  (new file)
import jwt
from datetime import datetime, timedelta

SECRET_KEY = settings.security.jwt_secret  # add to config.py
ALGORITHM = "HS256"

def create_access_token(username: str, expires_minutes: int = 60) -> str:
    payload = {
        "sub": username,
        "exp": datetime.utcnow() + timedelta(minutes=expires_minutes)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token: str) -> dict:
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
```

**Impact**: Enables multi-user deployments, fine-grained access control, and removes plaintext credentials from every HTTP request. Essential before exposing the API on a public network.

**Effort**: Medium. `python-jose` or `PyJWT` are lightweight drop-ins.

---

## Improvement 7 — Database Query Optimization & Caching

**Problem**: Several push loops call expensive DB queries on every poll cycle (e.g., `get_executors()` with full ORM hydration for hundreds of records every 2s). The `ExecutorService` also rebuilds position aggregates from all executors on every `get_positions_held()` call.

**Proposed Fixes**:

**A. In-Memory Executor Cache with TTL**:
```python
# In ExecutorService
_executor_cache: dict = {}
_cache_ts: float = 0.0
CACHE_TTL = 1.0  # seconds

async def get_executors_cached(self, **filters) -> list:
    now = time.monotonic()
    if now - self._cache_ts < self.CACHE_TTL:
        return self._filter_cached(self._executor_cache, **filters)
    self._executor_cache = await self._fetch_all_executors()
    self._cache_ts = now
    return self._filter_cached(self._executor_cache, **filters)
```

**B. Pagination for Large Executor Sets**:
Add `limit` and `offset` params to `GET /executors/search` and the `executors` WS subscription. This is especially important for accounts with thousands of historical executors.

**C. Indexed DB Columns**:
Ensure `controller_id`, `status`, `account_name`, and `timestamp` columns in the executors table have database indexes. Check `database/models.py` and add `index=True` to SQLAlchemy column definitions.

**Impact**: Reduces DB load and cuts `get_executors()` response time from O(N) full-scan to O(1) cache hit for most calls.

**Effort**: Low-to-Medium.

---

## Improvement 8 — Health Check & Observability Endpoint

**Problem**: There is no dedicated health check endpoint that a load balancer, Docker HEALTHCHECK, or uptime monitor can call to verify the API is fully operational (MQTT connected, DB reachable, Docker available).

**Proposed Fix**:

```python
# In main.py or a new routers/health.py
@app.get("/health")
async def health_check(request: Request):
    checks = {}
    # 1. DB
    try:
        async with request.app.state.db_manager.get_session_context() as session:
            await session.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as e:
        checks["database"] = f"error: {e}"

    # 2. MQTT
    mqtt = request.app.state.bots_orchestrator.mqtt_manager
    checks["mqtt"] = "ok" if mqtt.is_connected() else "disconnected"

    # 3. Docker
    docker_svc = request.app.state.docker_service
    checks["docker"] = "ok" if docker_svc.is_docker_running() else "unavailable"

    status_code = 200 if all(v == "ok" for v in checks.values()) else 503
    return JSONResponse(content={"status": checks}, status_code=status_code)
```

**Also add** `HEALTHCHECK` to `Dockerfile`:
```dockerfile
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1
```

**Impact**: Enables orchestrators (Docker Compose, Kubernetes) and uptime monitors to detect partial failures (e.g., MQTT disconnect while API is still responding to HTTP).

**Effort**: Low.

---

## Improvement 9 — Graceful MQTT Reconnection with Backoff

**Problem**: If EMQX goes offline briefly, the `MQTTManager` connection drops. Currently there is no documented backoff/reconnect strategy, meaning bots can appear "offline" until the API is restarted.

**Proposed Fix**:

In `utils/mqtt_manager.py`, ensure the `on_disconnect` callback triggers an exponential-backoff reconnect loop:

```python
async def _reconnect_loop(self):
    delay = 1.0
    max_delay = 60.0
    while not self._shutdown:
        try:
            logger.info(f"MQTT reconnecting in {delay:.1f}s...")
            await asyncio.sleep(delay)
            await self.client.reconnect()
            logger.info("MQTT reconnected successfully")
            return
        except Exception as e:
            logger.warning(f"MQTT reconnect failed: {e}")
            delay = min(delay * 2, max_delay)
```

**Impact**: Eliminates the need to restart the API on transient MQTT broker restarts (e.g., EMQX upgrades, brief network blips). Critical for production uptime.

**Effort**: Low.

---

## Improvement 10 — Bot Configuration Validation Before Deployment

**Problem**: When `POST /bot-orchestration/deploy-v2-controllers` is called with an invalid `script_config` YAML, the container is created, fails immediately, and the error is only surfaced via the health check / Docker logs after a 60-second timeout. The user gets poor feedback.

**Proposed Fix — Pre-flight Validation**:

Before calling `docker_service.create_hummingbot_instance()`:
1. Validate that `credentials_profile` directory exists.
2. Parse and validate the `script_config` YAML schema (required keys: `controllers_config`, etc.).
3. Check that all referenced controller YAML files exist.
4. Verify the Docker image exists locally (or is pullable).

Return a `400 Bad Request` with a specific error message before any container is created.

```python
# In bot_orchestration.py router, before deployment
def validate_deployment_config(config: V2ControllerDeployment) -> list[str]:
    errors = []
    cred_path = Path("bots/credentials") / config.credentials_profile
    if not cred_path.exists():
        errors.append(f"Credentials profile '{config.credentials_profile}' not found")
    if config.script_config:
        script_path = Path("bots/conf/scripts") / config.script_config
        if not script_path.exists():
            errors.append(f"Script config '{config.script_config}' not found")
    return errors
```

**Impact**: Fast-fail with meaningful errors instead of wasting 60s on a doomed deployment. Dramatically improves developer and operator UX.

**Effort**: Low.

---

## Prioritization Summary

| # | Improvement | Impact | Effort | Phase |
|---|---|---|---|---|
| 2 | Docker Event Stream | 🔴 High | Medium | 1 |
| 1 | Internal Event Bus | 🔴 High | Medium | 1 |
| 8 | Health Check Endpoint | 🟡 Medium | Low | 1 |
| 10 | Deploy Pre-flight Validation | 🟡 Medium | Low | 1 |
| 9 | MQTT Reconnection Backoff | 🟡 Medium | Low | 1 |
| 4 | WS RPC Commands | 🔴 High | Medium | 2 |
| 3 | Delta Protocol + Fine Channels | 🟡 Medium | Medium | 2 |
| 5 | Deep Controller Management | 🟡 Medium | Medium | 2 |
| 7 | DB Query Optimization | 🟡 Medium | Medium | 2 |
| 6 | JWT Authentication | 🟠 Critical (security) | Medium | 3 |

---

### Phase 1 — Foundation (Low-Hanging Fruit + Stability)

> **Goal**: Eliminate the most common production failures and improve observability with minimal code surface change. Each task is self-contained and can be shipped independently.

---

#### 1.1 — Docker Event Stream _(Improvement #2)_

**Description**: Replace the 1-second `containers.list()` polling loop in `BotsOrchestrator.update_active_bots()` with a real-time Docker event stream using `docker.client.events()`. Keep the polling loop as a 30-second reconciliation fallback.

**Subtasks**:
- [x] Add `_docker_event_listener()` async task to `BotsOrchestrator` that consumes `docker.client.events(filters={"type": "container"})` via `run_in_executor`.
- [x] Map Docker actions → internal handlers: `start` → `_on_container_started()`, `die`/`stop`/`kill` → `_on_container_stopped(exit_code)`.
- [x] On `_on_container_stopped` with non-zero exit code, call `mark_pending_bot_failed()` immediately and capture container logs.
- [x] Reduce `update_active_bots` sleep from `1.0s` to `30.0s` (reconciliation only).
- [x] Add restart logic for the event listener task if the Docker stream drops (reconnect with 5s delay).
- [x] Write unit tests mocking `docker.client.events()` for `start`, `die`, and `kill` actions.

**Acceptance Criteria**:
- A container crash is reflected in `pending_bots` within **< 500ms** of the Docker `die` event.
- The `update_active_bots` reconciliation still correctly syncs state after 30s if the event stream missed an event.
- No regression in the `bot_deployment` WS subscription test plan (`docs/test/bot_deployment_manual_test.md`).

---

#### 1.2 — MQTT Reconnection Backoff _(Improvement #9)_

**Description**: Add an exponential-backoff reconnect loop to `utils/mqtt_manager.py` so the API automatically recovers from transient EMQX broker restarts without requiring an API process restart.

**Subtasks**:
- [x] Implement `_reconnect_loop()` async method with initial delay `1s`, doubling up to `60s` max.
- [x] Hook into the `on_disconnect` callback of the MQTT client to trigger `_reconnect_loop()`.
- [x] On reconnect success, re-subscribe to all previously registered bot topics (`hbot/+/hb`, etc.).
- [x] Expose `MQTTManager.is_connected() -> bool` as a public method (used by the health check).
- [x] Add a `connected_since: float` timestamp attribute, reset on each reconnect.
- [x] Log all reconnect attempts at `WARNING` level with the current retry delay.

**Acceptance Criteria**:
- Stopping and restarting the EMQX container while the API is running causes zero API restarts; the MQTT connection recovers automatically.
- Reconnect attempts are visible in API logs with delay progression (1s, 2s, 4s, …).
- `is_connected()` returns `False` during the reconnect window and `True` after recovery.
- Bot heartbeat subscriptions are restored immediately after reconnect.

---

#### 1.3 — Health Check Endpoint _(Improvement #8)_

**Description**: Add a `/health` REST endpoint that checks DB, MQTT, and Docker connectivity, and wire it into the Docker `HEALTHCHECK` instruction so orchestrators can detect partial failures.

**Subtasks**:
- [x] Create `routers/health.py` with a `GET /health` endpoint (no auth required).
- [x] Check DB: execute `SELECT 1` within `get_session_context()`; report `"ok"` or `"error: <msg>"`.
- [x] Check MQTT: call `bots_orchestrator.mqtt_manager.is_connected()`.
- [x] Check Docker: call `docker_service.is_docker_running()`.
- [x] Return `HTTP 200` when all checks pass; `HTTP 503` if any check fails.
- [x] Include `{"status": {"database": "ok", "mqtt": "ok", "docker": "ok"}, "uptime_seconds": ...}` in the response body.
- [x] Add `HEALTHCHECK` directive to `Dockerfile` targeting `GET /health`.
- [x] Register the router in `main.py` **without** `Depends(auth_user)`.

**Acceptance Criteria**:
- `curl localhost:8000/health` returns `200` with all checks `"ok"` when services are running.
- Stopping the EMQX container causes `/health` to return `503` with `"mqtt": "disconnected"`.
- `docker inspect <api_container>` shows `Health: healthy` / `unhealthy` correctly reflecting the endpoint status.

---

#### 1.4 — Deploy Pre-flight Validation _(Improvement #10)_

**Description**: Before creating any Docker container, validate the deployment config against the local filesystem. Return a `400 Bad Request` with a specific error list if validation fails, rather than letting the container fail 60 seconds later.

**Subtasks**:
- [x] Create `validate_deployment_config(config: V2ControllerDeployment) -> list[str]` in `routers/bot_orchestration.py`.
- [x] Validate: credentials profile directory exists under `bots/credentials/`.
- [x] Validate: `script_config` YAML file exists under `bots/conf/scripts/` (if provided).
- [x] Validate: all `controllers_config` entries referenced in the script YAML exist under `bots/conf/controllers/`.
- [x] Validate: Docker image is available locally; if not, surface a warning (not a hard error — user may rely on auto-pull).
- [x] Call validator at the top of both `deploy-v2-controllers` and `deploy-v2-script` handlers; raise `HTTPException(400)` if errors are found.
- [x] Add a test for each validation failure case.

**Acceptance Criteria**:
- Deploying with a non-existent `credentials_profile` returns `400` immediately with message `"Credentials profile 'xyz' not found"`.
- Deploying with a missing controller YAML returns `400` listing the missing files.
- A valid config passes validation and proceeds to container creation as before.
- Response time for a failed validation is **< 50ms** (no Docker calls made).

---

### Phase 2 — Performance & UX (Terminal-Grade Responsiveness)

> **Goal**: Replace the polling architecture with an event-driven system, add WS command support, and enable fine-grained controller management. Phase 1 must be complete before starting Phase 2.

---

#### 2.1 — Internal Event Bus _(Improvement #1)_

**Description**: Introduce an `asyncio.Queue`-based `EventBus` that `MQTTManager` publishes to on every incoming message. Push loops in `ExecutorWebSocketManager` subscribe to the bus and replace their `asyncio.sleep` wait with `await queue.get()`, reducing event latency from `update_interval` seconds to near-zero.

**Subtasks**:
- [x] Create `utils/event_bus.py` with `EventBus` class and `BotEvent` dataclass (fields: `bot_name`, `event_type`, `payload`).
- [x] Instantiate `EventBus` in `main.py` `lifespan()` and store in `app.state.event_bus`.
- [x] Inject `EventBus` into `MQTTManager`; call `bus.publish(BotEvent(...))` in the `on_message` handler for `performance`, `hb`, `status_updates`, and `log` topics.
- [x] Refactor `_bot_status_push_loop`, `_performance_push_loop`, and `_logs_push_loop` in `ExecutorWebSocketManager` to use `await queue.get()` instead of `asyncio.sleep`.
- [x] Keep `asyncio.sleep(interval)` as a **heartbeat fallback** (send a `heartbeat` message if no event arrives within `update_interval`).
- [x] Ensure `EventBus.unsubscribe()` is called from `ExecutorWebSocketManager.remove_connection()` to prevent queue leaks.
- [x] Add a queue depth metric log (warn if queue depth > 100 for a subscriber).

**Acceptance Criteria**:
- A new MQTT performance message is pushed to all subscribed WS clients within **< 100ms** end-to-end.
- CPU usage of the API process is reduced when no MQTT messages are in flight (no busy-wait).
- Existing WS subscription types (`bot_status`, `performance`, `positions`) work without behavior change.
- Stress test: 10 simultaneous WS clients × 5 subscriptions each — no queue overflow or goroutine leak.

---

#### 2.2 — WS Delta Protocol + Fine-Grained Channels _(Improvement #3)_

**Description**: Add a `mode` field to WS push messages (`snapshot` on first send, `delta` on subsequent changes). Split the monolithic `all_bots_status` into narrower channels that the UI can subscribe to independently.

**Subtasks**:
- [x] Add `mode: "snapshot" | "delta"` field to all WS push message schemas.
- [x] In each push loop, send `mode: "snapshot"` on first push; compute field-level diff on subsequent pushes and send `mode: "delta"` with only changed fields.
- [x] Add new subscription type `bot_heartbeat`: event-driven (MQTT LWT + `hb` topic), sends `{ bot_name, online: bool, timestamp }`.
- [x] Add new subscription type `bot_trades`: subscribes to fill/order events from MQTT and forwards each trade event individually.
- [x] Add `heartbeat` keepalive message: if no delta is sent within `update_interval * 3`, send `{"type": "heartbeat", "subscription_id": ..., "timestamp": ...}`.
- [x] Update `SUBSCRIPTION_TYPES` set and the `_get_push_fn` dispatch map in `executor_ws_manager.py`.
- [x] Update `docs/ws.md` with the new message schemas and channel list.

**Acceptance Criteria**:
- The first message after `subscribe` always contains `"mode": "snapshot"` with full data.
- Subsequent messages only include changed fields (`"mode": "delta"`).
- `bot_heartbeat` fires within **< 200ms** of an MQTT `hb` message being received.
- WS payload size for `all_bots_status` with 20 bots and no changes drops by ≥ 70% compared to current (delta = empty).

---

#### 2.3 — WebSocket RPC for Bot Commands _(Improvement #4)_

**Description**: Extend the existing WS JSON dispatch in `routers/websocket.py` to accept `"action": "command"` messages. Route commands to `BotsOrchestrator` methods and return async acks and results over the same WS connection.

**Subtasks**:
- [x] Add `"command"` branch to the WS message dispatch in `routers/websocket.py` alongside existing `"subscribe"` and `"unsubscribe"`.
- [x] Define supported commands: `start_bot`, `stop_bot`, `configure_bot`, `import_strategy`.
- [x] For each command, immediately send a `command_ack` response (`status: "sent"` or `status: "error"`).
- [x] For commands that return MQTT responses (e.g., `history`), use `publish_command_and_wait()` and send a `command_result` message asynchronously once the response arrives.
- [x] Add `request_id` field threading: client sends `request_id`, all ack/result messages echo it back for correlation.
- [x] Add WS auth check at command dispatch (reject if connection is unauthenticated).
- [x] Document the command protocol in `docs/ws.md` with request/response examples.

**Acceptance Criteria**:
- Sending `{"action": "command", "command": "stop_bot", "bot_name": "bot_001", "request_id": "r1"}` over WS receives a `command_ack` within **< 50ms**.
- The bot actually stops (MQTT stop command is dispatched) and the corresponding `bot_status` subscription reflects the new state.
- An unknown `command` value returns `{"type": "error", "message": "Unknown command: xyz", "request_id": "r1"}`.
- REST endpoints remain fully functional alongside WS commands (no regression).

---

#### 2.4 — Deep Controller Management _(Improvement #5)_

**Description**: Add REST and WS endpoints to start, stop, and hot-reload individual controllers inside a running bot without restarting the entire bot process. Uses the MQTT `config` and `controller` topics supported by `v2_with_controllers.py`.

**Subtasks**:
- [x] Add `set_controller_config(bot_name, controller_id, params)` to `BotsOrchestrator`.
- [x] Add `stop_controller(bot_name, controller_id)` and `start_controller(bot_name, controller_id)` to `BotsOrchestrator`.
- [x] Add REST endpoints to `routers/bot_orchestration.py`:
  - `POST /bot-orchestration/{bot_name}/controllers/{controller_id}/start`
  - `POST /bot-orchestration/{bot_name}/controllers/{controller_id}/stop`
  - `PUT  /bot-orchestration/{bot_name}/controllers/{controller_id}/config`
- [x] Add WS command handlers for `enable_controller`, `disable_controller`, and `update_controller_config`.
- [x] Verify the MQTT payload format accepted by Hummingbot's `v2_with_controllers.py` for each action.
- [x] Write manual test plan in `docs/test/controller_management_manual_test.md` covering each controller action.

**Acceptance Criteria**:
- Calling `PUT /bot-orchestration/bot_001/controllers/ctrl_A/config` with new params updates the controller live; the bot does **not** restart.
- `POST /bot-orchestration/bot_001/controllers/ctrl_A/stop` stops only controller A; other controllers in the same bot keep running.
- `bot_status` WS subscription reflects the controller's new status within 5s of the action.
- Returns `404` if the bot is not active or the controller ID is unknown.

---

### Phase 3 — Security & Scale

> **Goal**: Harden the API for multi-user and public-network deployments. Phase 2 must be complete (or running in parallel with security hardening) before production exposure.

---

#### 3.1 — JWT Bearer Token Authentication _(Improvement #6)_

**Description**: Replace the single global HTTP Basic Auth credential with JWT Bearer tokens. Add a `POST /auth/token` endpoint for token issuance. WS connections accept the token via query param.

**Subtasks**:
- [ ] Add `jwt_secret` to `config.py` / `settings.security` (read from `.env`).
- [ ] Create `utils/auth.py` with `create_access_token(username, expires_minutes)` and `verify_token(token)` using `PyJWT`.
- [ ] Add `POST /auth/token` endpoint: validates Basic credentials, returns `{ "access_token": "...", "expires_in": 3600 }`.
- [ ] Replace `Depends(auth_user)` across all routers with `Depends(verify_jwt_token)`.
- [ ] Add WS token verification: extract `?token=<jwt>` query param in `routers/websocket.py`; reject with `1008 Policy Violation` if invalid.
- [ ] Keep Basic Auth on `/auth/token` only (token issuance endpoint).
- [ ] Add token expiry handling: return `HTTP 401` with `"WWW-Authenticate": "Bearer"` on expired tokens.
- [ ] Update `README.md` with the new auth flow.

**Acceptance Criteria**:
- `POST /auth/token` with valid credentials returns a JWT that can be used on all other endpoints.
- An expired or tampered token returns `401 Unauthorized`.
- WS connection without a valid token is rejected at handshake time (not after the first message).
- Existing Basic Auth credentials in `.env` still work for token issuance (backward compatibility).

---

#### 3.2 — Database Query Optimization & Caching _(Improvement #7)_

**Description**: Add an in-memory TTL cache for the executor list, add DB indexes to high-cardinality columns, and add pagination to the `GET /executors/search` endpoint.

**Subtasks**:
- [ ] Add `index=True` to `controller_id`, `status`, `account_name`, and `timestamp` columns in `database/models.py` for the executors table.
- [ ] Generate and apply an Alembic migration for the new indexes.
- [ ] Implement `get_executors_cached()` in `ExecutorService` with a 1-second TTL using `time.monotonic()`.
- [ ] Add `limit: int = 100` and `offset: int = 0` parameters to `POST /executors/search` request model and repository query.
- [ ] Add `limit` and `offset` support to the `executors` WS subscription (passed in `filters`).
- [ ] Add a `total_count` field to `GET /executors/search` response for UI pagination controls.
- [ ] Benchmark `get_executors()` before and after with a dataset of 10,000 executor records; document results.

**Acceptance Criteria**:
- `POST /executors/search` with 10,000 records returns in **< 100ms** with indexes applied.
- Repeated calls to `get_executors_cached()` within the 1s TTL window do not trigger a DB query.
- Paginated results are consistent (no duplicates, no missing records) with `limit=50&offset=50` etc.
- Alembic migration runs cleanly on a fresh and an existing database.

