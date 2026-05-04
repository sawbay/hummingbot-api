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

### Phase 1 — Foundation (Low-Hanging Fruit + Stability)
Docker event stream, MQTT reconnect, health check, and deploy validation. These have the highest ratio of impact to implementation cost.

### Phase 2 — Performance & UX (Terminal-Grade Responsiveness)
Event bus, WS delta protocol, WS commands, and controller management. These require coordinated refactoring but deliver the latency improvements needed for a real-time trading terminal.

### Phase 3 — Security & Scale
JWT auth, DB optimizations, pagination. Required before a multi-user or public-facing deployment.
