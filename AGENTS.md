# AGENTS.md

Guidelines for AI agents (Claude, Cursor, Copilot, etc.) working in this repository.

---

## Project Overview

**hummingbot-api** is a FastAPI backend that manages Hummingbot trading bot instances running
in Docker containers. It exposes:

- **REST API** (`/`) — full CRUD for bots, strategies, accounts, market data, portfolio
- **WebSocket API** (`/ws/market-data`, `/ws/executors`) — real-time push for candles, order books, bot status, executor performance, and deployment tracking

Key infrastructure: Docker (container lifecycle), EMQX/MQTT (bot communication), PostgreSQL (persistence), Hummingbot Python SDK (connector logic).

---

## Repository Layout

```
routers/          HTTP + WebSocket route handlers (one file per domain)
services/         Business logic services
  bots_orchestrator.py     Docker + MQTT bot lifecycle, pending_bots registry
  docker_service.py        Docker SDK wrapper, container health/logs
  executor_ws_manager.py   WebSocket push loops for all /ws/executors subscriptions
  websocket_manager.py     WebSocket push loops for /ws/market-data
models/           Pydantic request/response models
database/
  models.py                SQLAlchemy ORM models
  repositories/            Async repository classes (one per domain)
bots/
  controllers/             Strategy controller configs and implementations
  credentials/             Per-account encrypted credentials (gitignored)
  conf/                    Script and controller YAML configs
docs/                      Developer documentation (see below)
main.py                    FastAPI app, lifespan startup/shutdown
deps.py                    FastAPI dependency injection helpers
config.py                  Pydantic Settings (reads from .env)
```

---

## Key Patterns

### Dependency Injection

Services are initialized in `main.py`'s `lifespan()` and attached to `app.state.*`.
`deps.py` exposes them as FastAPI `Depends()` callables.

```python
# Adding a new service: register in lifespan(), expose in deps.py, inject via Depends()
bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator)
```

### Bot Lifecycle State Machine

```
Docker container created
  → BotRun.run_status = CREATED       (written in deploy endpoint)
  → pending_bots registry entry added (BotsOrchestrator.register_pending_bot)

Background health check (_post_deploy_health_check) polls every 3 s for up to 60 s:
  → container running?  → BotRun.run_status = RUNNING, pending entry removed
  → container crashed?  → BotRun.run_status = ERROR, deployment_status = FAILED,
                           Docker logs saved to BotRun.error_message

MQTT discovers bot:
  → active_bots dict populated, pending entry auto-removed

Bot stopped:
  → BotRun.run_status = STOPPED, stopped_at set

Bot archived (stop-and-archive flow):
  → container removed, BotRun.deployment_status = ARCHIVED
```

### Pending Bots Registry

`BotsOrchestrator.pending_bots` is an in-memory dict that bridges the gap between
"container created" and "MQTT discovered":

```python
# Add on deploy
bots_manager.register_pending_bot(instance_name, {"strategy": "...", "account": "..."})

# Update on failure
bots_manager.mark_pending_bot_failed(instance_name, error_message)

# Remove on success
bots_manager.resolve_pending_bot(instance_name)
```

`get_all_bots_status()` merges `active_bots` + `pending_bots`, so newly deployed bots
appear immediately with `status: "deploying"` in all status endpoints and WebSocket feeds.

### BotRun Database Model

`database/models.py → BotRun` tracks every deployment:

| Column | Values |
|---|---|
| `run_status` | `CREATED`, `RUNNING`, `STOPPED`, `ERROR` |
| `deployment_status` | `DEPLOYED`, `FAILED`, `ARCHIVED` |
| `error_message` | Plain text + Docker logs on failure |

Repository: `database/repositories/bot_run_repository.py`  
Key methods: `create_bot_run`, `update_bot_run_running`, `update_bot_run_failed`,
`update_bot_run_stopped`, `update_bot_run_archived`, `get_deployment_status`

### WebSocket Subscriptions

All `/ws/executors` subscriptions are implemented as asyncio push loops in
`services/executor_ws_manager.py`. Adding a new subscription type requires:

1. Add name to `SUBSCRIPTION_TYPES` set
2. Handle the subscribe message in `handle_subscribe()` (validate params, set `sub.sub_id`)
3. Register push function in `_get_push_fn()` dispatch dict
4. Implement `async def _<name>_push_loop(conn_id, websocket, sub)`

Push loops use `_compute_hash()` for change detection — only send when data changes.

---

## Documentation

| File | Contents |
|---|---|
| [`docs/ws.md`](docs/ws.md) | WebSocket API reference — all subscription types, payloads, auth, and the `bot_deployment` deployment tracking flow |
| [`docs/controllers/`](docs/controllers/) | Strategy controller documentation |
| [`README.md`](README.md) | Quick-start, available commands, service URLs |

---

## Bot Deployment — Key Facts for Agents

When asked to implement or modify bot deployment:

1. **Deploy endpoints** (`POST /bot-orchestration/deploy-v2-controllers` and `deploy-v2-script`):
   - Generate a `unique_instance_name` with `{name}-{YYYYMMDD-HHMMSS}` suffix
   - Call `docker_manager.create_hummingbot_instance()`
   - Call `bots_manager.register_pending_bot()` immediately
   - Create a `BotRun` DB record (`run_status=CREATED`)
   - Fire `_post_deploy_health_check` as a `BackgroundTask`

2. **Health check** (`_post_deploy_health_check` in `routers/bot_orchestration.py`):
   - Polls `docker_service.get_container_health()` every 3 s, up to 60 s
   - On container crash → `update_bot_run_failed()` + `mark_pending_bot_failed()`
   - On running → `update_bot_run_running()` + `resolve_pending_bot()`

3. **Deployment status endpoints**:
   - REST: `GET /bot-orchestration/deployment-status/{instance_name}`
   - WebSocket: subscribe `bot_deployment` on `/ws/executors` (preferred — push-based, auto-terminates)
   - See [`docs/ws.md`](docs/ws.md) for the full WebSocket flow

4. **Container diagnostics**:
   - `docker_service.get_container_health(name)` → `{found, status, running, exit_code, error, logs}`
   - `docker_service.get_container_logs(name, tail=100)` → raw log string
   - Logs are automatically captured and stored in `BotRun.error_message` on failure

---

## Style Guidelines

- **Async**: all DB access is async (`AsyncSession`). Use `async with db_manager.get_session_context()`.
- **Logging**: use module-level `logger = logging.getLogger(__name__)`, not `logging.info()` directly.
- **Error handling**: never let a DB or bot-tracking failure break the main operation (wrap in try/except, log the error).
- **Background tasks**: use FastAPI `BackgroundTasks` for post-deploy work; use `asyncio.create_task` inside services.
- **Pydantic models**: request/response models live in `models/`; DB models live in `database/models.py`.
