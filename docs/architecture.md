# Architecture Overview

This document describes the internal structure and design patterns of the **hummingbot-api** project.

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
docs/                      Developer documentation
main.py                    FastAPI app, lifespan startup/shutdown
deps.py                    FastAPI dependency injection helpers
config.py                  Pydantic Settings (reads from .env)
```

---

## Core Infrastructure

1. **Docker**: Used to manage the lifecycle of Hummingbot trading bot instances.
2. **EMQX/MQTT**: The primary communication channel between the API and running bots. See [MQTT Communication](mqtt.md).
3. **PostgreSQL**: Persistent storage for bot runs, configurations, and historical data.
4. **Hummingbot SDK**: Used for connector logic and strategy management.

---

## Key Design Patterns

### Dependency Injection
Services are initialized in `main.py`'s `lifespan()` and attached to `app.state.*`.
`deps.py` exposes them as FastAPI `Depends()` callables.

### Bot Lifecycle State Machine
1. **CREATED**: Container created, database record initialized.
2. **RUNNING**: Health check confirms container is up and running.
3. **CONNECTED**: MQTT handshake successful (bot is now controllable).
4. **STOPPED/ARCHIVED**: Container removed, logs saved, state persisted.

### Pending Bots Registry
`BotsOrchestrator.pending_bots` is an in-memory registry that tracks bots from deployment until they are discovered by MQTT. This ensures immediate visibility in the UI with a `deploying` status.

### WebSocket Subscription Pattern
Implemented in `services/executor_ws_manager.py` and `services/websocket_manager.py`. Uses asyncio push loops with hash-based change detection to minimize bandwidth.
Each subscription type (e.g., `bot_deployment`, `performance`) has a dedicated push loop function.

---

## BotRun Model
The `BotRun` DB model tracks every deployment:
- `run_status`: `CREATED`, `RUNNING`, `STOPPED`, `ERROR`
- `deployment_status`: `DEPLOYED`, `FAILED`, `ARCHIVED`
- `error_message`: Captures startup failures and Docker logs.
