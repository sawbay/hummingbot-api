# AGENTS.md

Guidelines for AI agents working in the **hummingbot-api** repository.

---

## 🏗️ Architecture & Layout
See [`docs/architecture.md`](docs/architecture.md) for a detailed breakdown of the system design, repository layout, and bot lifecycle state machine.

---

## 📚 Essential Documentation

| File | Contents |
|---|---|
| [`docs/ws.md`](docs/ws.md) | WebSocket API reference and the `bot_deployment` flow. |
| [`docs/backtesting.md`](docs/backtesting.md) | Backtesting implementation and API reference. |
| [`docs/test/bot_deployment_manual_test.md`](docs/test/bot_deployment_manual_test.md) | Manual test plan for bot deployment verification. |
| [`README.md`](README.md) | Quick-start, service URLs, and available commands. |

---

## 🚀 Bot Deployment Cheat Sheet
When implementing or modifying bot deployment:

1. **Deploy Endpoints**: Must generate a unique name, create the container, register it as `pending`, and fire the `_post_deploy_health_check` background task.
2. **Health Check**: Polls container status for 60s. Updates `BotRun` and pending registry on success/failure.
3. **Diagnostics**: Container logs are captured automatically on failure and stored in `BotRun.error_message`.

---

## 🛠️ Style Guidelines

- **Async**: Use `async with db_manager.get_session_context()` for all DB access.
- **Logging**: Use `logger = logging.getLogger(__name__)`.
- **Safety**: Wrap DB and MQTT updates in try/except blocks to prevent main loop failures.
- **Pydantic**: Request/Response models in `models/`; DB models in `database/models.py`.

---

## 🤖 Rule #1 for Agents
**Never deploy a bot without triggering the health check.** If you don't, the bot will stay in `CREATED` status even if it crashes, becoming a "ghost" instance.
