# AGENTS.md

Guidelines for agents working inside `rs-orchestrator/`.

## Scope

This crate is the Rust warm-pool sidecar for `hummingbot-api`.

It owns:

- In-memory warmbot slot state.
- MQTT subscription to `orchestrate/deploy`.
- MQTT status publishing to `orchestrate/status`.
- R2 hydration of requested durable deployment files.
- Copying credentials, script configs, and controller configs into `bots/pools/<warmbot_id>/conf`.
- Starting and stopping already-running warmbot containers over MQTT.
- Publishing runtime status so `hummingbot-api` can update durable deployment state.

It does not own:

- Python API request validation.
- Dynamic Hummingbot container creation.
- R2 write-through from API writes.
- Direct Postgres access or `bot_runs` writes.
- Database migrations for `bot_slots`.

## Architecture Rules

- Keep warmbot names aligned across config, compose, MQTT, and filesystem paths: `warmbot_1`, `warmbot_2`, `warmbot_3`.
- Do not reintroduce `bot_1`, `bot_2`, or `bot_3` as default pool names.
- Do not add a durable `bot_slots` table.
- Do not add direct DB access back into this crate. `hummingbot-api` owns `bot_runs`; this sidecar owns runtime slot state only.
- Slot state is intentionally in memory and rebuilt from MQTT heartbeats/status plus runtime filesystem/container observations.
- Treat `rs-orchestrator/bots/` as the sidecar runtime tree when using the moved compose file.
- Keep runtime-heavy data local. Do not upload or sync `bots/pools`, `bots/instances`, logs, data, SQLite files, or archives to R2.
- Use R2 only to hydrate requested durable files from `credentials/` and `conf/`.

## MQTT Rules

- Primary handoff topic from Python API: `orchestrate/deploy`.
- Status callback topic to Python API: `orchestrate/status`.
- Hummingbot command topics use `hbot/<warmbot_id>/<command>`.
- For V2 controller deployments, start with `hbot/<warmbot_id>/start` and payload field `v2_conf`.
- Do not call `hbot/<warmbot_id>/import` for V2 controller deployment. The current import path is V1-style and not headless-safe.
- Command replies should use `hummingbot-api/response/<request_id>`.

## Docker Compose Rules

- `rs-orchestrator/docker-compose.yml` is intended to be run from the repository root with:

```bash
docker compose -f docker-compose.yml -f rs-orchestrator/docker-compose.yml up --build
```

- Compose paths in that file are root-relative by design.
- Mount the sidecar runtime bots folder as `./rs-orchestrator/bots:/app/bots`.
- Warmbot services should mount `./rs-orchestrator/bots/pools/<warmbot_id>/...`.
- Warmbot services should read the root `.env` because Hummingbot needs `CONFIG_PASSWORD`.
- The Rust sidecar should read `./rs-orchestrator/.env`.

## Coding Guidelines

- Use `tokio` async patterns consistently.
- Keep shared state behind the existing async mutex types in `slot_store`.
- Prefer structured `tracing` logs with request IDs, instance names, and bot names.
- Preserve existing error propagation with `anyhow` internally and `AppError` at HTTP boundaries.
- Do not panic for recoverable MQTT, R2, filesystem, Docker, or database failures.
- When adding file operations, keep cleanup allowlists conservative and never delete baseline Hummingbot config files.

## Verification

Before handing off Rust changes, run:

```bash
cargo fmt
cargo test --offline -- --nocapture
```

For compose changes, validate from the repository root:

```bash
docker compose -f docker-compose.yml -f rs-orchestrator/docker-compose.yml config --quiet
```
