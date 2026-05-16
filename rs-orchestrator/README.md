# Rust Orchestrator

Warm-pool orchestration sidecar for `hummingbot-api`.

The service owns fixed pool bots (`warmbot_1`, `warmbot_2`, `warmbot_3` by default), assigns controller deployments to idle bots, copies runtime config into `bots/pools/<bot>/conf`, and controls Hummingbot through MQTT.

It does not create dynamic bot containers and does not add a `bot_slots` table. Slot state is in memory and rebuilt from MQTT, Docker, filesystem state, and `bot_runs`.

## Run Locally

```bash
cd rs-orchestrator
cargo run
```

Default port: `8001`.

Important environment variables:

```bash
DATABASE_URL=postgresql://hbot:hummingbot-api@localhost:5432/hummingbot_api
BROKER_HOST=localhost
BROKER_PORT=1883
BOTS_PATH=..
POOL_BOTS=warmbot_1,warmbot_2,warmbot_3
```

## Run With Docker Compose

```bash
cd rs-orchestrator
docker compose up --build
```

This starts the Rust sidecar, EMQX, and PostgreSQL. It mounts the repository `bots/` directory into the sidecar at `/app/bots` and exposes the API at http://localhost:8001.

Docker Compose reads sidecar settings from `rs-orchestrator/.env`.

## HTTP APIs

Base URL:

```text
http://localhost:8001
```

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Service health and MQTT connection summary. |
| `GET` | `/bot-orchestration/pool/slots` | List all in-memory warm-pool slots. |
| `GET` | `/bot-orchestration/pool/slots/{bot_name}` | Get one warm-pool slot, for example `warmbot_1`. |
| `POST` | `/bot-orchestration/deploy-v2-controllers` | Assign a V2 controller deployment to an idle warm-pool bot. |
| `POST` | `/bot-orchestration/stop-bot` | Stop a running strategy and release the warm-pool slot back to idle. |
| `GET` | `/bot-orchestration/deployment-status/{instance_name}` | Get deployment state from slot memory, Docker, and `bot_runs`. |
| `GET` | `/bot-orchestration/bot-runs?limit=100&offset=0` | List recent `bot_runs` rows. |

Deploy a V2 controller strategy:

```bash
curl -X POST http://localhost:8001/bot-orchestration/deploy-v2-controllers \
  -H 'Content-Type: application/json' \
  -d '{
    "instance_name": "warm-grid",
    "credentials_profile": "master_account",
    "controllers_config": ["usdc-usdt-recurring-buy"],
    "headless": true
  }'
```

Stop and release a warm-pool bot:

```bash
curl -X POST http://localhost:8001/bot-orchestration/stop-bot \
  -H 'Content-Type: application/json' \
  -d '{
    "bot_name": "warmbot_1",
    "skip_order_cancellation": false,
    "async_backend": true
  }'
```
