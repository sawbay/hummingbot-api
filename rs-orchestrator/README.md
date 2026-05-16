# rs-orchestrator

Rust warm-pool orchestration sidecar for `hummingbot-api`.

`hummingbot-api` prepares durable deployment files, writes them to R2, records the `BotRun`, and publishes an orchestration request to MQTT. `rs-orchestrator` receives that request, hydrates the required files into its local `bots/` tree, assigns an idle warm bot, copies config into that bot's pool directory, and starts the strategy over MQTT.

It does not create Hummingbot containers and it does not connect to Postgres. Deployment persistence belongs to `hummingbot-api`; the sidecar reports runtime progress through MQTT status events.

## Runtime Layout

The sidecar owns a local runtime tree under this folder:

```text
rs-orchestrator/
  bots/
    credentials/
    conf/
      scripts/
      controllers/
    pools/
      warmbot_1/
      warmbot_2/
```

Important mounts:

```yaml
./rs-orchestrator/bots:/app/bots
./rs-orchestrator/bots/pools/warmbot_1/conf:/home/hummingbot/conf
./rs-orchestrator/bots/pools/warmbot_1/data:/home/hummingbot/data
./rs-orchestrator/bots/pools/warmbot_1/logs:/home/hummingbot/logs
```

The Rust code expects `BOTS_PATH=/app`, so paths resolve as `/app/bots/...` inside the sidecar container.

## MQTT Flow

Primary command topic:

```text
orchestrate/deploy
```

Status callback topic:

```text
orchestrate/status
```

For V2 controller deployments, `rs-orchestrator` starts the selected warm bot with:

```text
hbot/<warmbot_id>/start
```

Payload:

```json
{
  "log_level": "INFO",
  "v2_conf": "<generated-script-config>.yml",
  "is_quickstart": true,
  "async_backend": true
}
```

Do not use `hbot/<warmbot_id>/import` for this path. The current Hummingbot import command is a V1-style CLI path and is not headless-safe for V2 controller configs.

## Configuration

Settings are read from `rs-orchestrator/.env`.

Required variables:

```bash
RS_ORCHESTRATOR_PORT=8001
BROKER_HOST=<mqtt-host>
BROKER_PORT=1883
BOTS_PATH=/app
POOL_BOTS=warmbot_1,warmbot_2,warmbot_3
```

R2 variables are required when R2 hydration is enabled:

```bash
R2_ENABLED=true
R2_BUCKET=<bucket>
R2_ENDPOINT_URL=https://<account-id>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=<access-key>
R2_SECRET_ACCESS_KEY=<secret-key>
R2_PREFIX=bots
```

Warmbot containers read the repository root `.env`, not `rs-orchestrator/.env`, because Hummingbot needs runtime settings such as `CONFIG_PASSWORD`.

## Docker Compose

Run the root API stack plus the warm-pool stack from the repository root:

```bash
docker compose -f docker-compose.yml -f rs-orchestrator/docker-compose.yml up --build
```

The compose file is written with root-relative paths because it is intended to be used with the root compose file. For example:

```yaml
- ./rs-orchestrator/bots:/app/bots
```

Using `./bots` here would mount the repository root `bots/` folder when the command is run from the repo root.

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
| `POST` | `/bot-orchestration/deploy-v2-controllers` | Manual test endpoint for assigning a V2 controller deployment to an idle warm bot. |
| `POST` | `/bot-orchestration/stop-bot` | Stop a running strategy and release the warm bot back to idle. |
| `GET` | `/bot-orchestration/deployment-status/{instance_name}` | Get in-memory deployment state for an instance currently known to the sidecar. |

Production deployments should normally enter through `hummingbot-api`, which publishes to `orchestrate/deploy`.

## Development

Run locally:

```bash
cd rs-orchestrator
cargo run
```

Format and test:

```bash
cargo fmt
cargo test --offline -- --nocapture
```

If running locally outside Docker, set `BOTS_PATH=.` or `BOTS_PATH=<repo>/rs-orchestrator` so the code can find `bots/`.
