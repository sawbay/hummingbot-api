# Bot Orchestration

This document summarizes how `hummingbot-api` prepares durable deployment files and how `rs-orchestrator` assigns those deployments to warm Hummingbot containers.

---

## 1. Runtime File Layout

The API treats each running bot as an isolated Hummingbot runtime directory.

```text
bots/
  credentials/
    <profile>/
      conf_client.yml
      conf_fee_overrides.yml
      hummingbot_logs.yml
      .password_verification
      connectors/
        <connector>.yml

  conf/
    scripts/
      <generated-script-config>.yml
    controllers/
      <controller-config>.yml

  instances/
    <instance_name>/
      conf/
      data/
      logs/

  pools/
    warmbot_1/
      conf/
      data/
      logs/
```

`bots/credentials/<profile>` is the source credential profile. `bots/conf` and `bots/credentials` are durable source-of-truth assets that can be synced through R2. Top-level `bots/controllers`, top-level `bots/scripts`, `.gitignore`, and `.dockerignore` are not uploaded or downloaded by the R2 sync. `bots/instances` is legacy dynamic-runtime state. `bots/pools/warmbot_1`, `warmbot_2`, and `warmbot_3` are warm bot pool directories used by `docker-compose.bot-pool.yml`.

---

## 2. `conf_client.yml`

`conf_client.yml` is the main Hummingbot client configuration file mounted into each bot container at:

```text
/home/hummingbot/conf/conf_client.yml
```

During warm-pool orchestration, `rs-orchestrator` copies the selected credential profile into the chosen pool slot:

```text
bots/credentials/<credentials_profile>
  -> bots/pools/<warmbot_id>/conf
```

After copying, `rs-orchestrator` rewrites:

```yaml
instance_id: <warmbot_id>
```

This matters because MQTT topics use the Hummingbot `instance_id`, for example:

```text
hbot/<instance_id>/status_updates
hbot/<instance_id>/log
hbot/<instance_id>/start
hbot/<instance_id>/stop
```

For the warm pool, each pool bot should have a stable `instance_id`:

```yaml
instance_id: warmbot_1
instance_id: warmbot_2
instance_id: warmbot_3
```

The current local pool config uses local MQTT:

```yaml
mqtt_bridge:
  mqtt_host: localhost
  mqtt_port: 1883
  mqtt_username: ''
  mqtt_password: ''
  mqtt_namespace: hbot
  mqtt_ssl: false
  mqtt_autostart: true
```

The pool bots also use local SQLite:

```yaml
db_mode:
  db_engine: sqlite
```

---

## 3. Connectors And Credentials

Connector credentials live under:

```text
bots/credentials/<profile>/connectors/
```

Example:

```text
bots/credentials/master_account/connectors/binance_perpetual_testnet.yml
```

During warm-pool deployment, the full credential profile is copied into the selected warm bot:

```text
bots/pools/<warmbot_id>/conf/
```

That means each deployed bot receives its own copy of:

```text
conf_client.yml
conf_fee_overrides.yml
hummingbot_logs.yml
.password_verification
connectors/
```

The container receives `CONFIG_PASSWORD` from the API environment. This unlocks encrypted Hummingbot credentials in headless mode:

```text
CONFIG_PASSWORD=<value from .env>
```

For pool bots, credentials are already materialized inside:

```text
bots/pools/warmbot_1/conf/
bots/pools/warmbot_2/conf/
bots/pools/warmbot_3/conf/
```

Each pool bot should keep its own `conf_client.yml` and connector credential files. Sharing a full `conf` directory across bots is unsafe because it causes `instance_id`, logs, SQLite DBs, and mutable config state to collide.

---

## 4. MQTT Orchestration Handoff

Warm-pool deployment is the primary orchestration path for:

```text
POST /bot-orchestration/deploy-v2-controllers
POST /bot-orchestration/deploy-v2-script
```

The Python API does not create Hummingbot Docker containers in this flow. It prepares durable files, creates the deployment intent, and publishes a handoff message. `rs-orchestrator` owns warm-slot selection, R2 hydration, file materialization into the selected pool slot, and MQTT strategy start.

### Controller Deployment

For `deploy-v2-controllers`, the API:

1. Validates the deployment request.
2. Generates a unique timestamped `instance_name`.
3. Generates a script config under `bots/conf/scripts/`.
4. Adds the requested controller config names into that script config.
5. Writes files locally, triggering R2 write-through when R2 is enabled.
6. Registers the deployment in the in-memory pending registry.
7. Creates a `BotRun` database record with `run_status=CREATED`.
8. Publishes an orchestration request to `hbot/orchestrate`.
9. Returns immediately with `orchestration_status: queued`.

Generated script config shape:

```yaml
script_file_name: v2_with_controllers.py
controllers_config:
  - my-controller.yml
```

### Script Deployment

For `deploy-v2-script`, the API:

1. Validates the deployment request.
2. Generates a unique timestamped `instance_name`.
3. Reads the script config if one is supplied, including referenced controllers.
4. Registers the deployment in the pending registry.
5. Creates a `BotRun` database record with `run_status=CREATED`.
6. Publishes an orchestration request to `hbot/orchestrate`.
7. Returns immediately with `orchestration_status: queued`.

---

## 5. Handoff Message Shape

The API publishes JSON to:

```
hbot/orchestrate
```

Example payload:

```json
{
  "request_id": "orch-my-strategy-20260516-120000",
  "instance_name": "my-strategy-20260516-120000",
  "strategy_type": "controller",
  "strategy_name": "v2_with_controllers",
  "credentials_profile": "master_account",
  "script_config": "my-strategy-20260516-120000.yml",
  "controllers_config": ["usdc-usdt-recurring-buy.yml"],
  "r2": {
    "prefix": "bots",
    "keys": {
      "credential_profile": "bots/credentials/master_account/",
      "script_config": "bots/conf/scripts/my-strategy-20260516-120000.yml",
      "controllers": ["bots/conf/controllers/usdc-usdt-recurring-buy.yml"],
      "scripts_runtime": null,
      "controllers_runtime": null
    }
  },
  "deployment_config": {
    "instance_name": "my-strategy-20260516-120000",
    "credentials_profile": "master_account"
  }
}
```

`instance_name` is the logical deployment/run identity. The selected warm bot is reported later as `bot_name`.

`rs-orchestrator` publishes progress to:

```text
hbot/orchestrate/status
```

Example status:

```json
{
  "request_id": "orch-my-strategy-20260516-120000",
  "instance_name": "my-strategy-20260516-120000",
  "bot_name": "warmbot_1",
  "status": "reserved",
  "error": null
}
```

Status values:

```text
reserved
hydrating
configuring
running
failed
```

On `running`, the API updates the latest `BotRun` from the temporary logical `instance_name` mapping to the selected warm slot, for example `bot_name=warmbot_1`.

---

## 6. Rust Warm-Pool Runtime Steps

For every valid message on `hbot/orchestrate`, `rs-orchestrator`:

1. Parses and validates the payload.
2. Reserves one in-memory idle slot, for example `warmbot_1`.
3. Publishes `reserved` to `hbot/orchestrate/status`.
4. Downloads requested R2 keys into local `bots/` when R2 is enabled.
5. Copies the selected credential profile into `bots/pools/<warmbot_id>/conf`.
6. Rewrites `conf_client.yml` so `instance_id` matches the warm bot id.
7. Copies the script config into `bots/pools/<warmbot_id>/conf/scripts/`.
8. Copies referenced controllers into `bots/pools/<warmbot_id>/conf/controllers/`.
9. Publishes `configuring`.
10. Sends `import` to `hbot/<warmbot_id>/import`.
11. Sends `start` to `hbot/<warmbot_id>/start`.
12. Waits for `strategy/running`.
13. Marks the slot `running` and publishes `running`.

On failure, `rs-orchestrator` marks the slot `error` and publishes:

```json
{
  "instance_name": "my-strategy-20260516-120000",
  "bot_name": "warmbot_1",
  "status": "failed",
  "error": "controller config not found"
}
```

---

## 7. Warm Bot Pool Flow

The warm pool is different from dynamic creation. Instead of creating a new Docker container for every strategy deployment, a fixed set of Hummingbot containers is started ahead of time and kept connected to MQTT.

The pool is started by:

```text
docker-compose.bot-pool.yml
```

Each pool bot has a stable identity and runtime directory:

```text
bots/pools/warmbot_1/
bots/pools/warmbot_2/
bots/pools/warmbot_3/
```

Each service mounts:

```text
bots/pools/<bot_id>/conf -> /home/hummingbot/conf
bots/pools/<bot_id>/data -> /home/hummingbot/data
bots/pools/<bot_id>/logs -> /home/hummingbot/logs
```

Each bot's Docker container name should match its Hummingbot `instance_id`:

```text
container_name: warmbot_1
conf_client.yml: instance_id: warmbot_1
```

That keeps Docker discovery, MQTT topics, and API state aligned.

### Pool Bot Baseline State

A warm pool bot should boot with only the baseline Hummingbot runtime config:

```text
conf/
  conf_client.yml
  conf_fee_overrides.yml
  hummingbot_logs.yml
  .password_verification
  connectors/
data/
logs/
```

The pool bot should not start with a controller or script config already assigned. These directories can exist but should be empty while the slot is idle:

```text
conf/controllers/
conf/scripts/
```

The bot starts in headless mode:

```text
HEADLESS_MODE=true
```

The bot also needs:

```text
CONFIG_PASSWORD=<same password used for .password_verification and encrypted connectors>
```

In the current local setup, pool bots use local MQTT:

```yaml
mqtt_bridge:
  mqtt_host: localhost
  mqtt_port: 1883
  mqtt_username: ''
  mqtt_password: ''
  mqtt_ssl: false
  mqtt_autostart: true
```

and local SQLite:

```yaml
db_mode:
  db_engine: sqlite
```

### Slot State Model

`rs-orchestrator` tracks a pool slot separately from a historical `BotRun`.

Recommended slot states:

```text
offline
bootstrapping
idle
reserved
configuring
running
stopping
cleanup
error
```

State meanings:

| State | Meaning |
|---|---|
| `offline` | No recent MQTT heartbeat from the bot. |
| `bootstrapping` | Bot process is starting and connecting to MQTT. |
| `idle` | Bot is connected and available for assignment. |
| `reserved` | API selected this bot for a deployment but has not started config import yet. |
| `configuring` | API is copying/sending strategy config and importing it into the bot. |
| `running` | Strategy or controllers are active. |
| `stopping` | API has sent stop command and is waiting for confirmation. |
| `cleanup` | API is clearing assigned script/controller config and transient state. |
| `error` | Bot failed startup, config import, strategy start, or stop/cleanup. |

The current implementation keeps this state in memory and rebuilds it from MQTT heartbeats/status, Docker state, filesystem state, and `bot_runs`. No `bot_slots` table is used.

In-memory shape:

```text
- bot_name
- status
- assigned_run_id
- account_name
- last_heartbeat
- current_config_name
- current_controller_ids
- last_error
- updated_at
```

### Startup Flow

1. Docker Compose starts `warmbot_1`, `warmbot_2`, and `warmbot_3`.
2. Each bot unlocks encrypted config using `CONFIG_PASSWORD`.
3. Each bot loads `conf_client.yml`.
4. Headless mode forces or requires MQTT autostart.
5. Each bot connects to local EMQX before starting any strategy.
6. Each bot publishes lifecycle status:

```json
{"type": "bootstrap", "msg": "bootstrapping"}
{"type": "bootstrap", "msg": "mqtt_connected"}
{"type": "strategy", "msg": "idle"}
```

7. `rs-orchestrator` discovers the bot from MQTT heartbeat and `status_updates`.
8. If no strategy is assigned, `rs-orchestrator` marks the slot `idle`.

### Assignment Flow

When a user requests a strategy deployment, the API should not create a new container. Instead:

1. The API validates the deployment request.
2. The API writes durable config locally and to R2.
3. The API creates a queued `BotRun`.
4. The API publishes to `hbot/orchestrate`.
5. `rs-orchestrator` finds an `idle` slot.
6. `rs-orchestrator` atomically reserves the slot:

```text
idle -> reserved
```

7. `rs-orchestrator` hydrates the requested R2 files.
8. `rs-orchestrator` materializes credentials/script/controller config into the selected bot.
9. `rs-orchestrator` imports the script/controller config over MQTT.
10. `rs-orchestrator` starts the strategy over MQTT.
11. `rs-orchestrator` waits for `status_updates` confirming the strategy is running.
12. `rs-orchestrator` marks the slot:

```text
configuring -> running
```

### Config Delivery

There are two possible config delivery models.

#### Current R2 Filesystem-Compatible Model

This model keeps local filesystem compatibility for Hummingbot while using R2 as the durable exchange between the Python API and `rs-orchestrator`.

R2 sync intentionally excludes top-level `bots/controllers`, top-level `bots/scripts`, `.gitignore`, and `.dockerignore`. Deployment-specific controller and script config YAML files under `bots/conf/controllers` and `bots/conf/scripts` are still synced.

The API's source-of-truth config directories are:

```text
bots/conf/scripts/
bots/conf/controllers/
```

For V2 controllers, the API either receives controller names from the deployment request or receives a script config name. The important files are:

```text
bots/conf/scripts/<script_config>.yml
bots/conf/controllers/<controller_config>.yml
```

Example source controller:

```text
bots/conf/controllers/usdc-usdt-recurring-buy.yml
```

Example source content:

```yaml
amount_quote: 6
connector_name: binance_perpetual_testnet
controller_name: examples.basic_order_example
controller_type: generic
id: usdc-usdt-recurring-buy
leverage: 1
manual_kill_switch: false
order_frequency: 60
position_mode: ONEWAY
side: 1
trading_pair: APT-USDT
```

For controller deployment, the API generates a script config into:

```text
bots/conf/scripts/<instance_name>.yml
```

with content like:

```yaml
script_file_name: v2_with_controllers.py
controllers_config:
  - usdc-usdt-recurring-buy.yml
```

The API write-through sync uploads durable files to R2:

```text
bots/conf/scripts/<instance_name>.yml
  -> R2 bots/conf/scripts/<instance_name>.yml

bots/conf/controllers/usdc-usdt-recurring-buy.yml
  -> R2 bots/conf/controllers/usdc-usdt-recurring-buy.yml
```

`rs-orchestrator` downloads the requested R2 keys and copies the source files into the selected pool slot:

```text
bots/pools/<bot_id>/conf/scripts/<run_config>.yml
bots/pools/<bot_id>/conf/controllers/<controller>.yml
```

Concrete example for `warmbot_1`:

```text
bots/conf/scripts/warmbot_1-run-001.yml
  -> bots/pools/warmbot_1/conf/scripts/warmbot_1-run-001.yml

bots/conf/controllers/usdc-usdt-recurring-buy.yml
  -> bots/pools/warmbot_1/conf/controllers/usdc-usdt-recurring-buy.yml
```

The deployed script config visible to the bot at runtime is:

```text
/home/hummingbot/conf/scripts/warmbot_1-run-001.yml
```

The deployed controller config visible to the bot at runtime is:

```text
/home/hummingbot/conf/controllers/usdc-usdt-recurring-buy.yml
```

After the files are copied, `rs-orchestrator` sends an MQTT command to import or load the script config.

The exact command topic should follow the existing Hummingbot command convention:

```text
hbot/<bot_id>/import
```

Example:

```json
{
  "request_id": "deploy-warmbot_1-run-001-import",
  "script": "v2_with_controllers.py",
  "conf": "warmbot_1-run-001.yml"
}
```

Then `rs-orchestrator` starts the strategy:

```text
hbot/<bot_id>/start
```

Example:

```json
{
  "request_id": "deploy-warmbot_1-run-001-start",
  "log_level": "INFO",
  "script": "v2_with_controllers.py",
  "conf": "warmbot_1-run-001.yml",
  "is_quickstart": true,
  "async_backend": true
}
```

The bot should respond on an API response topic or publish status transitions on:

```text
hbot/warmbot_1/status_updates
hbot/warmbot_1/log
hbot/warmbot_1/notify
```

Expected status sequence:

```json
{"type": "strategy", "msg": "loading"}
{"type": "strategy", "msg": "running"}
```

If the bot fails to load the copied YAML, it should publish:

```json
{
  "type": "strategy",
  "msg": "failed",
  "data": {
    "stage": "load_config",
    "script_config": "warmbot_1-run-001.yml",
    "error": "controller config usdc-usdt-recurring-buy.yml not found"
  }
}
```

#### Warm Pool Config Assignment Steps

For a warm bot assignment, the system performs these steps:

1. The API creates a run-specific script config name, for example:

```text
warmbot_1-run-001.yml
```

2. The API writes the script config into the API source directory:

```text
bots/conf/scripts/warmbot_1-run-001.yml
```

3. The API ensures every referenced controller exists under:

```text
bots/conf/controllers/
```

4. The API publishes the deployment request to `hbot/orchestrate`.
5. `rs-orchestrator` selects an idle bot, for example `warmbot_1`.
6. `rs-orchestrator` downloads the requested files from R2.
7. `rs-orchestrator` copies the script config into the selected bot:

```text
bots/pools/warmbot_1/conf/scripts/warmbot_1-run-001.yml
```

8. `rs-orchestrator` copies each referenced controller into the selected bot:

```text
bots/pools/warmbot_1/conf/controllers/usdc-usdt-recurring-buy.yml
```

9. `rs-orchestrator` sends `import` over MQTT.
10. `rs-orchestrator` waits for an import acknowledgement or failure status.
11. `rs-orchestrator` sends `start` over MQTT.
12. `rs-orchestrator` waits for `strategy/running` status.
13. `rs-orchestrator` marks the slot `running` and publishes status.
14. The API receives `hbot/orchestrate/status` and updates the `BotRun`.

This keeps the API's source-of-truth configs separate from the bot's runtime copy.

#### Future MQTT-Native Model

The MQTT-native model skips host file copying and sends the YAML content directly to the bot:

```json
{
  "command": "configure_strategy",
  "params": {
    "script_file_name": "v2_with_controllers.py",
    "script_config": {
      "controllers_config": ["controller-a.yml"]
    },
    "controllers": {
      "controller-a.yml": {
        "id": "controller-a",
        "controller_name": "..."
      }
    }
  }
}
```

The bot then writes the files into its own mounted config directory:

```text
/home/hummingbot/conf/scripts/<run_config>.yml
/home/hummingbot/conf/controllers/<controller>.yml
```

and returns an acknowledgement:

```json
{
  "request_id": "deploy-warmbot_1-run-001-configure",
  "success": true,
  "written_files": [
    "conf/scripts/warmbot_1-run-001.yml",
    "conf/controllers/usdc-usdt-recurring-buy.yml"
  ]
}
```

The filesystem-compatible model is easier to implement with current Hummingbot behavior. The MQTT-native model is better long term because the bot can validate and acknowledge each config update without relying on shared host files.

### MQTT Command Flow

A deployment should use request/response IDs so the API can distinguish "command sent" from "command completed".

Example high-level sequence:

```text
API -> hbot/warmbot_1/import
warmbot_1 -> hummingbot-api/response/<request_id>

API -> hbot/warmbot_1/start
warmbot_1 -> hbot/warmbot_1/status_updates
warmbot_1 -> hummingbot-api/response/<request_id>
```

Expected status updates:

```json
{"type": "strategy", "msg": "loading"}
{"type": "strategy", "msg": "running"}
```

Failure updates should include enough context for the UI:

```json
{
  "type": "strategy",
  "msg": "failed",
  "data": {
    "stage": "import",
    "error": "controller config not found"
  }
}
```

### Stop And Release Flow

When the user stops a strategy:

1. `rs-orchestrator` marks the slot `stopping`.
2. `rs-orchestrator` sends stop command over MQTT.
3. Bot stops the strategy and cancels orders according to the request options.
4. Bot publishes:

```json
{"type": "strategy", "msg": "stopped"}
```

5. `rs-orchestrator` marks the latest matching `BotRun` as stopped.
6. Logs/data are preserved according to policy.
7. `rs-orchestrator` removes assigned script/controller config from the pool bot directory.
8. `rs-orchestrator` verifies no strategy is running.
9. `rs-orchestrator` marks the slot `idle`.

The bot container remains running throughout the process.

### Cleanup Rules

Before returning a slot to `idle`, clean up strategy-specific files:

```text
conf/scripts/<assigned-script-config>.yml
conf/controllers/<assigned-controller-config>.yml
```

Do not delete baseline files:

```text
conf_client.yml
conf_fee_overrides.yml
hummingbot_logs.yml
.password_verification
connectors/
```

Logs may be cleared only after archiving or after the user explicitly accepts losing them.

### Recovery Rules

`rs-orchestrator` should reconcile slot state from durable data plus MQTT status.

On `rs-orchestrator` restart:

1. Load known slots from `POOL_BOTS`.
2. Subscribe to `hbot/+/hb` and `hbot/+/status_updates`.
3. Mark slots with recent heartbeat as connected.
4. Query or infer whether each bot is running a strategy.
5. Rebuild slot state:

```text
heartbeat + no strategy -> idle
heartbeat + active strategy -> running
no heartbeat -> offline
```

On bot restart:

1. Bot reconnects to MQTT.
2. Bot publishes `bootstrapping`.
3. Bot reports whether a strategy is loaded/running.
4. API either resumes the `BotRun` or marks it failed, depending on strategy state.

On MQTT outage:

1. Slots become stale after the heartbeat timeout.
2. `rs-orchestrator` marks them `offline` or `unknown`.
3. When MQTT recovers, bot lifecycle messages restore the slot state.

### Why Use Warm Bots

Warm bots trade dynamic container startup cost for persistent runtime capacity.

Benefits:

- Faster strategy start because Docker startup already happened.
- Early MQTT visibility because the bot is connected before assignment.
- Fewer startup failures during user deployment.
- A simpler path toward distributed workers.
- Clear separation between "bot runtime exists" and "strategy is assigned".

Tradeoffs:

- Idle containers consume CPU and memory.
- Each slot needs clean state management.
- The API needs slot reservation logic.
- Config drift must be detected and corrected.
- A stuck or dirty bot must be quarantined instead of reused.

The intended future pool flow is:

1. Start `warmbot_1`, `warmbot_2`, and `warmbot_3` as idle headless bots.
2. Each bot connects to MQTT and publishes lifecycle status.
3. `rs-orchestrator` discovers connected idle bots.
4. The API queues a deployment request instead of creating a new container.
5. `rs-orchestrator` selects an idle bot and sends config/import/start commands over MQTT.
6. When stopped, the bot is cleaned and returned to the idle pool.
