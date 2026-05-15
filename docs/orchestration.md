# Bot Orchestration

This document summarizes how `hummingbot-api` prepares Hummingbot runtime files, credentials, and containers during bot orchestration.

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
    bot_1/
      conf/
      data/
      logs/
```

`bots/credentials/<profile>` is the source credential profile. `bots/instances/<instance_name>` is the per-deployment runtime copy created by the API. `bots/pools/bot_1`, `bot_2`, and `bot_3` are warm bot pool directories used by `docker-compose.bot-pool.yml`.

---

## 2. `conf_client.yml`

`conf_client.yml` is the main Hummingbot client configuration file mounted into each bot container at:

```text
/home/hummingbot/conf/conf_client.yml
```

During dynamic orchestration, `DockerService.create_hummingbot_instance()` copies the selected credential profile into the new instance directory:

```text
bots/credentials/<credentials_profile>
  -> bots/instances/<instance_name>/conf
```

After copying, the API rewrites:

```yaml
instance_id: <instance_name>
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
instance_id: bot_1
instance_id: bot_2
instance_id: bot_3
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

During dynamic deployment, the full credential profile is copied into:

```text
bots/instances/<instance_name>/conf/
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
bots/pools/bot_1/conf/
bots/pools/bot_2/conf/
bots/pools/bot_3/conf/
```

Each pool bot should keep its own `conf_client.yml` and connector credential files. Sharing a full `conf` directory across bots is unsafe because it causes `instance_id`, logs, SQLite DBs, and mutable config state to collide.

---

## 4. Dynamic Bot Creation Flow

Dynamic creation is used by:

```text
POST /bot-orchestration/deploy-v2-controllers
POST /bot-orchestration/deploy-v2-script
```

### Controller Deployment

For `deploy-v2-controllers`, the API:

1. Validates the deployment request.
2. Generates a unique timestamped `instance_name`.
3. Generates a script config under `bots/conf/scripts/`.
4. Adds the requested controller config names into that script config.
5. Calls `DockerService.create_hummingbot_instance()`.
6. Registers the bot in the in-memory pending registry.
7. Creates a `BotRun` database record.
8. Starts `_post_deploy_health_check`.

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
3. Calls `DockerService.create_hummingbot_instance()`.
4. Registers the bot in the pending registry.
5. Creates a `BotRun` database record.
6. Starts `_post_deploy_health_check`.

---

## 5. `create_hummingbot_instance()` Steps

`DockerService.create_hummingbot_instance()` performs the filesystem and Docker work:

1. Creates:

```text
bots/instances/<instance_name>/
bots/instances/<instance_name>/data/
bots/instances/<instance_name>/logs/
```

2. Copies the credential profile:

```text
bots/credentials/<profile>
  -> bots/instances/<instance_name>/conf
```

3. If a script config is present, copies it into:

```text
bots/instances/<instance_name>/conf/scripts/
```

4. Reads the script config and copies referenced controllers into:

```text
bots/instances/<instance_name>/conf/controllers/
```

5. Rewrites `conf_client.yml`:

```yaml
instance_id: <instance_name>
```

6. Builds Docker volume mounts:

```text
bots/instances/<instance_name>/conf        -> /home/hummingbot/conf
bots/instances/<instance_name>/conf/scripts -> /home/hummingbot/conf/scripts
bots/instances/<instance_name>/conf/controllers -> /home/hummingbot/conf/controllers
bots/instances/<instance_name>/data        -> /home/hummingbot/data
bots/instances/<instance_name>/logs        -> /home/hummingbot/logs
bots/scripts                              -> /home/hummingbot/scripts
bots/controllers                          -> /home/hummingbot/controllers
```

7. Sets container environment:

```text
CONFIG_PASSWORD=<settings.security.config_password>
SCRIPT_CONFIG=<script config filename, if provided>
HEADLESS_MODE=true, if deployment.headless is true
```

8. Starts the Hummingbot container with Docker.

The current container uses host networking:

```text
network_mode="host"
```

---

## 6. Post-Deploy Health Check

Every successful deploy must trigger `_post_deploy_health_check`.

The health check polls the container for the startup window and:

- Marks the bot failed if the container disappears.
- Captures Docker logs if the container exits with a non-zero code.
- Updates the `BotRun` record to `RUNNING` when the container is healthy.
- Updates the pending bot registry so the UI does not leave the bot stuck in `deploying`.

Do not deploy a bot without this health check. Otherwise a crashed startup can remain visible as a created or pending bot.

---

## 7. Warm Bot Pool Flow

The warm pool is different from dynamic creation. Instead of creating a new Docker container for every strategy deployment, a fixed set of Hummingbot containers is started ahead of time and kept connected to MQTT.

The pool is started by:

```text
docker-compose.bot-pool.yml
```

Each pool bot has a stable identity and runtime directory:

```text
bots/pools/bot_1/
bots/pools/bot_2/
bots/pools/bot_3/
```

Each service mounts:

```text
bots/pools/<bot_id>/conf -> /home/hummingbot/conf
bots/pools/<bot_id>/data -> /home/hummingbot/data
bots/pools/<bot_id>/logs -> /home/hummingbot/logs
```

Each bot's Docker container name should match its Hummingbot `instance_id`:

```text
container_name: bot_1
conf_client.yml: instance_id: bot_1
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

The API should track a pool slot separately from a historical `BotRun`.

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

The API should persist this in a future `bot_slots` table rather than relying only on memory.

Example shape:

```text
bot_slots
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

1. Docker Compose starts `bot_1`, `bot_2`, and `bot_3`.
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

7. The API discovers the bot from MQTT heartbeat and `status_updates`.
8. If no strategy is assigned, the API marks the slot `idle`.

### Assignment Flow

When a user requests a strategy deployment, the API should not create a new container. Instead:

1. Validate the deployment request.
2. Find an `idle` slot compatible with the requested account/profile.
3. Atomically reserve the slot:

```text
idle -> reserved
```

4. Create a `BotRun` record linked to the selected slot.
5. Generate or select the strategy config.
6. Deliver the config to the selected bot.
7. Import the script/controller config over MQTT.
8. Start the strategy over MQTT.
9. Wait for `status_updates` confirming the strategy is running.
10. Mark the slot:

```text
configuring -> running
```

### Config Delivery

There are two possible config delivery models.

#### Current Filesystem-Compatible Model

This model matches how dynamic Docker deployments work today.

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

For dynamic deployment, the API generates a script config into:

```text
bots/conf/scripts/<instance_name>.yml
```

with content like:

```yaml
script_file_name: v2_with_controllers.py
controllers_config:
  - usdc-usdt-recurring-buy.yml
```

Then `DockerService.create_hummingbot_instance()` copies:

```text
bots/conf/scripts/<instance_name>.yml
  -> bots/instances/<instance_name>/conf/scripts/<instance_name>.yml

bots/conf/controllers/usdc-usdt-recurring-buy.yml
  -> bots/instances/<instance_name>/conf/controllers/usdc-usdt-recurring-buy.yml
```

For warm bots, the same source files should be copied into the selected pool slot instead:

```text
API writes:
bots/pools/<bot_id>/conf/scripts/<run_config>.yml
bots/pools/<bot_id>/conf/controllers/<controller>.yml
```

Concrete example for `bot_1`:

```text
bots/conf/scripts/bot_1-run-001.yml
  -> bots/pools/bot_1/conf/scripts/bot_1-run-001.yml

bots/conf/controllers/usdc-usdt-recurring-buy.yml
  -> bots/pools/bot_1/conf/controllers/usdc-usdt-recurring-buy.yml
```

The deployed script config visible to the bot at runtime is:

```text
/home/hummingbot/conf/scripts/bot_1-run-001.yml
```

The deployed controller config visible to the bot at runtime is:

```text
/home/hummingbot/conf/controllers/usdc-usdt-recurring-buy.yml
```

After the files are copied, the API should send an MQTT command to import or load the script config.

The exact command topic should follow the existing Hummingbot command convention:

```text
hbot/<bot_id>/import_strategy
```

Example:

```json
{
  "request_id": "deploy-bot_1-run-001-import",
  "script": "v2_with_controllers.py",
  "conf": "bot_1-run-001.yml"
}
```

Then the API starts the strategy:

```text
hbot/<bot_id>/start
```

Example:

```json
{
  "request_id": "deploy-bot_1-run-001-start",
  "log_level": "INFO",
  "script": "v2_with_controllers.py",
  "conf": "bot_1-run-001.yml",
  "is_quickstart": true,
  "async_backend": true
}
```

The bot should respond on an API response topic or publish status transitions on:

```text
hbot/bot_1/status_updates
hbot/bot_1/log
hbot/bot_1/notify
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
    "script_config": "bot_1-run-001.yml",
    "error": "controller config usdc-usdt-recurring-buy.yml not found"
  }
}
```

#### Warm Pool Config Assignment Steps

For a warm bot assignment, the API should perform these steps:

1. Select an idle bot, for example `bot_1`.
2. Create a run-specific script config name, for example:

```text
bot_1-run-001.yml
```

3. Write the script config into the API source directory:

```text
bots/conf/scripts/bot_1-run-001.yml
```

4. Ensure every referenced controller exists under:

```text
bots/conf/controllers/
```

5. Copy the script config into the selected bot:

```text
bots/pools/bot_1/conf/scripts/bot_1-run-001.yml
```

6. Copy each referenced controller into the selected bot:

```text
bots/pools/bot_1/conf/controllers/usdc-usdt-recurring-buy.yml
```

7. Send `import_strategy` over MQTT.
8. Wait for an import acknowledgement or failure status.
9. Send `start` over MQTT.
10. Wait for `strategy/running` status.
11. Mark the slot `running` and link it to the `BotRun`.

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
  "request_id": "deploy-bot_1-run-001-configure",
  "success": true,
  "written_files": [
    "conf/scripts/bot_1-run-001.yml",
    "conf/controllers/usdc-usdt-recurring-buy.yml"
  ]
}
```

The filesystem-compatible model is easier to implement with current Hummingbot behavior. The MQTT-native model is better long term because the bot can validate and acknowledge each config update without relying on shared host files.

### MQTT Command Flow

A deployment should use request/response IDs so the API can distinguish "command sent" from "command completed".

Example high-level sequence:

```text
API -> hbot/bot_1/import_strategy
bot_1 -> hummingbot-api/response/<request_id>

API -> hbot/bot_1/start
bot_1 -> hbot/bot_1/status_updates
bot_1 -> hummingbot-api/response/<request_id>
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
    "stage": "import_strategy",
    "error": "controller config not found"
  }
}
```

### Stop And Release Flow

When the user stops a strategy:

1. API marks the slot `stopping`.
2. API sends stop command over MQTT.
3. Bot stops the strategy and cancels orders according to the request options.
4. Bot publishes:

```json
{"type": "strategy", "msg": "stopped"}
```

5. API marks the `BotRun` as stopped.
6. API archives or preserves logs/data according to policy.
7. API removes assigned script/controller config from the pool bot directory.
8. API verifies no strategy is running.
9. API marks the slot `idle`.

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

The API should reconcile slot state from durable data plus MQTT status.

On API restart:

1. Load known slots from configuration or `bot_slots`.
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
2. API marks them `offline` or `unknown`.
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

1. Start `bot_1`, `bot_2`, and `bot_3` as idle headless bots.
2. Each bot connects to MQTT and publishes lifecycle status.
3. The API discovers connected idle bots.
4. A deployment request selects an idle bot instead of creating a new container.
5. The API sends config/import/start commands over MQTT.
6. When stopped, the bot is cleaned and returned to the idle pool.
