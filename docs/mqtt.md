# MQTT Communication

The **hummingbot-api** uses MQTT (Message Queuing Telemetry Transport) as the primary communication channel between the API and running Hummingbot instances.

## Overview

The integration relies on `aiomqtt` for asynchronous operations. The `MQTTManager` (located in `utils/mqtt_manager.py`) is responsible for managing connections, subscribing to topics, and publishing commands to the bots.

The broker configuration (host, port, username, password, ssl) is injected into the `BotsOrchestrator` which initializes the `MQTTManager`.

## Topic Structure

Hummingbot instances publish their status, logs, and events to specific topics, and listen to command topics. The API interacts with bots using the following patterns:

- **Base Pattern**: `hbot/{bot_id}/{channel}`
- *Note*: When translating a `bot_id` to an MQTT topic, periods are replaced with slashes (e.g., `bot.name` becomes `bot/name`).

### Subscribed Topics (Incoming to API)

The API subscribes to these channels to receive updates from bots:
- `log`: Application logs (Info, Error, etc.). The API deduplicates these logs within a 5-minute TTL to prevent spam.
- `notify`: Notification messages.
- `status_updates`: Bot status changes.
- `events`: Internal Hummingbot events.
- `hb`: Heartbeat messages. Used by the API to auto-discover and verify that bots are actively running.
- `performance`: Performance metrics and custom information sent by strategy controllers.
- `external/event/+`: External events from the bot.
- `hummingbot-api/response/+`: Specific RPC responses to commands.

### Publish Topics (Outgoing to Bots)

The API publishes to the following command channels:
- `start`: Starts a bot with a specific script/config.
- `stop`: Stops a bot.
- `config`: Modifies bot configuration parameters.
- `import_strategy`: Imports a new strategy.
- `history`: Requests trading history.

## RPC Command Pattern

For commands that require a response (like `history`), the API implements an RPC (Remote Procedure Call) pattern:

1. The API generates a unique reply topic: `hummingbot-api/response/{timestamp}`.
2. It publishes the command payload containing the `reply_to` header to the bot's command topic.
3. The `MQTTManager` creates an `asyncio.Future` and waits for a response on that specific `reply_to` topic.
4. When the bot responds, the message is routed back to resolve the Future, returning the data synchronously to the caller.

## Auto-Discovery

The API uses a combination of Docker container status and MQTT heartbeats (`hb` messages) to maintain an active registry of bots:
- **Heartbeats**: When a bot publishes to its `hb` topic, the API updates its "last seen" timestamp.
- Bots that haven't sent a message within 30 seconds (or 5 minutes, depending on context) are tracked, and their active status is determined by this recent activity. This prevents the API from thinking a bot is "running" if its container is up but it's disconnected from MQTT.
