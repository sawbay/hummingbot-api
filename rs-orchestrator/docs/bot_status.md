# Bot Status MQTT Surfaces

Status-related MQTT surfaces in this repo:

| Purpose | Topic | Type | Payload | Response / Message |
|---|---|---|---|---|
| Current strategy status query | `hbot/<instance_id>/status` | RPC | `{"async_backend": false}` | `{"status": 200, "msg": "<status text>", "data": ""}` |
| Async status trigger | `hbot/<instance_id>/status` | RPC | `{"async_backend": true}` | Immediate ack: `{"status": 200, "msg": "", "data": ""}` |
| Status output from async trigger | `hbot/<instance_id>/notify` | Pub/Sub | n/a | `{"msg": "<status text>"}` |
| Bot availability/lifecycle | `hbot/<instance_id>/status_updates` | Pub/Sub | n/a | `{"msg": "online", "type": "availability", "timestamp": <ms>}` |
| Heartbeat | `hbot/<instance_id>/hb` | Pub/Sub | n/a | heartbeat messages from the MQTT node |

For current bot status, prefer synchronous RPC:

```json
Topic: hbot/<instance_id>/status
Payload: {"async_backend": false}
```

Successful response:

```json
{
  "status": 200,
  "msg": "<formatted strategy status>",
  "data": ""
}
```

Error example when no strategy is running:

```json
{
  "status": 400,
  "msg": "No strategy is currently running!",
  "data": ""
}
```

From `/status`, you can know whether the bot has an active strategy, and if it does, the strategy's formatted runtime status. The exact contents depend on the strategy's `format_status()` implementation, but typically include markets/connectors, balances, active orders, executor/controller state for Strategy V2, performance, and warnings.

From `/status_updates`, you can know bridge lifecycle/availability only: whether the MQTT bridge came online/offline and any lifecycle messages published by the app. It is not a full bot-state snapshot.

From `/notify`, you receive CLI-style notification output, including async status output if `/status` is called with `async_backend: true`.
