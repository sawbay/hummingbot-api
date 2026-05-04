# Task 2.4 — Deep Controller Management (Hot-Reload Without Bot Restart)

> **Agent prompt** for adding per-controller start/stop/config REST endpoints and WS
> commands that target individual controllers inside a running bot via MQTT.

---

You are a backend Python engineer working on the `hummingbot-api` repository.

## Context
The bot command system sends MQTT messages via `BotsOrchestrator.publish_command()` (delegating
to `MQTTManager`). The `v2_with_controllers.py` Hummingbot script listens on the
`hbot/{bot_id}/config` MQTT topic for live config updates. Currently there is no API to target
a single controller inside a running bot.

## Task
Add controller lifecycle endpoints (start, stop, config update) to `BotsOrchestrator` and
expose them via new REST endpoints and WebSocket commands. Also add corresponding WS command
handlers.

### Changes required:

1. **Add three methods to `BotsOrchestrator`** in `services/bots_orchestrator.py`:

   ```python
   async def set_controller_config(
       self, bot_name: str, controller_id: str, params: dict
   ) -> dict:
       """Hot-reload a single controller's config without restarting the bot."""
       if bot_name not in self.active_bots:
           return {"success": False, "message": f"Bot '{bot_name}' not found in active bots"}
       payload = {"controller_id": controller_id, "params": params}
       success = await self.mqtt_manager.publish_command(bot_name, "config", payload)
       return {"success": success, "controller_id": controller_id}

   async def stop_controller(self, bot_name: str, controller_id: str) -> dict:
       """Stop a single controller inside a running bot."""
       if bot_name not in self.active_bots:
           return {"success": False, "message": f"Bot '{bot_name}' not found in active bots"}
       payload = {"controller_id": controller_id, "action": "stop"}
       success = await self.mqtt_manager.publish_command(bot_name, "controller", payload)
       return {"success": success, "controller_id": controller_id}

   async def start_controller(self, bot_name: str, controller_id: str) -> dict:
       """Start a single controller inside a running bot."""
       if bot_name not in self.active_bots:
           return {"success": False, "message": f"Bot '{bot_name}' not found in active bots"}
       payload = {"controller_id": controller_id, "action": "start"}
       success = await self.mqtt_manager.publish_command(bot_name, "controller", payload)
       return {"success": success, "controller_id": controller_id}
   ```

2. **Add three REST endpoints to `routers/bot_orchestration.py`**:

   ```python
   @router.post("/{bot_name}/controllers/{controller_id}/start")
   async def start_controller(
       bot_name: str,
       controller_id: str,
       bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
   ):
       """Start a single controller inside a running bot without restarting it."""
       result = await bots_manager.start_controller(bot_name, controller_id)
       if not result.get("success"):
           raise HTTPException(
               status_code=404 if "not found" in result.get("message", "") else 500,
               detail=result.get("message")
           )
       return {"status": "success", "data": result}

   @router.post("/{bot_name}/controllers/{controller_id}/stop")
   async def stop_controller(
       bot_name: str,
       controller_id: str,
       bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
   ):
       """Stop a single controller inside a running bot without stopping other controllers."""
       result = await bots_manager.stop_controller(bot_name, controller_id)
       if not result.get("success"):
           raise HTTPException(
               status_code=404 if "not found" in result.get("message", "") else 500,
               detail=result.get("message")
           )
       return {"status": "success", "data": result}

   @router.put("/{bot_name}/controllers/{controller_id}/config")
   async def update_controller_config(
       bot_name: str,
       controller_id: str,
       params: dict,
       bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
   ):
       """Hot-reload a single controller's config parameters while the bot keeps running."""
       result = await bots_manager.set_controller_config(bot_name, controller_id, params)
       if not result.get("success"):
           raise HTTPException(
               status_code=404 if "not found" in result.get("message", "") else 500,
               detail=result.get("message")
           )
       return {"status": "success", "data": result}
   ```

   Place these endpoints **before** the existing `deploy-v2-controllers` endpoint.

3. **Add WS command handlers for controller actions** in `routers/websocket.py`:

   In the `SUPPORTED_COMMANDS` set inside `_handle_ws_command()`, add:
   `"start_controller"`, `"stop_controller"`, `"update_controller_config"`

   In the command dispatch `if/elif` chain, add:
   ```python
   elif command == "start_controller":
       controller_id = params.get("controller_id") or msg.get("controller_id")
       if not controller_id:
           result = {"success": False, "message": "start_controller requires 'controller_id'"}
       else:
           result = await bots_orchestrator.start_controller(bot_name, controller_id)

   elif command == "stop_controller":
       controller_id = params.get("controller_id") or msg.get("controller_id")
       if not controller_id:
           result = {"success": False, "message": "stop_controller requires 'controller_id'"}
       else:
           result = await bots_orchestrator.stop_controller(bot_name, controller_id)

   elif command == "update_controller_config":
       controller_id = params.get("controller_id") or msg.get("controller_id")
       config_params = params.get("params", {})
       if not controller_id:
           result = {"success": False, "message": "update_controller_config requires 'controller_id'"}
       else:
           result = await bots_orchestrator.set_controller_config(bot_name, controller_id, config_params)
   ```

4. **Create `docs/test/controller_management_manual_test.md`** with a manual test plan:
   - Section per action: start_controller, stop_controller, update_controller_config
   - For each: prerequisites, curl command, expected response, verification step
   - Include both REST and WS variants

### Acceptance criteria:
- `PUT /bot-orchestration/{bot_name}/controllers/{ctrl_id}/config` with a valid running bot
  publishes an MQTT `config` message targeting the controller; the bot does NOT restart
  (verify by checking uptime or Docker container start time).
- `POST /bot-orchestration/{bot_name}/controllers/{ctrl_id}/stop` publishes a `controller`
  MQTT message with `{"action": "stop"}`.
- Calling any endpoint with a non-active `bot_name` returns `HTTP 404` with a clear message.
- `bot_status` WS subscription reflects controller status change within 5s (depends on MQTT
  performance topic being published by Hummingbot).
- WS commands `start_controller`, `stop_controller`, `update_controller_config` all return
  a `command_ack` immediately and a `command_result` after MQTT dispatch.
- The test plan document covers all three actions with REST and WS examples.
- Use `logger = logging.getLogger(__name__)`. Follow try/except style from existing methods.

Read `services/bots_orchestrator.py`, `routers/bot_orchestration.py`, and `routers/websocket.py`
in full before making any changes.

> **Dependency**: Task 2.3 must be completed first — this task extends the
> `_handle_ws_command()` function and `SUPPORTED_COMMANDS` set that Task 2.3 creates.
