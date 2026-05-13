# Task 2.3 — WebSocket RPC for Bot Commands

> **Agent prompt** for adding a `"command"` action branch to the `/ws/executors` WebSocket
> endpoint, enabling bot commands to be dispatched over the existing WS connection.

---

You are a backend Python engineer working on the `hummingbot-api` repository.

## Context
The WebSocket router is in `routers/websocket.py`. It currently handles three actions:
`subscribe`, `unsubscribe`, and `ping`. The bot orchestration logic lives in
`services/bots_orchestrator.py` which has methods: `start_bot()`, `stop_bot()`,
`configure_bot()`, `import_strategy_for_bot()`, and `get_bot_history()`.
Authentication is handled by the existing `_authenticate_websocket()` function in the
same file.

## Task
Add a `"command"` action branch to the `/ws/executors` WebSocket endpoint that dispatches
bot commands over the existing connection and returns acks and results asynchronously.

### Changes required:

1. **Add `_handle_ws_command()` async function in `routers/websocket.py`**:

   ```python
   async def _handle_ws_command(
       websocket: WebSocket,
       msg: dict,
       bots_orchestrator,  # BotsOrchestrator instance
   ) -> None:
       """Dispatch a bot command received over WebSocket."""
       command = msg.get("command")
       request_id = msg.get("request_id")
       bot_name = msg.get("bot_name")

       SUPPORTED_COMMANDS = {"start_bot", "stop_bot", "configure_bot",
                             "import_strategy", "get_history"}

       if command not in SUPPORTED_COMMANDS:
           await websocket.send_json({
               "type": "error",
               "message": f"Unknown command: {command}. Supported: {sorted(SUPPORTED_COMMANDS)}",
               "request_id": request_id,
           })
           return

       if not bot_name:
           await websocket.send_json({
               "type": "error",
               "message": "Command requires 'bot_name'",
               "request_id": request_id,
           })
           return

       # Immediately acknowledge receipt
       await websocket.send_json({
           "type": "command_ack",
           "request_id": request_id,
           "command": command,
           "bot_name": bot_name,
           "status": "sent",
           "message": f"Command '{command}' dispatched to {bot_name}",
       })

       # Execute the command
       try:
           params = msg.get("params", {})

           if command == "start_bot":
               result = await bots_orchestrator.start_bot(bot_name, **params)

           elif command == "stop_bot":
               result = await bots_orchestrator.stop_bot(bot_name, **params)

           elif command == "configure_bot":
               result = await bots_orchestrator.configure_bot(bot_name, params=params)

           elif command == "import_strategy":
               strategy = params.get("strategy") or msg.get("strategy")
               if not strategy:
                   result = {"success": False, "message": "import_strategy requires 'strategy'"}
               else:
                   result = await bots_orchestrator.import_strategy_for_bot(bot_name, strategy)

           elif command == "get_history":
               result = await bots_orchestrator.get_bot_history(bot_name, **params)

           await websocket.send_json({
               "type": "command_result",
               "request_id": request_id,
               "command": command,
               "bot_name": bot_name,
               "result": result,
           })

       except Exception as e:
           logger.error(f"[WS-Cmd] Error executing {command} for {bot_name}: {e}", exc_info=True)
           await websocket.send_json({
               "type": "command_result",
               "request_id": request_id,
               "command": command,
               "bot_name": bot_name,
               "result": {"success": False, "message": str(e)},
           })
   ```

2. **Wire into the `/ws/executors` message dispatch loop** in `routers/websocket.py`:

   In the `executors_websocket` handler, inside the `while True: raw = await websocket.receive_json()` block, add:
   ```python
   elif action == "command":
       # Commands are fire-and-forget; run in background task to not block receive loop
       bots_orchestrator = websocket.app.state.bots_orchestrator
       asyncio.create_task(
           _handle_ws_command(websocket, raw, bots_orchestrator),
           name=f"ws-cmd-{conn_id}-{raw.get('request_id', 'unknown')}"
       )
   ```
   Place this branch after the `elif action == "ping":` block.

   Update the error message for unknown actions to include `"command"`:
   ```python
   f"Valid actions: subscribe, unsubscribe, ping, command"
   ```

3. **Do NOT add `command` to `/ws/market-data`** — commands only apply to the executors socket.

4. **Update `docs/ws.md`**:
   - Add a section "## Bot Commands via WebSocket" documenting:
     - The command message schema (action, command, bot_name, params, request_id)
     - The `command_ack` response schema
     - The `command_result` response schema
     - All 5 supported commands with example payloads

### Acceptance criteria:
- Sending `{"action": "command", "command": "stop_bot", "bot_name": "bot_001", "request_id": "r1"}`
  over `/ws/executors` receives `command_ack` with `"status": "sent"` within < 50ms.
- The bot actually receives the MQTT stop command (verifiable by checking `bot_status` sub).
- An unknown command returns `{"type": "error", "message": "Unknown command: ...", "request_id": "r1"}`.
- Commands run as background tasks: receiving a slow command (e.g., `get_history`) does NOT
  block the WS from processing other subscribe/unsubscribe messages while waiting.
- Existing `subscribe`, `unsubscribe`, and `ping` actions are completely unchanged.
- Auth check applies: the WS must already be authenticated (it is — auth happens at connection
  time via `_authenticate_websocket()`; no additional auth is needed per command).
- Use `logger = logging.getLogger(__name__)` for logging in the new function.

Read `routers/websocket.py` and `services/bots_orchestrator.py` in full before making changes.
