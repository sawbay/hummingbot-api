# Task 2.2 — WS Delta Protocol + Fine-Grained Channels

> **Agent prompt** for adding `snapshot`/`delta` message modes and two new subscription
> channels (`bot_heartbeat`, `bot_trades`) to `ExecutorWebSocketManager`.

---

You are a backend Python engineer working on the `hummingbot-api` repository.

## Context
The file `services/executor_ws_manager.py` contains push loops that always send full
payloads on every hash change. The `ExecutorSubscription` dataclass already has a
`last_sent_hash` field for change detection, but there is no concept of snapshot vs delta.
The `SUBSCRIPTION_TYPES` set is defined at the top of the file.
The websocket router is in `routers/websocket.py`.

## Task
Add `mode: "snapshot" | "delta"` to all WS push messages, compute field-level diffs for
subsequent pushes, and add two new subscription channels: `bot_heartbeat` and `bot_trades`.

### Changes required:

1. **Add a `mode` helper to `executor_ws_manager.py`**:

   Add this utility function near `_compute_hash`:
   ```python
   def _compute_delta(old: Any, new: Any) -> Any:
       """Return only the top-level keys that changed between old and new dicts.
       If either is not a dict, return new in full."""
       if not isinstance(old, dict) or not isinstance(new, dict):
           return new
       return {k: v for k, v in new.items() if old.get(k) != v}
   ```

   Add a `last_sent_data: Optional[Any] = None` field to the `ExecutorSubscription` dataclass.

2. **Update every existing push loop** to include `"mode"` in the sent message:
   - On first send (when `sub.last_sent_data is None`): set `mode = "snapshot"`, send full data.
   - On subsequent sends: set `mode = "delta"`, send `_compute_delta(sub.last_sent_data, data)`
     as the data payload.
   - After sending, update `sub.last_sent_data = data`.

   Apply this pattern to ALL of these loops:
   `_executors_push_loop`, `_executor_detail_push_loop`, `_summary_push_loop`,
   `_performance_push_loop`, `_positions_push_loop`, `_bot_status_push_loop`,
   `_all_bots_status_push_loop`.

   **Do NOT** change `_logs_push_loop` (it already sends only new entries) or
   `_bot_deployment_push_loop` (deployment state machine should always send full state).

   The message schema becomes:
   ```json
   {"type": "bot_status", "subscription_id": "...", "mode": "snapshot", "data": {...}, "timestamp": ...}
   {"type": "bot_status", "subscription_id": "...", "mode": "delta", "data": {"status": "running"}, "timestamp": ...}
   ```

3. **Add heartbeat keepalive per subscription**:
   In each updated push loop, add a heartbeat: if no delta has been sent within
   `update_interval * 3` seconds (track with `last_push_time = time.time()`), send:
   ```json
   {"type": "heartbeat", "subscription_id": "...", "timestamp": ...}
   ```
   Reset `last_push_time` after each send (snapshot, delta, or heartbeat).

4. **Add new subscription type `bot_heartbeat`**:
   - Add `"bot_heartbeat"` to `SUBSCRIPTION_TYPES`.
   - In `handle_subscribe`, handle `sub_type == "bot_heartbeat"`:
     - Require `"bot_name"` field; set `sub.bot_name` and `sub.sub_id = f"bot_heartbeat_{bot_name}"`.
   - Add `_bot_heartbeat_push_loop` method:
     - On each iteration, call `self._bots_orchestrator.mqtt_manager.get_discovered_bots(timeout_seconds=30)`.
     - Determine `online = sub.bot_name in discovered_bots`.
     - Build payload: `{"bot_name": sub.bot_name, "online": online, "timestamp": time.time()}`.
     - If the EventBus is available, use `await queue.get()` with a `timeout=30` fallback;
       otherwise use `await asyncio.sleep(sub.update_interval)`.
     - Always send mode `"snapshot"` on first push; `"delta"` with changed fields after.
   - Add `"bot_heartbeat": self._bot_heartbeat_push_loop` to `_get_push_fn`.

5. **Add new subscription type `bot_trades`**:
   - Add `"bot_trades"` to `SUBSCRIPTION_TYPES`.
   - In `handle_subscribe`, handle `sub_type == "bot_trades"`:
     - Require `"bot_name"` field; set `sub.bot_name` and `sub.sub_id = f"bot_trades_{bot_name}"`.
   - Add `_bot_trades_push_loop` method:
     - Use EventBus (if available) — subscribe with `subscriber_id`, filter events where
       `event.bot_name == sub.bot_name and event.event_type == "trade"`.
     - If EventBus not available, fall back to: send an empty `snapshot` immediately and
       sleep — trades will not be available without the EventBus.
     - Forward each trade event as:
       `{"type": "bot_trades", "subscription_id": ..., "mode": "event", "data": event.payload, "timestamp": ...}`
     - `"mode": "event"` signals a single occurrence (not a full state replacement).
   - Add `"bot_trades": self._bot_trades_push_loop` to `_get_push_fn`.

6. **Update `docs/ws.md`** (file exists in `docs/`):
   - Add a section "## Message Modes" explaining `snapshot`, `delta`, `event`, and `heartbeat`.
   - Add entries for `bot_heartbeat` and `bot_trades` subscription types with example messages.

### Acceptance criteria:
- First message after subscribe always has `"mode": "snapshot"` with complete data.
- Subsequent messages only contain changed keys in `data` and have `"mode": "delta"`.
- If no delta occurs within `update_interval * 3` seconds, a `heartbeat` is sent.
- `bot_heartbeat` subscription correctly reflects online/offline state within < 200ms
  when the EventBus is wired up.
- All existing subscription types still work without any breaking change to the client
  protocol (only `mode` field is added, which is additive).
- Do NOT modify `_bot_deployment_push_loop` or `_logs_push_loop`.

Read all relevant files first before making any changes.

> **Note**: Task 2.1 (EventBus) should be completed before this task. If it has not been
> merged yet, implement the `bot_heartbeat` and `bot_trades` loops using `asyncio.sleep`
> as a fallback and leave EventBus integration as a `if self._event_bus:` guard.
