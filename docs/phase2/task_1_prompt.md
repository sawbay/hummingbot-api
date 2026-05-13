# Task 2.1 — Internal Event Bus

> **Agent prompt** for implementing the `asyncio.Queue`-based EventBus that replaces
> polling-based push loops in `ExecutorWebSocketManager`.

---

You are a backend Python engineer working on the `hummingbot-api` repository.

## Context
The file `services/executor_ws_manager.py` contains an `ExecutorWebSocketManager` class.
Every push loop (`_bot_status_push_loop`, `_performance_push_loop`, `_logs_push_loop`, etc.)
uses a `while True: ... await asyncio.sleep(interval)` pattern. MQTT messages land in
`utils/mqtt_manager.py` callbacks but are only stored in dicts — the push loops must poll
them periodically, introducing fixed `update_interval` latency (default 2s).

## Task
Introduce an `asyncio.Queue`-based `EventBus` so that push loops wake up immediately when
an MQTT message arrives rather than sleeping.

### Changes required:

1. **Create `utils/event_bus.py`** (new file):
   ```python
   import asyncio
   from dataclasses import dataclass
   from typing import Any, Dict

   @dataclass
   class BotEvent:
       bot_name: str
       event_type: str   # "performance", "hb", "status", "log"
       payload: Any

   class EventBus:
       def __init__(self):
           self._queues: Dict[str, asyncio.Queue] = {}

       def subscribe(self, subscriber_id: str, maxsize: int = 1000) -> asyncio.Queue:
           q = asyncio.Queue(maxsize=maxsize)
           self._queues[subscriber_id] = q
           return q

       def unsubscribe(self, subscriber_id: str) -> None:
           self._queues.pop(subscriber_id, None)

       def publish(self, event: BotEvent) -> None:
           for sub_id, q in self._queues.items():
               try:
                   q.put_nowait(event)
               except asyncio.QueueFull:
                   import logging
                   logging.getLogger(__name__).warning(
                       f"EventBus queue full for subscriber '{sub_id}', dropping event"
                   )

       def queue_depth(self, subscriber_id: str) -> int:
           q = self._queues.get(subscriber_id)
           return q.qsize() if q else 0
   ```

2. **Instantiate `EventBus` in `main.py` `lifespan()`**:
   - `from utils.event_bus import EventBus`
   - After `db_manager` is initialized, add: `event_bus = EventBus()`
   - Store in app state: `app.state.event_bus = event_bus`
   - Inject it into `MQTTManager` constructor: add an optional `event_bus: Optional[EventBus] = None`
     parameter to `MQTTManager.__init__` and store it as `self._event_bus = event_bus`.
   - Pass it when constructing in `main.py`: `MQTTManager(..., event_bus=event_bus)`.
   - Also inject into `ExecutorWebSocketManager`: add `event_bus: Optional[EventBus] = None`
     to its `__init__` and store as `self._event_bus = event_bus`.
   - Pass it when constructing `executor_ws_manager` in `main.py`.

3. **Publish events from `MQTTManager`** in `utils/mqtt_manager.py`:
   - In `_handle_performance(bot_id, data)`: after storing data, call:
     `self._event_bus.publish(BotEvent(bot_name=bot_id, event_type="performance", payload=data))`
   - In `_handle_heartbeat(bot_id, data)`: call:
     `self._event_bus.publish(BotEvent(bot_name=bot_id, event_type="hb", payload=data))`
   - In `_handle_status(bot_id, data)`: call:
     `self._event_bus.publish(BotEvent(bot_name=bot_id, event_type="status", payload=data))`
   - In `_handle_log(bot_id, data)`: after appending to deque, call:
     `self._event_bus.publish(BotEvent(bot_name=bot_id, event_type="log", payload=data))`
   - Guard each publish call: `if self._event_bus:`.
   - Add `from utils.event_bus import EventBus, BotEvent` import (guarded by TYPE_CHECKING
     if you prefer to avoid circular imports).

4. **Refactor push loops in `services/executor_ws_manager.py`**:

   Refactor exactly these three loops:
   - `_bot_status_push_loop`
   - `_performance_push_loop`
   - `_logs_push_loop`

   **Pattern for each refactored loop**:
   ```python
   async def _bot_status_push_loop(self, conn_id, websocket, sub):
       subscriber_id = f"{conn_id}-{sub.sub_id}"
       queue = self._event_bus.subscribe(subscriber_id) if self._event_bus else None
       try:
           while True:
               # Wait for an event OR fall back to interval-based refresh
               if queue:
                   try:
                       await asyncio.wait_for(queue.get(), timeout=sub.update_interval)
                   except asyncio.TimeoutError:
                       pass  # Heartbeat fallback: push even if no event arrived
               else:
                   await asyncio.sleep(sub.update_interval)

               try:
                   # ... existing data-fetch and send logic unchanged ...
               except Exception as e:
                   logger.error(...)

               # Warn if queue is building up
               if queue and self._event_bus.queue_depth(subscriber_id) > 100:
                   logger.warning(f"[WS-Exec] Queue depth > 100 for {subscriber_id}")
       except asyncio.CancelledError:
           pass
       finally:
           if queue and self._event_bus:
               self._event_bus.unsubscribe(subscriber_id)
   ```

   Do NOT refactor `_executors_push_loop`, `_positions_push_loop`, `_summary_push_loop`,
   `_bot_deployment_push_loop`, or `_all_bots_status_push_loop` — leave them using
   `asyncio.sleep` for now.

5. **Call `unsubscribe` on disconnection**: In `ExecutorWebSocketManager.remove_connection()`,
   after cancelling tasks, iterate over the (now-removed) subscriptions and call
   `self._event_bus.unsubscribe(f"{conn_id}-{sub.sub_id}")` for each if `self._event_bus`.

### Acceptance criteria:
- The three refactored push loops wake up and push data within < 100ms of an MQTT event.
- If no MQTT event arrives, they still push on the `update_interval` timeout (heartbeat fallback).
- Existing behavior of all other push loops is unchanged.
- When a WS client disconnects, the EventBus queue for that subscriber is cleaned up.
- Use `logger = logging.getLogger(__name__)` for all new logging.
- Guard all `self._event_bus` accesses with `if self._event_bus:` to keep backward compatibility
  (EventBus is optional; loops fall back to sleep-based polling if not injected).

Read all files before editing. Make targeted changes only — do not rewrite unrelated code.
