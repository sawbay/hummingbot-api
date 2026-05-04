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
