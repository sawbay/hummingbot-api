import asyncio
import json
import logging
import ssl
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Any, Callable, Dict, Optional, Set

import aiomqtt
from utils.event_bus import EventBus, BotEvent

logger = logging.getLogger(__name__)


class MQTTManager:
    """
    Manages MQTT connections and message handling for Hummingbot bot communication.
    Uses asyncio-mqtt (aiomqtt) for asynchronous MQTT operations.
    """

    def __init__(self, host: str, port: int, username: str, password: str, ssl: bool = False, event_bus: Optional[EventBus] = None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssl = ssl
        self._event_bus = event_bus

        # Message handlers by topic pattern
        self._handlers: Dict[str, Callable] = {}

        # Bot data storage - stores full controller reports (performance + custom_info)
        self._bot_controller_reports: Dict[str, Dict] = defaultdict(dict)
        self._bot_logs: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._bot_error_logs: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))

        # Auto-discovered bots
        self._discovered_bots: Dict[str, float] = {}  # bot_id: last_seen_timestamp
        
        # Message deduplication tracking
        self._processed_messages: Dict[str, float] = {}  # message_hash: timestamp
        self._message_ttl = 300  # 5 minutes TTL for processed messages

        # Connection state
        self._connected: bool = False
        self.connected_since: Optional[float] = None
        self._reconnecting: bool = False
        self._shutdown: bool = False
        self._client: Optional[aiomqtt.Client] = None
        self._tasks: Set[asyncio.Task] = set()

        # RPC response tracking
        self._pending_responses: Dict[str, asyncio.Future] = {}  # reply_to_topic: future

        # Subscriptions to restore on reconnect
        self._subscribed_topics: Set[str] = {
            "hbot/+/log",
            "hbot/+/notify",
            "hbot/+/status_updates",
            "hbot/+/events",
            "hbot/+/hb",
            "hbot/+/performance",
            "hbot/+/external/event/+",
            "hbot/orchestrate/status",
            "hummingbot-api/response/+",
        }

        if username:
            logger.info(f"MQTT client configured for user: {username}")
        else:
            logger.info("MQTT client configured without authentication")

    @asynccontextmanager
    async def _get_client(self):
        """Get MQTT client for a single connection attempt."""
        client_id = f"hummingbot-api-{int(time.time())}"

        client_kwargs = {
            "hostname": self.host,
            "port": self.port,
            "identifier": client_id,
            "keepalive": 60,
        }
        if self.username and self.password:
            client_kwargs["username"] = self.username
            client_kwargs["password"] = self.password
        if self.ssl:
            client_kwargs["tls_context"] = ssl.create_default_context()

        client = aiomqtt.Client(**client_kwargs)

        async with client:
            yield client

    def is_connected(self) -> bool:
        """Return True if the MQTT client is currently connected, False otherwise."""
        return self._connected

    async def subscribe(self, topic: str, qos: int = 1):
        """Subscribe to a topic and track it for reconnection."""
        self._subscribed_topics.add(topic)
        if self._connected and self._client:
            try:
                await self._client.subscribe(topic, qos=qos)
                logger.debug(f"Subscribed to {topic}")
            except Exception as e:
                logger.error(f"Failed to subscribe to {topic}: {e}")

    async def _on_connect(self):
        """Set connection state and re-subscribe to all topics."""
        self._connected = True
        self.connected_since = time.time()
        self._reconnecting = False
        logger.info(f"✓ Connected to MQTT broker at {self.host}:{self.port}")

        # Re-subscribe to all topics
        for topic in self._subscribed_topics:
            try:
                await self._client.subscribe(topic)
            except Exception as e:
                logger.error(f"Failed to re-subscribe to {topic}: {e}")

    def _on_disconnect(self):
        """Set connection state and trigger reconnection loop."""
        self._connected = False
        self.connected_since = None

        if not self._shutdown and not self._reconnecting:
            self._reconnecting = True
            task = asyncio.create_task(self._reconnect_loop())
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _reconnect_loop(self):
        """Exponential backoff reconnection loop."""
        delay = 1.0
        while not self._shutdown:
            logger.warning(f"MQTT reconnecting in {delay}s...")
            await asyncio.sleep(delay)

            try:
                # Try to connect and process messages
                await self._handle_messages()

                # If _handle_messages returns, check if we are connected
                if self._connected:
                    logger.info("MQTT reconnected successfully")
                    return
            except Exception as e:
                logger.warning(f"Reconnection attempt failed: {e}")

            delay = min(delay * 2, 60.0)

    async def _handle_messages(self):
        """Main message handling loop."""
        try:
            async with self._get_client() as client:
                self._client = client
                await self._on_connect()
                async for message in client.messages:
                    await self._process_message(message)

            # Normal exit (e.g. stop() called)
            if not self._shutdown:
                self._on_disconnect()
        except (aiomqtt.MqttError, Exception) as error:
            if not self._shutdown:
                logger.error(f'MQTT connection error: "{error}"')
                self._on_disconnect()

    async def _process_message(self, message):
        """Process incoming MQTT message."""
        try:
            topic = str(message.topic)
            try:
                generic_data = json.loads(message.payload.decode("utf-8"))
            except json.JSONDecodeError:
                generic_data = message.payload.decode("utf-8")

            # Check if this is an RPC response to our hummingbot-api
            if topic.startswith("hummingbot-api/response/"):
                await self._handle_rpc_response(topic, message)
                return

            for pattern, handler in self._handlers.items():
                if self._match_topic(pattern, topic):
                    topic_parts_for_handler = topic.split("/")
                    bot_id = topic_parts_for_handler[1] if len(topic_parts_for_handler) > 1 else ""
                    channel = "/".join(topic_parts_for_handler[2:]) if len(topic_parts_for_handler) > 2 else ""
                    if asyncio.iscoroutinefunction(handler):
                        await handler(bot_id, channel, generic_data)
                    else:
                        await asyncio.get_event_loop().run_in_executor(None, handler, bot_id, channel, generic_data)

            topic_parts = topic.split("/")

            # Check if this matches namespace/instance_id/channel pattern
            if len(topic_parts) >= 3:
                namespace, bot_id, channel = topic_parts[0], topic_parts[1], "/".join(topic_parts[2:])
                # Only process if it's the expected namespace
                if namespace == "hbot":
                    # Auto-discover bot
                    self._discovered_bots[bot_id] = time.time()
                    data = generic_data

                    # Route to appropriate handler based on Hummingbot's topics
                    if channel == "log":
                        await self._handle_log(bot_id, data)
                    elif channel == "notify":
                        await self._handle_notify(bot_id, data)
                    elif channel == "status_updates":
                        await self._handle_status(bot_id, data)
                    elif channel == "hb":  # heartbeat
                        await self._handle_heartbeat(bot_id, data)
                    elif channel == "events":
                        await self._handle_events(bot_id, data)
                    elif channel == "performance":
                        await self._handle_performance(bot_id, data)
                    elif channel.startswith("response/"):
                        await self._handle_command_response(bot_id, channel, data)
                    elif channel.startswith("external/event/"):
                        await self._handle_external_event(bot_id, channel, data)
                    elif channel in ["history", "start", "stop", "config", "import_strategy"]:
                        # These are command channels - responses should come on response/* topics
                        logger.debug(f"Command channel '{channel}' for bot {bot_id} - waiting for response")
                    else:
                        logger.info(f"Unknown channel '{channel}' for bot {bot_id}")

        except Exception as e:
            logger.error(f"Error processing message from {message.topic}: {e}", exc_info=True)

    def _match_topic(self, pattern: str, topic: str) -> bool:
        """Check if topic matches pattern (supports + wildcard)."""
        pattern_parts = pattern.split("/")
        topic_parts = topic.split("/")

        if len(pattern_parts) != len(topic_parts):
            return False

        for p, t in zip(pattern_parts, topic_parts):
            if p != "+" and p != t:
                return False
        return True

    async def _handle_performance(self, bot_id: str, data: Any):
        """Handle performance updates.

        Expected data structure from Hummingbot:
        {
            "controller_id": {
                "performance": { ... performance metrics ... },
                "custom_info": { ... custom controller data ... }
            }
        }
        """
        if isinstance(data, dict):
            for controller_id, controller_report in data.items():
                if bot_id not in self._bot_controller_reports:
                    self._bot_controller_reports[bot_id] = {}
                self._bot_controller_reports[bot_id][controller_id] = controller_report
            
            if self._event_bus:
                self._event_bus.publish(BotEvent(bot_name=bot_id, event_type="performance", payload=data))

    async def _handle_log(self, bot_id: str, data: Any):
        """Handle log messages with deduplication."""
        # Create a unique message identifier for deduplication
        if isinstance(data, dict):
            level = data.get("level_name") or data.get("levelname") or data.get("level", "INFO")
            message = data.get("msg") or data.get("message", "")
            timestamp = data.get("timestamp") or data.get("time") or time.time()
            
            # Create hash for deduplication (bot_id + message + timestamp within 1 second)
            message_hash = f"{bot_id}:{message}:{int(timestamp)}"
        elif isinstance(data, str):
            message = data
            timestamp = time.time()
            level = "INFO"
            
            # Create hash for string messages
            message_hash = f"{bot_id}:{message}:{int(timestamp)}"
        else:
            return  # Skip invalid data

        # Check for duplicates
        current_time = time.time()
        if message_hash in self._processed_messages:
            # Skip duplicate message
            logger.debug(f"Skipping duplicate log message from {bot_id}: {message[:50]}...")
            return

        # Clean up old message hashes (older than TTL)
        expired_hashes = [h for h, t in self._processed_messages.items() if current_time - t > self._message_ttl]
        for h in expired_hashes:
            del self._processed_messages[h]

        # Record this message as processed
        self._processed_messages[message_hash] = current_time

        # Process the message
        if isinstance(data, dict):
            # Normalize the log entry
            log_entry = {
                "level_name": level,
                "msg": message,
                "timestamp": timestamp,
                **data,  # Include all original fields
            }

            if level.upper() == "ERROR":
                self._bot_error_logs[bot_id].append(log_entry)
            else:
                self._bot_logs[bot_id].append(log_entry)
        elif isinstance(data, str):
            # Handle plain string logs
            log_entry = {"level_name": "INFO", "msg": data, "timestamp": timestamp}
            self._bot_logs[bot_id].append(log_entry)

        if self._event_bus:
            self._event_bus.publish(BotEvent(bot_name=bot_id, event_type="log", payload=data))

    async def _handle_notify(self, bot_id: str, data: Any):
        """Handle notification messages."""
        # Store notifications if needed

    async def _handle_status(self, bot_id: str, data: Any):
        """Handle status updates."""
        # Store latest status
        if self._event_bus:
            self._event_bus.publish(BotEvent(bot_name=bot_id, event_type="status", payload=data))

    async def _handle_heartbeat(self, bot_id: str, data: Any):
        """Handle heartbeat messages."""
        self._discovered_bots[bot_id] = time.time()  # Update last seen
        if self._event_bus:
            self._event_bus.publish(BotEvent(bot_name=bot_id, event_type="hb", payload=data))

    async def _handle_events(self, bot_id: str, data: Any):
        """Handle internal events."""
        # Process events as needed

    async def _handle_external_event(self, bot_id: str, channel: str, data: Any):
        """Handle external events."""
        event_type = channel.split("/")[-1]

    async def _handle_rpc_response(self, topic: str, message):
        """Handle RPC responses on hummingbot-api/response/* topics."""
        try:
            # Parse the response data
            try:
                data = json.loads(message.payload.decode("utf-8"))
            except json.JSONDecodeError:
                data = message.payload.decode("utf-8")

            # Check if we have a pending response for this topic
            if topic in self._pending_responses:
                future = self._pending_responses.pop(topic)
                if not future.done():
                    future.set_result(data)
            else:
                logger.warning(f"No pending RPC response found for topic: {topic}")

        except Exception as e:
            logger.error(f"Error handling RPC response on {topic}: {e}", exc_info=True)

    async def _handle_command_response(self, bot_id: str, channel: str, data: Any):
        """Handle command responses (legacy - keeping for backward compatibility)."""
        # Extract command from response channel (e.g., response/start/1234567890 or response/history)
        channel_parts = channel.split("/")
        if len(channel_parts) >= 2:
            command = channel_parts[1]

    async def start(self):
        """Start the MQTT client."""
        try:
            # Create and store the main message handling task
            task = asyncio.create_task(self._handle_messages())
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

            logger.info("MQTT client started")

            # Wait a bit for connection to establish
            for i in range(10):
                if self._connected:
                    logger.info("MQTT connection established successfully")
                    break
                await asyncio.sleep(0.5)
            else:
                logger.warning("MQTT connection not established after 5 seconds")

        except Exception as e:
            logger.error(f"Failed to start MQTT client: {e}", exc_info=True)

    async def stop(self):
        """Stop the MQTT client."""
        self._shutdown = True
        self._connected = False

        # Cancel all running tasks
        for task in self._tasks:
            task.cancel()

        # Wait for all tasks to complete
        await asyncio.gather(*self._tasks, return_exceptions=True)

        logger.info("MQTT client stopped")

    async def publish_command_and_wait(
        self, bot_id: str, command: str, data: Dict[str, Any], timeout: float = 30.0, qos: int = 1
    ) -> Optional[Any]:
        """
        Publish a command to a bot and wait for the response.

        :param bot_id: The bot instance ID
        :param command: The command to send
        :param data: Command data
        :param timeout: Timeout in seconds to wait for response
        :param qos: Quality of Service level
        :return: Response data if received, None if timeout or error
        """
        if not self._connected or not self._client:
            logger.error("Not connected to MQTT broker")
            return None

        # Generate unique reply_to topic
        timestamp = int(time.time() * 1000)
        reply_to_topic = f"hummingbot-api/response/{timestamp}"

        # Create a future to track the response using the reply_to topic as key
        future = asyncio.Future()
        self._pending_responses[reply_to_topic] = future

        try:
            # Send the command with custom reply_to
            success = await self._publish_command_with_reply_to(bot_id, command, data, reply_to_topic, qos)
            if not success:
                self._pending_responses.pop(reply_to_topic, None)
                return None

            # Wait for response with timeout
            try:
                response = await asyncio.wait_for(future, timeout=timeout)
                return response
            except asyncio.TimeoutError:
                logger.warning(f"⏰ Timeout waiting for response from {bot_id} for command '{command}' on {reply_to_topic}")
                self._pending_responses.pop(reply_to_topic, None)
                return None

        except Exception as e:
            logger.error(f"Error sending command and waiting for response: {e}")
            self._pending_responses.pop(reply_to_topic, None)
            return None

    async def _publish_command_with_reply_to(
        self, bot_id: str, command: str, data: Dict[str, Any], reply_to: str, qos: int = 1
    ) -> bool:
        """
        Publish a command to a bot with custom reply_to topic.

        :param bot_id: The bot instance ID
        :param command: The command to send
        :param data: Command data
        :param reply_to: Custom reply_to topic
        :param qos: Quality of Service level
        :return: True if published successfully
        """
        if not self._connected or not self._client:
            logger.error("Not connected to MQTT broker")
            return False

        # Convert dots to slashes for MQTT topic
        mqtt_bot_id = bot_id.replace(".", "/")

        # Use the correct topic for each command
        topic = f"hbot/{mqtt_bot_id}/{command}"

        # Create the full RPC message structure with custom reply_to
        message = {
            "header": {
                "timestamp": int(time.time() * 1000),  # Milliseconds
                "reply_to": reply_to,  # Custom reply_to topic
                "msg_id": int(time.time() * 1000),
                "node_id": "hummingbot-api",
                "agent": "hummingbot-api",
                "properties": {},
            },
            "data": data or {},
        }

        try:
            await self._client.publish(topic, payload=json.dumps(message), qos=qos)
            return True
        except Exception as e:
            logger.error(f"Failed to publish command to {bot_id}: {e}")
            return False

    async def publish_command(self, bot_id: str, command: str, data: Dict[str, Any], qos: int = 1) -> bool:
        """
        Publish a command to a bot using proper RPCMessage Request format.

        :param bot_id: The bot instance ID
        :param command: The command to send
        :param data: Command data (should match the specific CommandMessage.Request structure)
        :param qos: Quality of Service level
        :return: True if published successfully
        """
        if not self._connected or not self._client:
            logger.error("Not connected to MQTT broker")
            return False

        # Convert dots to slashes for MQTT topic
        mqtt_bot_id = bot_id.replace(".", "/")

        # Use the correct topic for each command
        topic = f"hbot/{mqtt_bot_id}/{command}"

        # Create the full RPC message structure as expected by commlib
        # Based on RPCClient._prepare_request method
        message = {
            "header": {
                "timestamp": int(time.time() * 1000),  # Milliseconds
                "reply_to": f"hummingbot-api-response-{int(time.time() * 1000)}",  # Unique response topic
                "msg_id": int(time.time() * 1000),
                "node_id": "hummingbot-api",
                "agent": "hummingbot-api",
                "properties": {},
            },
            "data": data or {},
        }

        try:
            await self._client.publish(topic, payload=json.dumps(message), qos=qos)
            return True
        except Exception as e:
            logger.error(f"Failed to publish command to {bot_id}: {e}")
            return False

    async def publish_raw(self, topic: str, data: Dict[str, Any], qos: int = 1) -> bool:
        """Publish a raw JSON payload to an arbitrary MQTT topic."""
        if not self._connected or not self._client:
            logger.error("Not connected to MQTT broker")
            return False

        try:
            await self._client.publish(topic, payload=json.dumps(data), qos=qos)
            return True
        except Exception as e:
            logger.error(f"Failed to publish raw MQTT message to {topic}: {e}")
            return False

    def add_handler(self, topic_pattern: str, handler: Callable):
        """
        Add a custom message handler for a topic pattern.

        :param topic_pattern: Topic pattern (supports + wildcard)
        :param handler: Callback function(bot_id, channel, data) - can be sync or async
        """
        self._handlers[topic_pattern] = handler

    def remove_handler(self, topic_pattern: str):
        """Remove a message handler."""
        self._handlers.pop(topic_pattern, None)

    def get_bot_controller_reports(self, bot_id: str) -> Dict[str, Any]:
        """Get controller reports for a bot.

        Returns:
            Dict with controller_id as key and report dict as value.
            Each report contains 'performance' and 'custom_info' keys.
        """
        return self._bot_controller_reports.get(bot_id, {})

    def get_bot_logs(self, bot_id: str) -> list:
        """Get recent logs for a bot."""
        return list(self._bot_logs.get(bot_id, []))

    def get_bot_error_logs(self, bot_id: str) -> list:
        """Get recent error logs for a bot."""
        return list(self._bot_error_logs.get(bot_id, []))

    def clear_bot_data(self, bot_id: str):
        """Clear stored data for a bot."""
        self._bot_controller_reports.pop(bot_id, None)
        self._bot_logs.pop(bot_id, None)
        self._bot_error_logs.pop(bot_id, None)
        self._discovered_bots.pop(bot_id, None)

    def clear_bot_controller_reports(self, bot_id: str):
        """Clear only controller report data for a bot (useful when bot is stopped)."""
        self._bot_controller_reports.pop(bot_id, None)


    def get_discovered_bots(self, timeout_seconds: int = 300) -> list:
        """Get list of auto-discovered bots.

        :param timeout_seconds: Consider bots inactive after this many seconds without messages
        :return: List of active bot IDs
        """
        current_time = time.time()
        active_bots = [
            bot_id for bot_id, last_seen in self._discovered_bots.items() if current_time - last_seen < timeout_seconds
        ]
        return active_bots

    async def subscribe_to_bot(self, bot_id: str):
        """Subscribe to all topics for a specific bot."""
        # Convert dots to slashes for MQTT topic
        mqtt_bot_id = bot_id.replace(".", "/")

        # Subscribe to all topics for this specific bot
        topic = f"hbot/{mqtt_bot_id}/#"
        await self.subscribe(topic)


if __name__ == "__main__":
    # Example usage
    import sys

    # For Windows compatibility
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    logging.basicConfig(level=logging.INFO)

    async def main():
        mqtt_manager = MQTTManager(host="localhost", port=1883, username="", password="")

        await mqtt_manager.start()

        try:
            # Keep running to listen for messages
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            await mqtt_manager.stop()

    asyncio.run(main())
