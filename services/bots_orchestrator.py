import asyncio
import logging
from typing import Optional
import re

import docker

from utils.mqtt_manager import MQTTManager

logger = logging.getLogger(__name__)


# HummingbotPerformanceListener class is no longer needed
# All functionality is now handled by MQTTManager


class BotsOrchestrator:
    """Orchestrates Hummingbot instances using Docker and MQTT communication."""

    def __init__(self, broker_host, broker_port, broker_username, broker_password, broker_ssl=False):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.broker_username = broker_username
        self.broker_password = broker_password
        self.broker_ssl = broker_ssl

        # Initialize Docker client
        self.docker_client = docker.from_env()

        # Initialize MQTT manager
        self.mqtt_manager = MQTTManager(
            host=broker_host,
            port=broker_port,
            username=broker_username,
            password=broker_password,
            ssl=broker_ssl,
        )

        # Active bots tracking
        self.active_bots = {}
        self._update_bots_task: Optional[asyncio.Task] = None

        # Track bots that are currently being stopped and archived
        self.stopping_bots = set()

        # Bots that have been deployed but not yet discovered via Docker/MQTT.
        # Keys are instance_name strings; values are deployment metadata dicts.
        # Entries are removed once the bot appears in active_bots.
        self.pending_bots: dict = {}

        # MQTT manager will be started asynchronously later

    @staticmethod
    def hummingbot_containers_fiter(container):
        """Filter for Hummingbot containers based on image name pattern."""
        try:
            # Get the image name (first tag if available, otherwise the image ID)
            image_name = container.image.tags[0] if container.image.tags else str(container.image)
            pattern = r'.+/hummingbot:'
            return bool(re.match(pattern, image_name))
        except Exception:
            return False

    async def get_active_containers(self):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_get_active_containers)

    def _sync_get_active_containers(self):
        return [
            container.name
            for container in self.docker_client.containers.list()
            if container.status == "running" and self.hummingbot_containers_fiter(container)
        ]

    def start(self):
        """Start the loop that monitors active bots."""
        # Start MQTT manager and update loop in async context
        self._update_bots_task = asyncio.create_task(self._start_async())

    async def _start_async(self):
        """Start MQTT manager and update loop asynchronously."""
        logger.info("Starting MQTT manager...")
        await self.mqtt_manager.start()

        # Start the Docker event listener task
        asyncio.create_task(self._docker_event_listener())

        # Then start the update loop
        await self.update_active_bots()

    def stop(self):
        """Stop the active bots monitoring loop."""
        if self._update_bots_task:
            self._update_bots_task.cancel()
        self._update_bots_task = None

        # Stop MQTT manager asynchronously
        asyncio.create_task(self.mqtt_manager.stop())

    async def update_active_bots(self, sleep_time=30.0):
        """Monitor and update active bots list using both Docker and MQTT discovery."""
        while True:
            try:
                # Get bots from Docker containers
                docker_bots = await self.get_active_containers()

                # Get bots from MQTT messages (auto-discovered)
                mqtt_bots = self.mqtt_manager.get_discovered_bots(timeout_seconds=30)  # 30 second timeout

                # Combine both sources
                all_active_bots = set([bot for bot in docker_bots + mqtt_bots if not self.is_bot_stopping(bot)])

                # Remove bots that are no longer active
                for bot_name in list(self.active_bots):
                    if bot_name not in all_active_bots:
                        self.mqtt_manager.clear_bot_data(bot_name)
                        del self.active_bots[bot_name]

                # Add new bots
                for bot_name in all_active_bots:
                    if bot_name not in self.active_bots:
                        self.active_bots[bot_name] = {
                            "bot_name": bot_name,
                            "status": "connected",
                            "source": "docker" if bot_name in docker_bots else "mqtt",
                        }
                        # Promote from pending if it was waiting
                        self.pending_bots.pop(bot_name, None)
                        # Subscribe to this specific bot's topics
                        await self.mqtt_manager.subscribe_to_bot(bot_name)

            except Exception as e:
                logger.error(f"Error in update_active_bots: {e}", exc_info=True)

            await asyncio.sleep(sleep_time)

    async def _docker_event_listener(self):
        """Listen to Docker events in real-time."""
        while True:
            try:
                loop = asyncio.get_event_loop()
                # Run the blocking events() call in a separate thread/executor
                event_stream = await loop.run_in_executor(
                    None,
                    lambda: self.docker_client.events(decode=True, filters={"type": "container"})
                )

                def get_next_event(stream):
                    try:
                        return next(stream)
                    except StopIteration:
                        return None

                while True:
                    event = await loop.run_in_executor(None, get_next_event, event_stream)
                    if event is None:
                        break

                    action = event.get("action")
                    actor = event.get("Actor", {})
                    attributes = actor.get("Attributes", {})
                    name = attributes.get("name")

                    if not name:
                        continue

                    if action == "start":
                        await self._on_container_started(name)
                    elif action in ("die", "stop", "kill"):
                        exit_code = int(attributes.get("exitCode", 0))
                        await self._on_container_stopped(name, exit_code)
            except Exception as e:
                logger.error(f"Docker event listener error: {e}. Restarting in 5s...", exc_info=True)
                await asyncio.sleep(5)

    async def _on_container_started(self, name: str):
        """Handle container start event."""
        try:
            if self.is_bot_stopping(name):
                return

            loop = asyncio.get_event_loop()
            container = await loop.run_in_executor(None, self.docker_client.containers.get, name)
            if self.hummingbot_containers_fiter(container):
                if name not in self.active_bots:
                    self.active_bots[name] = {
                        "bot_name": name,
                        "status": "connected",
                        "source": "docker",
                    }
                    await self.mqtt_manager.subscribe_to_bot(name)
                # Promote from pending if it was waiting
                self.pending_bots.pop(name, None)
        except Exception as e:
            # Container might have disappeared quickly
            logger.debug(f"Could not process start event for {name}: {e}")

    async def _on_container_stopped(self, name: str, exit_code: int):
        """Handle container stop/die event."""
        try:
            if name in self.active_bots:
                del self.active_bots[name]

            if exit_code != 0 and name in self.pending_bots:
                self.mark_pending_bot_failed(name, f"Container exited with code {exit_code}")

            self.mqtt_manager.clear_bot_data(name)
        except Exception as e:
            logger.error(f"Error in _on_container_stopped for {name}: {e}", exc_info=True)

    # ---------------------------------------------------------------------------
    # Pending bot registry
    # ---------------------------------------------------------------------------

    def register_pending_bot(self, instance_name: str, metadata: dict) -> None:
        """Register a newly-deployed bot so it shows up in status before Docker/MQTT pick it up."""
        if instance_name not in self.active_bots:
            self.pending_bots[instance_name] = {"status": "deploying", **metadata}
            logger.info(f"Registered pending bot: {instance_name}")

    def mark_pending_bot_failed(self, instance_name: str, error: str) -> None:
        """Update a pending bot's status to 'failed' (container crashed on start)."""
        if instance_name in self.pending_bots:
            self.pending_bots[instance_name]["status"] = "failed"
            self.pending_bots[instance_name]["error"] = error
            logger.info(f"Marked pending bot as failed: {instance_name}")

    def resolve_pending_bot(self, instance_name: str) -> None:
        """Remove a bot from the pending registry (used when it is confirmed running)."""
        self.pending_bots.pop(instance_name, None)

    # Interact with a specific bot
    async def start_bot(self, bot_name, **kwargs):
        """
        Start a bot with optional script.
        Maintains backward compatibility with kwargs.
        """
        if bot_name not in self.active_bots:
            logger.warning(f"Bot {bot_name} not found in active bots")
            return {"success": False, "message": f"Bot {bot_name} not found"}

        # Create StartCommandMessage.Request format
        data = {
            "log_level": kwargs.get("log_level"),
            "script": kwargs.get("script"),
            "conf": kwargs.get("conf"),
            "is_quickstart": kwargs.get("is_quickstart", False),
            "async_backend": kwargs.get("async_backend", True),
        }

        success = await self.mqtt_manager.publish_command(bot_name, "start", data)
        return {"success": success}

    async def stop_bot(self, bot_name, **kwargs):
        """
        Stop a bot.
        Maintains backward compatibility with kwargs.
        """
        if bot_name not in self.active_bots:
            logger.warning(f"Bot {bot_name} not found in active bots")
            return {"success": False, "message": f"Bot {bot_name} not found"}

        # Create StopCommandMessage.Request format
        data = {
            "skip_order_cancellation": kwargs.get("skip_order_cancellation", False),
            "async_backend": kwargs.get("async_backend", True),
        }

        success = await self.mqtt_manager.publish_command(bot_name, "stop", data)

        # Clear performance data after stop command to immediately reflect stopped status
        if success:
            self.mqtt_manager.clear_bot_controller_reports(bot_name)

        return {"success": success}

    async def import_strategy_for_bot(self, bot_name, strategy, **kwargs):
        """
        Import a strategy configuration for a bot.
        Maintains backward compatibility.
        """
        if bot_name not in self.active_bots:
            logger.warning(f"Bot {bot_name} not found in active bots")
            return {"success": False, "message": f"Bot {bot_name} not found"}

        # Create ImportCommandMessage.Request format
        data = {"strategy": strategy}
        success = await self.mqtt_manager.publish_command(bot_name, "import_strategy", data)
        return {"success": success}

    async def configure_bot(self, bot_name, params, **kwargs):
        """
        Configure bot parameters.
        Maintains backward compatibility.
        """
        if bot_name not in self.active_bots:
            logger.warning(f"Bot {bot_name} not found in active bots")
            return {"success": False, "message": f"Bot {bot_name} not found"}

        # Create ConfigCommandMessage.Request format
        data = {"params": params}
        success = await self.mqtt_manager.publish_command(bot_name, "config", data)
        return {"success": success}

    async def get_bot_history(self, bot_name, **kwargs):
        """
        Request bot trading history and wait for the response.
        Maintains backward compatibility.
        """
        if bot_name not in self.active_bots:
            logger.warning(f"Bot {bot_name} not found in active bots")
            return {"success": False, "message": f"Bot {bot_name} not found"}

        # Create HistoryCommandMessage.Request format
        data = {
            "days": kwargs.get("days", 0),
            "verbose": kwargs.get("verbose", False),
            "precision": kwargs.get("precision"),
            "async_backend": kwargs.get("async_backend", False),
        }

        # Use the new RPC method to wait for response
        timeout = kwargs.get("timeout", 30.0)  # Default 30 second timeout
        response = await self.mqtt_manager.publish_command_and_wait(bot_name, "history", data, timeout=timeout)

        if response is None:
            return {
                "success": False,
                "message": f"No response received from {bot_name} within {timeout} seconds",
                "timeout": True,
            }

        return {"success": True, "data": response}

    @staticmethod
    def _should_skip_log_line(line: str) -> bool:
        return " - hummingbot.core.event.event_reporter - EVENT_LOG - " in line

    @staticmethod
    def determine_controller_performance(controller_reports):
        """Process controller reports and extract performance and custom_info.

        Args:
            controller_reports: Dict with controller_id as key and report dict as value.
                New format: Each report contains 'performance' and 'custom_info' keys.
                Old format: Report contains performance metrics directly (backward compatible).

        Returns:
            Dict with cleaned controller data including status, performance, and custom_info.
        """
        cleaned_data = {}
        for controller_id, report in controller_reports.items():
            try:
                # Support both new format (nested) and old format (flat)
                # New format: {"performance": {...}, "custom_info": {...}}
                # Old format: {...performance metrics directly...}
                if "performance" in report:
                    # New format with nested structure
                    performance = report.get("performance", {})
                    custom_info = report.get("custom_info", {})
                else:
                    # Old format - metrics are directly in the report
                    performance = report
                    custom_info = {}

                # Validate performance metrics are numeric (skip known non-numeric fields)
                non_numeric_fields = ("positions_summary", "close_type_counts")
                _ = sum(
                    metric for key, metric in performance.items()
                    if key not in non_numeric_fields and isinstance(metric, (int, float))
                )

                cleaned_data[controller_id] = {
                    "status": "running",
                    "performance": performance,
                    "custom_info": custom_info
                }
            except Exception as e:
                # Handle both formats in error case too
                if "performance" in report:
                    perf = report.get("performance", {})
                    info = report.get("custom_info", {})
                else:
                    perf = report
                    info = {}
                cleaned_data[controller_id] = {
                    "status": "error",
                    "error": f"Error processing controller data: {e}",
                    "performance": perf,
                    "custom_info": info
                }
        return cleaned_data

    def get_all_bots_status(self):
        """Get status information for all active bots (including recently-deployed pending bots)."""
        all_bots_status = {}

        # Active / live bots
        for bot in [bot for bot in self.active_bots if not self.is_bot_stopping(bot)]:
            status = self.get_bot_status(bot)
            status["source"] = self.active_bots[bot].get("source", "unknown")
            all_bots_status[bot] = status

        # Pending bots that haven't appeared yet
        for bot_name, meta in self.pending_bots.items():
            if bot_name not in all_bots_status:
                all_bots_status[bot_name] = {
                    "status": meta.get("status", "deploying"),
                    "error": meta.get("error"),
                    "performance": {},
                    "error_logs": [],
                    "general_logs": [],
                    "recently_active": False,
                    "source": "pending",
                }

        return all_bots_status

    def get_bot_status(self, bot_name):
        """
        Get status information for a specific bot.
        """
        if bot_name not in self.active_bots:
            return {"status": "not_found", "error": f"Bot {bot_name} not found"}

        try:
            # Check if bot is currently being stopped and archived
            if bot_name in self.stopping_bots:
                return {
                    "status": "stopping",
                    "message": "Bot is currently being stopped and archived",
                    "performance": {},
                    "error_logs": [],
                    "general_logs": [],
                    "recently_active": False,
                }
            
            # Get data from MQTT manager
            controller_reports = self.mqtt_manager.get_bot_controller_reports(bot_name)
            performance = self.determine_controller_performance(controller_reports)
            error_logs = self.mqtt_manager.get_bot_error_logs(bot_name)
            general_logs = self.mqtt_manager.get_bot_logs(bot_name)

            # Check if bot has sent recent messages (within last 30 seconds)
            discovered_bots = self.mqtt_manager.get_discovered_bots(timeout_seconds=30)
            recently_active = bot_name in discovered_bots

            # Determine status based on performance data and recent activity
            if len(performance) > 0 and recently_active:
                status = "running"
            elif len(performance) > 0 and not recently_active:
                status = "idle"  # Has performance data but no recent activity
            else:
                status = "stopped"

            return {
                "status": status,
                "performance": performance,
                "error_logs": error_logs,
                "general_logs": general_logs,
                "recently_active": recently_active,
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def set_bot_stopping(self, bot_name: str):
        """Mark a bot as currently being stopped and archived."""
        self.stopping_bots.add(bot_name)
        logger.info(f"Marked bot {bot_name} as stopping")
    
    def clear_bot_stopping(self, bot_name: str):
        """Clear the stopping status for a bot."""
        self.stopping_bots.discard(bot_name)
        logger.info(f"Cleared stopping status for bot {bot_name}")
    
    def is_bot_stopping(self, bot_name: str) -> bool:
        """Check if a bot is currently being stopped."""
        return bot_name in self.stopping_bots
    
