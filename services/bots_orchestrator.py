import asyncio
import logging
import os
from datetime import datetime
from typing import Optional
import re

import docker

from utils.mqtt_manager import MQTTManager

logger = logging.getLogger(__name__)


# HummingbotPerformanceListener class is no longer needed
# All functionality is now handled by MQTTManager


class BotsOrchestrator:
    """Orchestrates Hummingbot instances using Docker and MQTT communication."""

    def __init__(
        self,
        broker_host,
        broker_port,
        broker_username,
        broker_password,
        broker_ssl=False,
        docker_service: 'DockerService' = None,
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.broker_username = broker_username
        self.broker_password = broker_password
        self.broker_ssl = broker_ssl
        self.docker_service = docker_service

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

        # MQTT manager will be started asynchronously later

    def stop_container(self, bot_name: str):
        """Stop the Docker container for a bot."""
        if self.docker_service:
            return self.docker_service.stop_container(bot_name)
        return {"success": False, "message": "DockerService not initialized in BotsOrchestrator"}

    def start_container(self, bot_name: str):
        """Start the Docker container for a bot."""
        if self.docker_service:
            return self.docker_service.start_container(bot_name)
        return {"success": False, "message": "DockerService not initialized in BotsOrchestrator"}

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

    def _get_container_state(self, bot_name: str) -> dict:
        """Return Docker state for a bot container, including stopped containers."""
        try:
            container = self.docker_client.containers.get(bot_name)
            container.reload()
            state = container.attrs.get("State", {})
            return {
                "exists": True,
                "id": container.id,
                "name": container.name,
                "status": state.get("Status") or container.status,
                "running": state.get("Running", container.status == "running"),
                "exit_code": state.get("ExitCode"),
                "started_at": state.get("StartedAt"),
                "finished_at": state.get("FinishedAt"),
                "image": container.image.tags[0] if container.image.tags else container.image.id[:12],
            }
        except docker.errors.NotFound:
            return {"exists": False, "status": "not_found", "running": False}
        except docker.errors.DockerException as e:
            logger.warning(f"Unable to inspect container {bot_name}: {e}")
            return {"exists": False, "status": "unknown", "running": False, "error": str(e)}

    def start(self):
        """Start the loop that monitors active bots."""
        # Start MQTT manager and update loop in async context
        self._update_bots_task = asyncio.create_task(self._start_async())

    async def _start_async(self):
        """Start MQTT manager and update loop asynchronously."""
        logger.info("Starting MQTT manager...")
        await self.mqtt_manager.start()

        # Then start the update loop
        await self.update_active_bots()

    def stop(self):
        """Stop the active bots monitoring loop."""
        if self._update_bots_task:
            self._update_bots_task.cancel()
        self._update_bots_task = None

        # Stop MQTT manager asynchronously
        asyncio.create_task(self.mqtt_manager.stop())

    async def update_active_bots(self, sleep_time=1.0):
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
                        # Subscribe to this specific bot's topics
                        await self.mqtt_manager.subscribe_to_bot(bot_name)

            except Exception as e:
                logger.error(f"Error in update_active_bots: {e}", exc_info=True)

            await asyncio.sleep(sleep_time)

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
    def _get_bot_storage_dirs(bot_name: str) -> list[str]:
        return [
            os.path.join("bots", "instances", bot_name),
            os.path.join("bots", "archived", bot_name),
        ]

    @classmethod
    def _bot_storage_exists(cls, bot_name: str) -> bool:
        return any(os.path.isdir(storage_dir) for storage_dir in cls._get_bot_storage_dirs(bot_name))

    @classmethod
    def _get_bot_logs_from_files(cls, bot_name: str, max_lines_per_file: int = 1000) -> tuple[list[dict], list[dict]]:
        general_logs = []
        error_logs = []

        for storage_dir in cls._get_bot_storage_dirs(bot_name):
            logs_dir = os.path.join(storage_dir, "logs")
            if not os.path.isdir(logs_dir):
                continue

            general_candidates = [
                os.path.join(logs_dir, f"logs_{bot_name}.log"),
                os.path.join(logs_dir, "logs_hummingbot.log"),
            ]
            for path in general_candidates:
                general_logs.extend(cls._read_log_file(path, max_lines_per_file=max_lines_per_file))

            error_logs.extend(
                cls._read_log_file(
                    os.path.join(logs_dir, "errors.log"),
                    max_lines_per_file=max_lines_per_file,
                    level_name="ERROR",
                )
            )

        return general_logs, error_logs

    @staticmethod
    def _read_log_file(path: str, max_lines_per_file: int = 1000, level_name: str = "INFO") -> list[dict]:
        if not os.path.isfile(path):
            return []

        try:
            with open(path, "r", encoding="utf-8", errors="replace") as file:
                lines = file.readlines()
        except OSError as e:
            logger.warning(f"Unable to read log file {path}: {e}")
            return []

        start_line = max(len(lines) - max_lines_per_file, 0)
        
        # Regex to parse Hummingbot log format: 2026-04-28 15:34:01,942 - 17 - logger.name - LEVEL - message
        log_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - \d+ - (.*?) - (DEBUG|INFO|WARNING|ERROR|CRITICAL|EVENT_LOG) - (.*)$")
        
        parsed_logs = []
        for index, line in enumerate(lines[start_line:]):
            line = line.strip()
            if not line:
                continue
            if BotsOrchestrator._should_skip_log_line(line):
                continue
                
            match = log_pattern.match(line)
            if match:
                ts_str, logger_name, level, msg = match.groups()
                # Convert "2026-04-28 15:34:01,942" to float timestamp
                try:
                    # Replacing comma with dot for fractional seconds
                    ts_dt = datetime.strptime(ts_str.replace(",", "."), "%Y-%m-%d %H:%M:%S.%f")
                    timestamp = ts_dt.timestamp()
                except ValueError:
                    timestamp = None
                
                parsed_logs.append({
                    "level_name": level,
                    "msg": msg,
                    "timestamp": timestamp,
                    "logger_name": logger_name,
                    "source": "file",
                    "file": path,
                    "line_number": start_line + index + 1,
                })
            else:
                # Fallback for lines that don't match the standard format (e.g. stack traces)
                parsed_logs.append({
                    "level_name": level_name,
                    "msg": line,
                    "timestamp": None,
                    "source": "file",
                    "file": path,
                    "line_number": start_line + index + 1,
                })
                
        return parsed_logs

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
        # TODO: improve logic of bots state management
        """Get status information for all active bots."""
        all_bots_status = {}
        for bot in [bot for bot in self.active_bots if not self.is_bot_stopping(bot)]:
            status = self.get_bot_status(bot)
            status["source"] = self.active_bots[bot].get("source", "unknown")
            all_bots_status[bot] = status
        return all_bots_status

    def get_bot_status(self, bot_name):
        """
        Get status information for a specific bot.
        """
        try:
            container_state = self._get_container_state(bot_name)
            bot_is_active = bot_name in self.active_bots
            bot_has_storage = self._bot_storage_exists(bot_name)

            if not bot_is_active and not container_state.get("exists") and not bot_has_storage:
                return {
                    "status": "not_found",
                    "error": f"Bot {bot_name} not found",
                    "container": container_state,
                }

            # Check if bot is currently being stopped and archived
            if bot_name in self.stopping_bots:
                file_general_logs, file_error_logs = self._get_bot_logs_from_files(bot_name)
                return {
                    "status": "stopping",
                    "message": "Bot is currently being stopped and archived",
                    "container": container_state,
                    "performance": {},
                    "error_logs": file_error_logs,
                    "general_logs": file_general_logs,
                    "recently_active": False,
                }
            
            # Get data from MQTT manager
            controller_reports = self.mqtt_manager.get_bot_controller_reports(bot_name)
            performance = self.determine_controller_performance(controller_reports)
            
            # Retrieve logs only from files
            general_logs, error_logs = self._get_bot_logs_from_files(bot_name)

            # Check if bot has sent recent messages (within last 30 seconds)
            discovered_bots = self.mqtt_manager.get_discovered_bots(timeout_seconds=30)
            recently_active = bot_name in discovered_bots

            # Docker is the source of truth for whether the instance process exists.
            if container_state.get("exists") and not container_state.get("running"):
                status = "stopped"
            elif container_state.get("exists") and not bot_is_active:
                status = "container_running"
            elif bot_has_storage and not bot_is_active:
                status = "archived"
            elif len(performance) > 0 and recently_active:
                status = "running"
            elif len(performance) > 0 and not recently_active:
                status = "idle"  # Has performance data but no recent activity
            else:
                status = "stopped"

            return {
                "status": status,
                "container": container_state,
                "performance": performance,
                "error_logs": error_logs,
                "general_logs": general_logs,
                "recently_active": recently_active,
                "source": self.active_bots.get(bot_name, {}).get(
                    "source", "docker" if container_state.get("exists") else "unknown"
                ),
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
    
