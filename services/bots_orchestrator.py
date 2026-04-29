import asyncio
import logging
import os
import time
from typing import Optional
import re

import docker
from sqlalchemy import text

from utils.mqtt_manager import MQTTManager
from utils.hummingbot_database_reader import HummingbotDatabase

logger = logging.getLogger(__name__)


# HummingbotPerformanceListener class is no longer needed
# All functionality is now handled by MQTTManager


class BotsOrchestrator:
    """Orchestrates Hummingbot instances using Docker and MQTT communication."""

    def __init__(self, broker_host, broker_port, broker_username, broker_password):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.broker_username = broker_username
        self.broker_password = broker_password

        # Initialize Docker client
        self.docker_client = docker.from_env()

        # Initialize MQTT manager
        self.mqtt_manager = MQTTManager(host=broker_host, port=broker_port, username=broker_username, password=broker_password)

        # Active bots tracking
        self.active_bots = {}
        self._update_bots_task: Optional[asyncio.Task] = None
        
        # Track bots that are currently being stopped and archived
        self.stopping_bots = set()

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
        Get bot trade history regardless of whether the bot is running,
        stopped, or archived.
        """
        db_manager = kwargs.pop("db_manager", None)
        if db_manager:
            postgres_history = await self._get_bot_history_from_postgres(bot_name, db_manager, **kwargs)
            if postgres_history["success"]:
                return postgres_history

        db_history = self._get_bot_history_from_database(bot_name, **kwargs)
        if db_history["success"]:
            return db_history

        container_state = self._get_container_state(bot_name)
        if container_state.get("exists") and not container_state.get("running"):
            return self._empty_bot_history_response(
                bot_name=bot_name,
                source="database",
                message=f"No persisted trade database found for stopped bot {bot_name}",
                searched_paths=db_history.get("searched_paths", []),
            )

        if bot_name not in self.active_bots:
            logger.warning(f"Bot {bot_name} not found in active bots and no database history was found")
            return self._empty_bot_history_response(
                bot_name=bot_name,
                source="database",
                message=f"Bot {bot_name} not found in active bots or persisted bot databases",
                searched_paths=db_history.get("searched_paths", []),
            )

        # Fall back to the live Hummingbot history command only when no DB exists.
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

    async def _get_bot_history_from_postgres(self, bot_name: str, db_manager, **kwargs) -> dict:
        """Read trade history from PostgreSQL for Hummingbot-native or API-managed schemas."""
        identifiers = await self._get_bot_history_identifiers(bot_name, db_manager)
        native_history = await self._get_native_postgres_trade_fills(bot_name, db_manager, identifiers, **kwargs)
        if native_history["success"]:
            return native_history

        custom_history = await self._get_api_postgres_trades(bot_name, db_manager, identifiers, **kwargs)
        if custom_history["success"]:
            return custom_history

        return {
            "success": False,
            "message": "No PostgreSQL trade history found",
            "postgres_errors": [native_history, custom_history],
        }

    async def _get_bot_history_identifiers(self, bot_name: str, db_manager) -> dict:
        identifiers = {
            "bot_name": bot_name,
            "config_names": {bot_name, f"{bot_name}.yml"},
            "account_names": set(),
        }

        try:
            async with db_manager.get_session_context() as session:
                result = await session.execute(
                    text(
                        "SELECT config_name, account_name FROM bot_runs "
                        "WHERE bot_name = :bot_name OR instance_name = :bot_name"
                    ),
                    {"bot_name": bot_name},
                )
                for config_name, account_name in result.fetchall():
                    if config_name:
                        identifiers["config_names"].add(config_name)
                        identifiers["config_names"].add(os.path.basename(config_name))
                    if account_name:
                        identifiers["account_names"].add(account_name)
        except Exception as e:
            logger.debug(f"Could not load bot run identifiers for {bot_name}: {e}")

        return identifiers

    async def _get_native_postgres_trade_fills(self, bot_name: str, db_manager, identifiers: dict, **kwargs) -> dict:
        table_name = await self._find_postgres_table(
            db_manager,
            ["TradeFill", "tradefill", "trade_fill", "hummingbot_trade_fills"],
        )
        if not table_name:
            return {"success": False, "message": "No native Hummingbot TradeFill table found"}

        where_clauses = []
        config_clauses = []
        params = {}
        config_names = sorted(identifiers.get("config_names", set()))
        for idx, config_name in enumerate(config_names):
            exact_key = f"config_exact_{idx}"
            like_key = f"config_like_{idx}"
            params[exact_key] = config_name
            params[like_key] = f"%{config_name}%"
            config_clauses.append(f"(config_file_path = :{exact_key} OR config_file_path LIKE :{like_key})")

        allow_unfiltered = bool(kwargs.get("allow_unfiltered", False))
        if config_clauses and not allow_unfiltered:
            where_clauses.append(f"({' OR '.join(config_clauses)})")
        elif not allow_unfiltered:
            where_clauses.append("(config_file_path LIKE :bot_name_like)")
            params["bot_name_like"] = f"%{bot_name}%"

        days = kwargs.get("days", 0) or 0
        if days > 0:
            params["cutoff_ms"] = int((time.time() - (days * 24 * 60 * 60)) * 1000)
            where_clauses.append("timestamp >= :cutoff_ms")

        limit = kwargs.get("limit")
        offset = max(int(kwargs.get("offset", 0) or 0), 0)
        params["offset"] = offset

        pagination_clause = "OFFSET :offset"
        if limit is not None:
            params["limit"] = max(int(limit), 0)
            pagination_clause = "LIMIT :limit OFFSET :offset"

        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"
        table_sql = self._quote_table_name(table_name)

        try:
            async with db_manager.get_session_context() as session:
                count_result = await session.execute(
                    text(f"SELECT COUNT(*) FROM {table_sql} WHERE {where_sql}"),
                    params,
                )
                total = count_result.scalar() or 0
                result = await session.execute(
                    text(
                        "SELECT config_file_path, strategy, market AS connector_name, symbol AS trading_pair, "
                        "base_asset, quote_asset, timestamp, order_id, trade_type, order_type, price, amount, "
                        "leverage, trade_fee, trade_fee_in_quote, exchange_trade_id, position "
                        f"FROM {table_sql} WHERE {where_sql} ORDER BY timestamp ASC {pagination_clause}"
                    ),
                    params,
                )
                rows = [self._normalize_native_trade_fill(row._mapping) for row in result.fetchall()]

            if total == 0:
                sample_configs = await self._get_native_trade_fill_config_samples(db_manager, table_name)
                return {
                    "success": False,
                    "message": f"No native PostgreSQL trade fills found for {bot_name}",
                    "table": table_name,
                    "config_names_used": config_names,
                    "sample_config_file_paths": sample_configs,
                }

            return {
                "success": True,
                "source": "postgresql_native",
                "bot_name": bot_name,
                "table": table_name,
                "filtered": not allow_unfiltered,
                "data": {
                    "trades": rows,
                    "pagination": {
                        "total": total,
                        "limit": params.get("limit"),
                        "offset": offset,
                        "has_more": offset + len(rows) < total,
                    },
                },
            }
        except Exception as e:
            logger.error(f"Error reading native PostgreSQL trade history for {bot_name}: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _get_native_trade_fill_config_samples(self, db_manager, table_name: str) -> list[str]:
        table_sql = self._quote_table_name(table_name)
        try:
            async with db_manager.get_session_context() as session:
                result = await session.execute(
                    text(
                        f"SELECT DISTINCT config_file_path FROM {table_sql} "
                        "WHERE config_file_path IS NOT NULL ORDER BY config_file_path LIMIT 20"
                    )
                )
                return [str(row[0]) for row in result.fetchall() if row[0]]
        except Exception as e:
            logger.debug(f"Could not sample TradeFill config paths from {table_name}: {e}")
            return []

    async def _get_api_postgres_trades(self, bot_name: str, db_manager, identifiers: dict, **kwargs) -> dict:
        account_names = sorted(identifiers.get("account_names", set()))
        if not account_names:
            return {"success": False, "message": "No account name found for API trade lookup"}

        where_clauses = []
        params = {}
        for idx, account_name in enumerate(account_names):
            key = f"account_name_{idx}"
            params[key] = account_name
            where_clauses.append(f"o.account_name = :{key}")

        days = kwargs.get("days", 0) or 0
        if days > 0:
            params["cutoff_seconds"] = time.time() - (days * 24 * 60 * 60)
            where_clauses.append("EXTRACT(EPOCH FROM t.timestamp) >= :cutoff_seconds")

        limit = kwargs.get("limit")
        offset = max(int(kwargs.get("offset", 0) or 0), 0)
        params["offset"] = offset
        pagination_clause = "OFFSET :offset"
        if limit is not None:
            params["limit"] = max(int(limit), 0)
            pagination_clause = "LIMIT :limit OFFSET :offset"

        where_sql = " AND ".join(f"({clause})" for clause in where_clauses)

        try:
            async with db_manager.get_session_context() as session:
                count_result = await session.execute(
                    text(f"SELECT COUNT(*) FROM trades t JOIN orders o ON t.order_id = o.id WHERE {where_sql}"),
                    params,
                )
                total = count_result.scalar() or 0
                result = await session.execute(
                    text(
                        "SELECT t.trade_id, o.client_order_id AS order_id, o.account_name, o.connector_name, "
                        "t.trading_pair, t.trade_type, t.amount, t.price, t.fee_paid, t.fee_currency, t.timestamp "
                        f"FROM trades t JOIN orders o ON t.order_id = o.id WHERE {where_sql} "
                        f"ORDER BY t.timestamp ASC {pagination_clause}"
                    ),
                    params,
                )
                rows = [self._normalize_api_trade(row._mapping) for row in result.fetchall()]

            if total == 0:
                return {"success": False, "message": f"No API PostgreSQL trades found for {bot_name}"}

            return {
                "success": True,
                "source": "postgresql_api",
                "bot_name": bot_name,
                "data": {
                    "trades": rows,
                    "pagination": {
                        "total": total,
                        "limit": params.get("limit"),
                        "offset": offset,
                        "has_more": offset + len(rows) < total,
                    },
                },
            }
        except Exception as e:
            logger.error(f"Error reading API PostgreSQL trade history for {bot_name}: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    @staticmethod
    async def _find_postgres_table(db_manager, candidates: list[str]) -> Optional[str]:
        try:
            params = {f"candidate_{idx}": candidate for idx, candidate in enumerate(candidates)}
            placeholders = ", ".join(f":candidate_{idx}" for idx in range(len(candidates)))
            async with db_manager.get_session_context() as session:
                result = await session.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        f"WHERE table_schema = 'public' AND table_name IN ({placeholders})"
                    ),
                    params,
                )
                row = result.first()
                return row[0] if row else None
        except Exception as e:
            logger.debug(f"Could not inspect PostgreSQL tables: {e}")
            return None

    @staticmethod
    def _quote_table_name(table_name: str) -> str:
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name):
            raise ValueError(f"Unsafe table name: {table_name}")
        return f'public."{table_name}"'

    @staticmethod
    def _scaled_number(value):
        if value is None:
            return None
        return float(value) / 1e6

    @classmethod
    def _normalize_native_trade_fill(cls, row) -> dict:
        trade_fee_in_quote = row.get("trade_fee_in_quote")
        return {
            "config_file_path": row.get("config_file_path"),
            "strategy": row.get("strategy"),
            "connector_name": row.get("connector_name"),
            "trading_pair": row.get("trading_pair"),
            "base_asset": row.get("base_asset"),
            "quote_asset": row.get("quote_asset"),
            "timestamp": row.get("timestamp"),
            "order_id": row.get("order_id"),
            "trade_type": row.get("trade_type"),
            "order_type": row.get("order_type"),
            "price": cls._scaled_number(row.get("price")),
            "amount": cls._scaled_number(row.get("amount")),
            "leverage": row.get("leverage"),
            "trade_fee": row.get("trade_fee"),
            "trade_fee_in_quote": cls._scaled_number(trade_fee_in_quote),
            "trade_fee_amount": cls._scaled_number(trade_fee_in_quote),
            "exchange_trade_id": row.get("exchange_trade_id"),
            "position": row.get("position"),
        }

    @staticmethod
    def _normalize_api_trade(row) -> dict:
        timestamp = row.get("timestamp")
        return {
            "trade_id": row.get("trade_id"),
            "order_id": row.get("order_id"),
            "account_name": row.get("account_name"),
            "connector_name": row.get("connector_name"),
            "trading_pair": row.get("trading_pair"),
            "trade_type": row.get("trade_type"),
            "amount": float(row.get("amount")) if row.get("amount") is not None else None,
            "price": float(row.get("price")) if row.get("price") is not None else None,
            "fee_paid": float(row.get("fee_paid")) if row.get("fee_paid") is not None else None,
            "fee_currency": row.get("fee_currency"),
            "timestamp": timestamp.isoformat() if hasattr(timestamp, "isoformat") else timestamp,
        }

    @staticmethod
    def _empty_bot_history_response(
        bot_name: str,
        source: str,
        message: str,
        searched_paths: Optional[list[str]] = None,
    ) -> dict:
        return {
            "success": True,
            "source": source,
            "bot_name": bot_name,
            "message": message,
            "searched_paths": searched_paths or [],
            "data": {
                "trades": [],
                "pagination": {
                    "total": 0,
                    "limit": None,
                    "offset": 0,
                    "has_more": False,
                },
            },
        }

    def _get_bot_history_from_database(self, bot_name: str, **kwargs) -> dict:
        """Read raw trade fills from a bot SQLite DB in active or archived storage."""
        db_path, searched_paths = self._find_bot_database(bot_name)
        if not db_path:
            return {"success": False, "searched_paths": searched_paths}

        try:
            trades = HummingbotDatabase(db_path).get_trade_fills()
            if "timestamp" in trades.columns:
                trades = trades.sort_values("timestamp")

            days = kwargs.get("days", 0) or 0
            if days > 0 and "timestamp" in trades.columns:
                cutoff_ms = int((time.time() - (days * 24 * 60 * 60)) * 1000)
                trades = trades[trades["timestamp"] >= cutoff_ms]

            total = len(trades)
            offset = max(int(kwargs.get("offset", 0) or 0), 0)
            limit = kwargs.get("limit")
            if limit is not None:
                limit = max(int(limit), 0)
                trades = trades.iloc[offset:offset + limit]
            elif offset:
                trades = trades.iloc[offset:]

            return {
                "success": True,
                "source": "database",
                "bot_name": bot_name,
                "db_path": db_path,
                "data": {
                    "trades": trades.fillna(0).to_dict("records"),
                    "pagination": {
                        "total": total,
                        "limit": limit,
                        "offset": offset,
                        "has_more": (offset + len(trades)) < total,
                    },
                },
            }
        except Exception as e:
            logger.error(f"Error reading trade history database for {bot_name}: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Error reading persisted trade history for {bot_name}: {e}",
                "db_path": db_path,
                "searched_paths": searched_paths,
            }

    @staticmethod
    def _find_bot_database(bot_name: str) -> tuple[Optional[str], list[str]]:
        searched_paths = [
            os.path.join(instance_dir, "data")
            for instance_dir in BotsOrchestrator._get_bot_storage_dirs(bot_name)
        ]

        for data_dir in searched_paths:
            if not os.path.isdir(data_dir):
                continue

            expected_name = f"{bot_name}.sqlite"
            expected_path = os.path.join(data_dir, expected_name)
            if os.path.isfile(expected_path):
                return expected_path, searched_paths

            sqlite_files = [
                os.path.join(data_dir, filename)
                for filename in os.listdir(data_dir)
                if filename.endswith(".sqlite")
            ]
            if sqlite_files:
                sqlite_files.sort(key=os.path.getmtime, reverse=True)
                return sqlite_files[0], searched_paths

        return None, searched_paths

    @staticmethod
    def _get_bot_storage_dirs(bot_name: str) -> list[str]:
        return [
            os.path.join("bots", "instances", bot_name),
            os.path.join("bots", "archived", bot_name),
        ]

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
    
