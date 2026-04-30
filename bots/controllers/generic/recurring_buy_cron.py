from datetime import datetime
from decimal import Decimal
from typing import Set
from zoneinfo import ZoneInfo

from pydantic import Field

from hummingbot.core.data_type.common import MarketDict, PositionMode, PriceType, TradeType
from hummingbot.strategy_v2.controllers import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


class RecurringBuyCronConfig(ControllerConfigBase):
    controller_name: str = "recurring_buy_cron"
    connector_name: str = "binance_perpetual"
    trading_pair: str = "WLD-USDT"
    position_mode: PositionMode = PositionMode.HEDGE
    leverage: int = 20
    amount_quote: Decimal = Decimal("10")
    cron_schedule: str = Field(
        default="0 * * * *",
        description="Five-field cron schedule: minute hour day-of-month month day-of-week.",
    )
    timezone: str = Field(
        default="UTC",
        description="IANA timezone used to evaluate the cron schedule, e.g. UTC or America/New_York.",
    )

    def update_markets(self, markets: MarketDict) -> MarketDict:
        return markets.add_or_update(self.connector_name, self.trading_pair)


class RecurringBuyCron(ControllerBase):
    def __init__(self, config: RecurringBuyCronConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self._cron = self._parse_cron_schedule(config.cron_schedule)
        self._timezone = ZoneInfo(config.timezone)
        self._last_triggered_minute = None

    @staticmethod
    def _parse_cron_field(field: str, minimum: int, maximum: int, name: str) -> Set[int]:
        values = set()
        for part in field.split(","):
            part = part.strip()
            if not part:
                raise ValueError(f"Invalid empty value in cron {name} field.")

            if "/" in part:
                range_part, step_part = part.split("/", 1)
                step = int(step_part)
                if step <= 0:
                    raise ValueError(f"Cron {name} step must be greater than zero.")
            else:
                range_part = part
                step = 1

            if range_part == "*":
                start, end = minimum, maximum
            elif "-" in range_part:
                start_part, end_part = range_part.split("-", 1)
                start, end = int(start_part), int(end_part)
            else:
                start = end = int(range_part)

            if start < minimum or end > maximum or start > end:
                raise ValueError(f"Cron {name} value must be between {minimum} and {maximum}.")

            values.update(range(start, end + 1, step))
        return values

    @classmethod
    def _parse_cron_schedule(cls, schedule: str) -> dict:
        fields = schedule.split()
        if len(fields) != 5:
            raise ValueError("cron_schedule must have five fields: minute hour day-of-month month day-of-week.")

        weekdays = cls._parse_cron_field(fields[4], 0, 7, "day-of-week")
        if 7 in weekdays:
            weekdays.add(0)
            weekdays.remove(7)

        return {
            "minutes": cls._parse_cron_field(fields[0], 0, 59, "minute"),
            "hours": cls._parse_cron_field(fields[1], 0, 23, "hour"),
            "days": cls._parse_cron_field(fields[2], 1, 31, "day-of-month"),
            "months": cls._parse_cron_field(fields[3], 1, 12, "month"),
            "weekdays": weekdays,
            "day_is_wildcard": fields[2] == "*",
            "weekday_is_wildcard": fields[4] == "*",
        }

    def _current_cron_minute(self) -> datetime:
        current_time = self.market_data_provider.time()
        current_datetime = datetime.fromtimestamp(current_time, tz=self._timezone)
        return current_datetime.replace(second=0, microsecond=0)

    def _is_cron_due(self, current_minute: datetime) -> bool:
        cron_weekday = (current_minute.weekday() + 1) % 7
        day_matches = current_minute.day in self._cron["days"]
        weekday_matches = cron_weekday in self._cron["weekdays"]

        if self._cron["day_is_wildcard"] or self._cron["weekday_is_wildcard"]:
            date_matches = day_matches and weekday_matches
        else:
            date_matches = day_matches or weekday_matches

        return (
            current_minute.minute in self._cron["minutes"]
            and current_minute.hour in self._cron["hours"]
            and current_minute.month in self._cron["months"]
            and date_matches
        )

    async def update_processed_data(self):
        mid_price = self.market_data_provider.get_price_by_type(
            self.config.connector_name,
            self.config.trading_pair,
            PriceType.MidPrice,
        )
        n_active_executors = len([executor for executor in self.executors_info if executor.is_active])
        current_minute = self._current_cron_minute()
        self.processed_data = {
            "mid_price": mid_price,
            "n_active_executors": n_active_executors,
            "current_cron_minute": current_minute,
            "is_cron_due": self._is_cron_due(current_minute),
        }

    def determine_executor_actions(self) -> list[ExecutorAction]:
        current_minute = self.processed_data["current_cron_minute"]
        if (
            self.processed_data["is_cron_due"]
            and self.processed_data["n_active_executors"] == 0
            and current_minute != self._last_triggered_minute
        ):
            self._last_triggered_minute = current_minute
            config = OrderExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=TradeType.BUY,
                amount=self.config.amount_quote / self.processed_data["mid_price"],
                execution_strategy=ExecutionStrategy.MARKET,
                price=self.processed_data["mid_price"],
            )
            return [CreateExecutorAction(controller_id=self.config.id, executor_config=config)]
        return []

    def to_format_status(self) -> list[str]:
        lines = [
            "Recurring Buy Cron Status:",
            f"  Schedule: {self.config.cron_schedule} ({self.config.timezone})",
            f"  Last triggered minute: {self._last_triggered_minute or 'N/A'}",
        ]
        if self.processed_data:
            lines.extend([
                f"  Current cron minute: {self.processed_data.get('current_cron_minute', 'N/A')}",
                f"  Cron due now: {self.processed_data.get('is_cron_due', 'N/A')}",
                f"  Mid price: {self.processed_data.get('mid_price', 'N/A')}",
                f"  Active executors: {self.processed_data.get('n_active_executors', 'N/A')}",
            ])
        return lines
