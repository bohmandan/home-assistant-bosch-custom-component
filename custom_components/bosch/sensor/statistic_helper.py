"""Bosch statistic helper for Recording/Energy sensor."""

from __future__ import annotations
import logging
import asyncio
from datetime import datetime, timedelta
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData,
    datetime_to_timestamp_or_none,
)
from sqlalchemy.exc import IntegrityError
from homeassistant.util import dt as dt_util

try:
    from homeassistant.components.recorder.statistics import StatisticMeanType
except ImportError:
    StatisticMeanType = None

try:
    from homeassistant.components.recorder.db_schema import StatisticsMeta
except ImportError:
    from homeassistant.components.recorder.models import StatisticsMeta
from homeassistant.components.recorder.util import session_scope
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
    StatisticsRow,
)
from homeassistant.components.recorder import get_instance
from .base import BoschBaseSensor

_LOGGER = logging.getLogger(__name__)

# How far back to look for the base row when the caller's query window holds
# none: one month first (cheap), then everything there could be.
BASE_ROW_LOOKBACKS = (timedelta(days=30), timedelta(days=3660))


class StatisticHelper(BoschBaseSensor):
    """Statistic helper class."""

    def __init__(self, new_stats_api: bool = False, **kwargs):
        """Initialize statistic helper."""
        self._short_id = None
        self._new_stats_api = new_stats_api
        self._statistic_import_lock = asyncio.Lock()
        super().__init__(**kwargs)

    async def move_old_entity_data_to_new(self, event_time=None) -> None:
        """Rename old entity_id in statistic table. Not working currently."""
        old_entity_id = self.entity_id
        _LOGGER.debug("Moving entity id statistic data to new format.")
        try:
            with session_scope(hass=self.hass) as session:
                session.query(StatisticsMeta).filter(
                    (StatisticsMeta.statistic_id == old_entity_id)
                    & (StatisticsMeta.source == "recorder")
                ).update(
                    {
                        StatisticsMeta.statistic_id: self.statistic_id,
                        StatisticsMeta.source: self._domain_name.lower(),
                        StatisticsMeta.name: f"Stats {self._attr_name}",
                    }
                )
        except IntegrityError as err:
            _LOGGER.error("Can't move entity id. It already exists. %s", err)

    @property
    def statistic_id(self) -> str:
        """External API statistic ID."""
        raise NotImplementedError()

    @property
    def should_poll(self):
        """Don't poll."""
        return False

    _UNIT_CLASS_MAP = {
        "kWh": "energy",
        "Wh": "energy",
        "kW": "power",
    }

    def _get_unit_class(self):
        """Derive unit_class from unit_of_measurement."""
        return self._UNIT_CLASS_MAP.get(self._unit_of_measurement)

    @property
    def statistic_metadata(self) -> StatisticMetaData:
        """Statistic Metadata recorder model class."""
        return StatisticMetaData(
            has_mean=False,
            **({'mean_type': StatisticMeanType.NONE} if StatisticMeanType is not None else {}),
            has_sum=True,
            name=f"Stats {self._attr_name}",
            source=self._domain_name.lower(),
            statistic_id=self.statistic_id,
            unit_of_measurement=self._unit_of_measurement,
            unit_class=self._get_unit_class(),
        )

    async def get_last_stat(self) -> dict[str, list[StatisticsRow]]:
        return await get_instance(self.hass).async_add_executor_job(
            get_last_statistics,
            self.hass,
            1,
            self.statistic_id,
            True,
            {"state", "sum"},
        )

    async def get_stats_from_ha_db(
        self, start_time: datetime, end_time: datetime
    ) -> dict[str, list[StatisticsRow]]:
        """Get stats during period."""
        return await get_instance(self.hass).async_add_executor_job(
            statistics_during_period,
            self.hass,
            start_time,
            end_time,
            [self.statistic_id],
            "hour",
            None,
            {"state", "sum"},
        )

    def add_external_stats(self, stats: list[StatisticData]) -> None:
        """Add external statistics."""
        self._state = -17
        if not stats:
            return
        async_add_external_statistics(self.hass, self.statistic_metadata, stats)
        self.async_schedule_update_ha_state()

    def get_last_stats_before_date(
        self, last_stats: dict[str, list[StatisticsRow]], day: datetime
    ) -> StatisticsRow | None:
        """Return the newest statistic row strictly before day, or None."""
        day_stamp = datetime_to_timestamp_or_none(day)
        closest_stat = None
        for stat in last_stats.get(self.statistic_id, []):
            tstmp = stat.get("start")
            if tstmp is not None and day_stamp is not None and tstmp < day_stamp:
                if closest_stat is None or tstmp > closest_stat.get("start"):
                    closest_stat = stat
        return closest_stat

    async def async_get_last_stats_before_date(
        self, last_stats: dict[str, list[StatisticsRow]], day: datetime
    ) -> StatisticsRow:
        """Return the base row for a re-import: the newest row before day.

        Ported from upstream PR #604 (fixes #599). The caller's query window
        can miss the base row, because hours without consumption are not
        stored. The old fallback took the newest row of the window instead,
        a row of the day being rebuilt, so every hourly run added that day's
        values again. Search the recorder further back; with no row at all,
        start from 0 like the initial import (start None: no gateway fetch).
        """
        closest_stat = self.get_last_stats_before_date(last_stats, day)
        if closest_stat is None:
            for lookback in BASE_ROW_LOOKBACKS:
                older_stats = await self.get_stats_from_ha_db(
                    start_time=day - lookback, end_time=day
                )
                closest_stat = self.get_last_stats_before_date(older_stats, day)
                if closest_stat is not None:
                    break
        if closest_stat is None:
            _LOGGER.debug(
                "No statistic row of %s before %s, starting from sum 0.",
                self.statistic_id,
                day,
            )
            return {"start": None, "state": 0.0, "sum": 0.0}
        _LOGGER.debug(
            "Last stat for %s found %s", self.statistic_id, closest_stat
        )
        return closest_stat

    async def insert_statistics_range(self, start_time: datetime) -> None:
        """Attempt to put past data into database."""
        start = dt_util.start_of_local_day(start_time)
        stop = start + timedelta(hours=24)  # fetch one day only from API
        async with self._statistic_import_lock:
            await self._upsert_past_statistics(start=start, stop=stop)

    async def fetch_past_data(
        self, start_time: datetime, stop_time: datetime
    ) -> dict:
        """Rename old entity_id in statistic table."""
        start_time = dt_util.start_of_local_day(start_time)
        _LOGGER.debug(
            "Attempt to fetch range %s - %s for %s",
            start_time,
            stop_time,
            self.statistic_id,
        )
        my_range = await self._bosch_object.fetch_range(
            start_time=start_time, stop_time=stop_time
        )
        return my_range

    async def _upsert_past_statistics(
        self, start: datetime, stop: datetime
    ) -> None:
        raise NotImplementedError
