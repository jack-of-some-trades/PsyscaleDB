"""Suppoting Dataclass Objects and Constants for interfacing with a Timescale Database"""

from __future__ import annotations
import logging
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Protocol,
    Self,
    Tuple,
)

from dataclasses import dataclass, field
from pandas import Timedelta, Timestamp

DEFAULT_ORIGIN_DATE = Timestamp("2000/01/03 00:00", tz="UTC")
DEFAULT_HTF_ORIGIN_DATE = Timestamp("2000/01/01 00:00", tz="UTC")
DEFAULT_AGGREGATES = [
    Timedelta("5min"),
    Timedelta("15min"),
    Timedelta("30min"),
    Timedelta("1h"),
    Timedelta("4h"),
    Timedelta("1D"),
    Timedelta("1W"),
]
log = logging.getLogger("pycharts-timescaledb")


def _get_ensured[T](_dict: dict[str, T | None], key: str, default: T) -> T:
    "Enhanced get function to return the default even when the value stored in the dict is None"
    val = _dict.get(key)
    return val if val is not None else default


class Storable(Protocol):
    "Protocol definition for a Class instance that can be dumped into and loaded from JSON"

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> Self:
        "Loads and returns a class instance from a JSON representation of the object"
        raise NotImplementedError

    def to_json(self) -> dict:
        "Returns JSON representation of the object as a dictionary"
        raise NotImplementedError


# region -------- -------- -------- Asset Table Dataclass + Functions -------- -------- --------
# pylint: disable='missing-function-docstring'


@dataclass
class AssetTable:
    """
    Dataclass to Facilitate interacting with timeseries data tables

    -- PARAMS --
    - asset_name - The Name of the asset class this belongs to.
    - period - Timedelta of the Aggregation window
    - raw - Whether the data was inserted or calculated.
        * True : Table is raw data inserted from an API
        * False : Table is a Continuous Aggregate from a raw table
    - ext - Whether the asset class contains any ext information
        * True : Contains EXT information at any level of aggregation
        * False : Asset Class only has RTH Data (e.g. Crypto)
    - rth - Whether the table stores RTH data, ETH Data, or Both
        * None: Has 'rth' Boolean Column denoting a mix of RTH & ETH Data
        * True: No 'rth' Column, All data is RTH
        * False: No 'rth' Column, All data is ETH
    - _origin - Bucketing Start time for timeframes < 4 Weeks
    - _origin_htf - Time Bucket Start Time for timeframes >= 4 Weeks
    """

    asset_class: str
    period: Timedelta
    raw: bool
    ext: bool
    rth: Optional[bool]
    _origin_ltf: Optional[Timestamp] = None
    _origin_htf: Optional[Timestamp] = None
    _table_name: str = field(init=False)

    def __post_init__(self):
        """
        Format storage parameters into a series table name
        PARAMS:
        """
        self._table_name = (
            self.asset_class + "_" + str(int(self.period.total_seconds()))
        )

        if self._origin_ltf is not None and self._origin_htf is None:
            # Normalize to midnight of the first of whatever month was given.
            self._origin_htf = self._origin_ltf.replace(day=1).normalize()

        if self.raw:
            self._table_name += "_raw"  # Raw Data Inserted from an API

        if self.ext is False:
            return  # Neither RTH and ETH Data
        if self.rth is None:
            self._table_name += "_ext"  # Both RTH and ETH Data
        elif self.rth:
            self._table_name += "_rth"
        else:
            self._table_name += "_eth"

    def __eq__(self, other: Self) -> bool:
        if not isinstance(other, AssetTable):
            raise NotImplementedError(
                "Can only Compare between an AssetTable to an AssetTable"
            )
        # Compares asset_class, period, raw, ext, and rth all in one check
        return self._table_name == other._table_name

    def __hash__(self) -> int:
        "Converts the table name to a concatenated list of ascii integers."
        return int("".join(map(lambda x: str(ord(x)), self._table_name)))

    def __repr__(self) -> str:
        return self._table_name

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def origin_ts(self) -> Timestamp:
        if self.period < Timedelta("4W"):
            return DEFAULT_ORIGIN_DATE if self._origin_ltf is None else self._origin_ltf
        else:
            return (
                self._origin_htf
                if self._origin_htf is not None
                else DEFAULT_HTF_ORIGIN_DATE
            )

    @property
    def origin(self) -> str:
        return self.origin_ltf if self.period < Timedelta("4W") else self.origin_htf

    @property
    def origin_ltf(self) -> str:
        return str(
            DEFAULT_ORIGIN_DATE if self._origin_ltf is None else self._origin_ltf
        )

    @property
    def origin_htf(self) -> str:
        return str(
            self._origin_htf
            if self._origin_htf is not None
            else DEFAULT_HTF_ORIGIN_DATE
        )

    @property
    def psql_interval(self) -> str:
        return str(int(self.period.total_seconds())) + " seconds"

    @classmethod
    def from_table_name(cls, table_name: str) -> Self:
        """
        Return an AssetTable with the storage paramaters from a given table name.
        Near Inverse Operation to forming the table name on construction. (Origin is lost)
        """
        if table_name.endswith("_rth"):
            ext, rth = True, True
            table_name = table_name.removesuffix("_rth")
        elif table_name.endswith("_eth"):
            ext, rth = True, False
            table_name = table_name.removesuffix("_eth")
        elif table_name.endswith("_ext"):
            ext, rth = True, None
            table_name = table_name.removesuffix("_ext")
        else:
            ext, rth = False, None

        raw = table_name.endswith("_raw")
        if raw:
            table_name = table_name.removesuffix("_raw")

        parts = table_name.split("_")

        # Pop off the period in seconds
        period = Timedelta(seconds=int(parts.pop()))
        # Rejoin on the delimiter in case the asset_class str included the delimiter.
        asset_class = "_".join(parts)

        return cls(asset_class, period, raw, ext, rth)


# pylint: disable=line-too-long
@dataclass
class TimeseriesConfig:
    """
    Defines the storeage scheme of timeseries data within the postgres database.
    3 Of these objects are used by the TimescaleDB interface, One for each of the following Schema:
    Tick Data, Minute Data, Pre-aggregated Data.

    - Tick Data: Generates aggregates from raw Ticks information.
    - Minute Data: Generates Aggregates from Pre-aggrigated Minute Data.
        * From Sources such as Alpaca & Polygon.io
    - Pre-Aggregated: Only Aggregates are stored, Little/No information is derived.
        * From Sources such as Trading-View & yFinance where data at HTF extends further back in time than LTF Data

    -- PARAMS --
    - asset_types: Iterable[str] - Asset types that can be stored in the Schema. These strings will be used
        as dictionary keys for other input parameters.

        How these types are delineated is important and should be done based on market opening time.
        This is because each Asset Type shares a table. When this table is Aggregated into a Higher-Timeframe
        it will be done with a constant Bucketing Origin Timestamp.
        e.g. US_Stock Data has an origin timestamp of 2000/01/03 8:30 EST for RTH and 2000/01/03 04:00 EST for ETH
        While a Forex Aggregate needs an origin timestamp of 2000/01/03 00:00 UTC. If These assets were stored
        in the same table, some of the Higher Timeframe Aggregates would be incorrect.

    - rth_origins: Dict[Asset_type:str, pd.Timestamp]
        : These Timestamps are Origins for Aggregating Regular-Trading-Hours at periods of less than 4 Weeks
        This should be and should be the market opening time on the first trading day of a week.
        * Optional, If None is given then 2000/01/03 00:00:00 UTC is Assumed.
        * 'default' can be passed as a key to override stated default parameter.

    - eth_origins: Dict[Asset_type:str, pd.Timestamp]
        : These Timestamps are Origins for Extended-Trading-Hours.
        * Optional, If None is given then the RTH_Origin is assumed thus ignoring any RTH/ETH Distinction
        * 'default' can be passed as a key to override stated default parameter.

    - htf_origins: Dict[Asset_type:str, pd.Timestamp]
        : These Timestamps are Origins for Aggregation Periods of 4 Weeks or Larger. Should generally align with
        at least midnight of the first of the month, ideally also the first of a year, decade, and millennium.
        * Optional, If None is given then the 2000/01/01 00:00:00 UTC is Assumed.
        * 'default' can be passed as a key to override stated default parameter.

    - aggregate_periods: Dict[Asset_type:str, List[pd.Timedelta]]
        : The Aggregate Periods that should be calculated and stored.
        * The Minimum value is 1 second for Tick Data, and 2 Minutes for Minute Data
        * The Default Aggregates are [5min, 15min, 30min, 1h, 4h, 1D, 1W]
        * 'default' can be passed as a key to override stated default parameter.

    - aggregate_periods: Dict[Asset_type:str, List[pd.Timedelta]]
        : The Aggregate Periods that should will be inserted from an API and Joined on retrieval.
        * There are no Default Inserted Aggregate
        * 'default' can be passed as a key to override stated default parameter.

    - prioritize_rth: Dict[Asset_type:str, Bool | None]
        : Determines the Behavior on Conflict, per asset, when storing RTH and ETH Aggregates.
        If given a differing RTH & ETH Origin Timestamp for an asset, there is a chance for a given aggregation
        period to result in differing period start times. This bool determines what should be stored.
        e.g. NYSE 1h ETH Periods start aggregation at 8AM/9AM while RTH starts aggregating at 8:30AM.

        * True - Only store the RTH Aggregate at HTFs (Calculate ETH at Runtime) (Default Behavior)
        * False - Only store the ETH Aggregate at HTFs (Calculate RTH at Runtime)
        * None - Store Both the RTH and ETH Aggregate at HTFs
        * 'default' can be passed as a key to override stated default parameter.
    """

    asset_classes: Iterable[str]
    rth_origins: dict[str, Timestamp | None] = field(default_factory=dict)
    eth_origins: dict[str, Timestamp | None] = field(default_factory=dict)
    htf_origins: dict[str, Timestamp | None] = field(default_factory=dict)
    prioritize_rth: dict[str, bool | None] = field(default_factory=dict)
    aggregate_periods: dict[str, list[Timedelta] | None] = field(default_factory=dict)
    inserted_aggregate_periods: dict[str, list[Timedelta] | None] = field(
        default_factory=dict
    )

    def __post_init__(self):
        # Get all the desired Defaults from the given input
        self._std_tables: Dict[str, List[AssetTable]] = {}
        self._rth_tables: Dict[str, List[AssetTable]] = {}
        self._eth_tables: Dict[str, List[AssetTable]] = {}
        self._inserted_tables: Dict[str, List[AssetTable]] = {}

        _default_rth = _get_ensured(self.rth_origins, "default", DEFAULT_ORIGIN_DATE)
        _default_eth = _get_ensured(self.eth_origins, "default", _default_rth)
        _default_htf = _get_ensured(
            self.htf_origins, "default", DEFAULT_HTF_ORIGIN_DATE
        )
        _default_aggs = _get_ensured(
            self.aggregate_periods, "default", DEFAULT_AGGREGATES
        )

        _default_raw_aggs = _get_ensured(self.inserted_aggregate_periods, "default", [])
        _default_priority = self.prioritize_rth.get("default", True)

        for asset_class in self.asset_classes:
            asset_aggregates = _get_ensured(
                self.aggregate_periods, asset_class, _default_aggs
            )
            inserted_aggregates = _get_ensured(
                self.inserted_aggregate_periods, asset_class, _default_raw_aggs
            )

            # Error check to Ensure no overlapping aggregate and inserted timeframes
            # No fuckin way I'm managing that mess of trying to detect what tables need to be JOIN'd
            overlap = {*asset_aggregates}.intersection(inserted_aggregates)
            if len(overlap) > 0:
                raise AttributeError(
                    f"Asset Class {asset_class} is set to store *and* aggregate the timeframes : \n{overlap}\n\n"
                    "Timeframes can only be Stored or Aggregated, not Both."
                )

            asset_rth_origin = _get_ensured(self.rth_origins, asset_class, _default_rth)
            asset_eth_origin = _get_ensured(self.eth_origins, asset_class, _default_eth)
            asset_htf_origin = _get_ensured(self.htf_origins, asset_class, _default_htf)

            # Store all the asset origins so they can be referenced later (to insert into the DB)
            # Ensures the each key is valid and will yield a Timestamp
            self.rth_origins[asset_class] = asset_rth_origin
            self.eth_origins[asset_class] = asset_eth_origin
            self.htf_origins[asset_class] = asset_htf_origin

            asset_args = {
                "asset_class": asset_class,
                "_origin_htf": asset_htf_origin,
            }

            # region ---- ---- Create All Associated Asset Tables ---- ----
            if asset_rth_origin == asset_eth_origin:
                # No EXT Information for this asset table. Simplify Table creation.
                asset_args |= {
                    "_origin_ltf": asset_rth_origin,
                    "ext": False,
                    "rth": None,
                }
                self._std_tables[asset_class] = [
                    AssetTable(period=period, raw=False, **asset_args)
                    for period in asset_aggregates
                ]
                self._inserted_tables[asset_class] = [
                    AssetTable(period=period, raw=True, **asset_args)
                    for period in inserted_aggregates
                ]
                continue

            # Get the ETH/RTH conflicting aggregates and create the appropriate tables
            asset_args |= {"ext": True, "raw": False, "_origin_ltf": asset_eth_origin}
            std, rth_only, eth_only = _determine_conflicting_timedeltas(
                asset_aggregates,
                asset_rth_origin,
                asset_eth_origin,
                self.prioritize_rth.get(asset_class, _default_priority),
            )

            # std_tables (that have an ext column) use eth_origin since it's earlier than rth_origin
            self._std_tables[asset_class] = [
                AssetTable(period=period, rth=None, **asset_args) for period in std
            ]
            self._eth_tables[asset_class] = [
                AssetTable(period=period, rth=False, **asset_args)
                for period in eth_only
            ]

            # Union Operator Overrides existing keys
            asset_args |= {"_origin_ltf": asset_rth_origin}

            self._rth_tables[asset_class] = [
                AssetTable(period=period, rth=True, **asset_args) for period in rth_only
            ]

            asset_args |= {"raw": True}

            std, rth_only, eth_only = _determine_conflicting_timedeltas(
                inserted_aggregates,  # re-determine conflicts for the inserted aggs.
                asset_rth_origin,
                asset_eth_origin,
                self.prioritize_rth.get(asset_class, _default_priority),
            )

            self._inserted_tables[asset_class] = [
                AssetTable(period=period, rth=True, **asset_args)
                for period in inserted_aggregates
                if period in rth_only
            ]

            asset_args |= {"_origin_ltf": asset_eth_origin}

            self._inserted_tables[asset_class].extend(
                AssetTable(period=period, rth=False, **asset_args)
                for period in inserted_aggregates
                if period in eth_only
            )
            self._inserted_tables[asset_class].extend(
                AssetTable(period=period, rth=None, **asset_args)
                for period in inserted_aggregates
                if period in std
            )
            # endregion

    def all_tables(
        self, asset_class: str, *, include_raw: bool = True
    ) -> List[AssetTable]:
        if asset_class not in self.asset_classes:
            raise KeyError(f"{asset_class = } is not a known asset type.")

        base_list = self._inserted_tables.get(asset_class, []) if include_raw else []
        return (
            base_list
            + self._std_tables.get(asset_class, [])
            + self._rth_tables.get(asset_class, [])
            + self._eth_tables.get(asset_class, [])
        )

    def std_tables(self, asset_class: str, inserted: bool = False) -> List[AssetTable]:
        if asset_class not in self.asset_classes:
            raise KeyError(f"{asset_class = } is not a known asset type.")
        if not inserted:
            return self._std_tables[asset_class]
        else:
            return self._std_tables[asset_class] + self.raw_tables(asset_class, "std")

    def rth_tables(self, asset_class: str, inserted: bool = False) -> List[AssetTable]:
        if asset_class not in self.asset_classes:
            raise KeyError(f"{asset_class = } is not a known asset type.")
        if not inserted:
            return self._rth_tables[asset_class]
        else:
            return self._rth_tables[asset_class] + self.raw_tables(asset_class, "rth")

    def eth_tables(self, asset_class: str, inserted: bool = False) -> List[AssetTable]:
        if asset_class not in self.asset_classes:
            raise KeyError(f"{asset_class = } is not a known asset type.")
        if not inserted:
            return self._eth_tables[asset_class]
        else:
            return self._eth_tables[asset_class] + self.raw_tables(asset_class, "eth")

    def raw_tables(
        self, asset_class: str, rth: Literal["all", "std", "rth", "eth"] = "all"
    ) -> List[AssetTable]:
        if asset_class not in self.asset_classes:
            raise KeyError(f"{asset_class = } is not a known asset type.")
        if rth == "all":
            return self._inserted_tables[asset_class]
        elif rth == "std":
            return [
                tbl for tbl in self._inserted_tables[asset_class] if tbl.rth is None
            ]
        elif rth == "rth":
            return [
                tbl for tbl in self._inserted_tables[asset_class] if tbl.rth is True
            ]
        else:
            return [
                tbl for tbl in self._inserted_tables[asset_class] if tbl.rth is False
            ]

    def get_aggregation_source(self, derived_table: AssetTable) -> AssetTable:
        "Return the Most appropriate Asset Table to Aggregate into the given table."
        all_lower_tfs = [
            tbl
            for tbl in self.all_tables(derived_table.asset_class)
            if tbl.period < derived_table.period
        ]
        if len(all_lower_tfs) == 0:
            raise AttributeError(
                f"Cannot create Aggregate Table: {derived_table} \n"
                "The Current config lacks lower timeframe data."
            )

        # Match Ext True/False
        all_valid_ext = [tbl for tbl in all_lower_tfs if tbl.ext == derived_table.ext]
        # Match RTH True/False/None
        all_valid_ext = [
            tbl
            for tbl in all_valid_ext
            if tbl.rth is None or tbl.rth == derived_table.rth
        ]
        if len(all_valid_ext) == 0:
            raise AttributeError(
                f"Cannot create Aggregate Table: {derived_table} \n"
                "The config and Derived tables have mismatching ETH Data."
            )

        all_valid_multiples = [
            tbl
            for tbl in all_valid_ext
            if (derived_table.period % tbl.period) == Timedelta(0)
        ]
        if len(all_valid_multiples) == 0:
            raise AttributeError(
                f"Cannot create Aggregate Table: {derived_table} \n"
                "There are no Whole Period Devisors of the Aggregate Table"
            )

        # Strip out all but the lowest *inserted* Timeframe. All HTF inserted data is assumed
        # to not be maintained and therefore should only be used with a join
        all_raws = [tbl for tbl in all_valid_multiples if tbl.raw]
        all_raws.sort(key=lambda x: x.period)

        # Filter down only to only the raw tables (that have no origin) and tables
        # with identical origins. Painful, but timescale does not allow an aggregate
        # to form from an aggregate that has a different time_bucket origin.
        all_non_raws = [
            tbl
            for tbl in all_valid_multiples
            if not tbl.raw and tbl.origin_ts == derived_table.origin_ts
        ]

        all_valid = all_non_raws + all_raws[0:1]
        all_valid.sort(key=lambda x: x.period)

        if len(all_valid) == 0:
            raise AttributeError(
                f"Cannot create Aggregate Table: {derived_table} \n"
                "There are no Whole Period Devisors With an Identical Time_bucket Origin, "
                "and the Smallest TimeFrame of Inserted Data is not a Whole Devisor"
            )

        # Get the longest period that satisfies the above conditions
        return all_valid[-1]

    def get_tables_to_refresh(self, altered_table: AssetTable) -> List[AssetTable]:
        "Return all the tables that need to be refreshed for a given table that has been altered"
        all_aggs = self.all_tables(altered_table.asset_class, include_raw=False)
        # Really this is pretty inefficient and will cause more updates than needed, but I mean,
        # Does it really matter given data will probably be inserted once a day at most? No.
        filtered_aggs = [agg for agg in all_aggs if agg.period > altered_table.period]
        filtered_aggs.sort(key=lambda x: x.period)  # Ensure aggregates are ordered.
        return filtered_aggs

    @classmethod
    def from_table_names(
        cls,
        names: list[str],
        origins: dict[str, Tuple[Timestamp, Timestamp, Timestamp]] = {},
    ):
        """
        Reconstruct the Timeseries Config from the given asset names.
        Origin Timestamps must be given in a dictionary of
        [asset_class:str, (rth_origin, eth_origin, htf_origin)]
        """
        filtered_names = [name for name in names if name.startswith("_")]
        if len(filtered_names) > 0:
            log.debug(
                "Filtered out table names %s when reconstructing TimeseriesConfig from Database table names.",
                filtered_names,
            )

        tables = []
        for name in names:
            # Filter out All Table names that begin with an '_'. They are metadata.
            if name.startswith("_"):
                continue
            try:
                tables.append(AssetTable.from_table_name(name))
            except ValueError:
                log.warning(
                    "Timeseries Database contains an invalid table name: %s", name
                )

        asset_classes = {table.asset_class for table in tables}

        cls_inst = cls(asset_classes)

        # Reconstruct the std, raw, eth, and inserted table dictionaries.
        for asset_class in asset_classes:
            # Get all the tables for this asset class
            cls_tables = [table for table in tables if table.asset_class == asset_class]

            if asset_class in origins:
                cls_inst.rth_origins[asset_class] = origins[asset_class][0]
                cls_inst.eth_origins[asset_class] = origins[asset_class][1]
                cls_inst.htf_origins[asset_class] = origins[asset_class][2]

            # Store the raw inserted tables
            cls_inst._inserted_tables[asset_class] = [
                table for table in cls_tables if table.raw
            ]
            cls_inst.inserted_aggregate_periods[asset_class] = [
                table.period for table in cls_inst._inserted_tables[asset_class]
            ]

            # Remove the Raw Tables so that doesn't need to be checked on following generators
            cls_tables = [table for table in cls_tables if not table.raw]

            cls_inst._std_tables[asset_class] = [
                table for table in cls_tables if table.rth is None
            ]
            cls_inst._rth_tables[asset_class] = [
                table for table in cls_tables if table.rth is True
            ]
            cls_inst._eth_tables[asset_class] = [
                table for table in cls_tables if table.rth is False
            ]
            cls_inst.aggregate_periods[asset_class] = [
                table.period for table in cls_tables
            ]

        return cls_inst


def _determine_conflicting_timedeltas(
    periods: List[Timedelta],
    rth_origin: Timestamp,
    eth_origin: Timestamp,
    prioritize_rth: Optional[bool],
) -> Tuple[List[Timedelta], List[Timedelta], List[Timedelta]]:
    """
    Determines if an aggregate period would conflict when aggregating on RTH and ETH Times.
    Returns the sorted time periods that need to be stored as a tuple of lists:
    ([Combined ETH/RTH],[RTH only times],[ETH only times])
    """
    # rth_origin and eth_origin are guerenteed to be different at this stage.

    eth_delta = rth_origin - eth_origin
    # These checks are currently in place since it is unclear if these edge-cases would cause problems
    # Safer to assume they will and remove this limitation later on if it is needed and doesn't cause any issues.
    if eth_delta < Timedelta(0):
        raise ValueError(
            "TimescaleDB ETH Aggregate Origin must occur before RTH Origin."
        )
    if eth_delta >= Timedelta("1D"):
        raise ValueError(
            "TimescaleDB RTH Origin Must be less than 1D after ETH Origin."
        )

    periods.sort()
    std_periods, rth_periods, eth_periods = [], [], []

    for period in periods:
        remainder = eth_delta % period
        if remainder == Timedelta(0):
            std_periods.append(period)
        elif prioritize_rth:
            rth_periods.append(period)
        elif prioritize_rth is None:
            rth_periods.append(period)
            eth_periods.append(period)
        else:
            eth_periods.append(period)

    return std_periods, rth_periods, eth_periods


# endregion
