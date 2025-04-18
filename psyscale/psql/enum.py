from enum import Enum, StrEnum, auto


class Operation(Enum):
    "Postgres Operations"

    CREATE = auto()
    INSERT = auto()
    UPSERT = auto()
    UPDATE = auto()
    SELECT = auto()
    COPY = auto()
    DROP = auto()
    DELETE = auto()
    REFRESH = auto()


class Schema(StrEnum):
    "Schema Names available within the database"

    SECURITY = auto()
    USER_DATA = auto()
    TICK_DATA = auto()
    MINUTE_DATA = auto()
    AGGREGATE_DATA = auto()


class GenericTbls(StrEnum):
    "Generic Cmds that may apply to multiple table types"

    SCHEMA = auto()
    SCHEMA_TABLES = auto()
    TABLE = auto()
    VIEW = auto()


class SeriesTbls(StrEnum):
    "Raw & Aggregated Timeseries Data Tables"

    _ORIGIN = auto()
    TICK = auto()
    TICK_BUFFER = auto()
    RAW_AGGREGATE = auto()
    RAW_AGG_BUFFER = auto()
    CONTINUOUS_AGG = auto()
    CONTINUOUS_TICK_AGG = auto()
    # Following is Not a stored_table, just references a specific select function
    CALCULATE_AGGREGATE = auto()


class AssetTbls(StrEnum):
    "Security Information Tables"

    _METADATA = auto()
    _METADATA_FUNC = auto()
    SYMBOLS = auto()
    SYMBOLS_BUFFER = auto()
    _SYMBOL_SEARCH_FUNCS = auto()

    # Not Implemented Yet. All Data is assumed to be Adjusted.
    # SPLITS = auto()
    # DIVIDENDS = auto()
    # EARNINGS = auto()


class UserTbls(StrEnum):
    "TBD Tables for storing various user data"

    USER_TABLE = auto()
    WATCHLIST = auto()
    PRIMITIVES = auto()
    INDICATOR = auto()
