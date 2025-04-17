"Timeseries Configuration Script"

import dotenv
from pandas import Timedelta, Timestamp

import pycharts_timescaledb as tsdb
from pycharts_timescaledb.api_extention import TimescaleDB_EXT

tsdb.set_timescale_db_log_level("INFO")


def main():
    dotenv.load_dotenv(dotenv.find_dotenv())
    db = TimescaleDB_EXT()

    db.configure_timeseries_schema(
        minute_tables=STD_MINUTE_CONFIG,
        aggregate_tables=STD_AGGREGATE_CONFIG,
    )


# region ---- ---- ---- Example Standard Database Configurations ---- ---- ----

# Make this an empty list to have the option to clear the entire database
# STD_ASSET_LIST = []
STD_ASSET_LIST = ["us_fund", "us_stock", "crypto"]

# Minute Schema Imports Minute Data and Aggreagtes higher Timeframe information
STD_MINUTE_CONFIG = tsdb.TimeseriesConfig(
    STD_ASSET_LIST,
    rth_origins={
        "us_stock": Timestamp("2000/01/03 08:30", tz="America/New_York"),
        "us_fund": Timestamp("2000/01/03 08:30", tz="America/New_York"),
    },
    eth_origins={
        "us_stock": Timestamp("2000/01/03 04:00", tz="America/New_York"),
        "us_fund": Timestamp("2000/01/03 04:00", tz="America/New_York"),
    },
    prioritize_rth={"us_stock": True, "us_fund": True},
    aggregate_periods={"default": tsdb.DEFAULT_AGGREGATES},
    inserted_aggregate_periods={"default": [Timedelta("1m")]},
)


STD_TICK_PERIODS = [
    Timedelta("5sec"),
    Timedelta("15sec"),
    Timedelta("30sec"),
    Timedelta("1min"),
]
STD_TICK_PERIODS.extend(tsdb.DEFAULT_AGGREGATES)

# Tick Schema imports Tick Data and Aggregates HTF Data
STD_TICK_CONFIG = tsdb.TimeseriesConfig(
    STD_ASSET_LIST,
    rth_origins={
        "us_stock": Timestamp("2000/01/03 08:30", tz="America/New_York"),
        "us_fund": Timestamp("2000/01/03 08:30", tz="America/New_York"),
    },
    eth_origins={
        "us_stock": Timestamp("2000/01/03 04:00", tz="America/New_York"),
        "us_fund": Timestamp("2000/01/03 04:00", tz="America/New_York"),
    },
    prioritize_rth={"us_stock": True, "us_fund": True},
    aggregate_periods={"default": STD_TICK_PERIODS},
    inserted_aggregate_periods={"default": []},
)

# Aggregate Schema only imports aggregate data. Useful when Higher Timeframe
# Data extends further back in time than lower timeframe data.
STD_AGGREGATE_CONFIG = tsdb.TimeseriesConfig(
    STD_ASSET_LIST,
    rth_origins={
        "us_stock": Timestamp("2000/01/03 08:30", tz="America/New_York"),
        "us_fund": Timestamp("2000/01/03 08:30", tz="America/New_York"),
    },
    eth_origins={
        "us_stock": Timestamp("2000/01/03 04:00", tz="America/New_York"),
        "us_fund": Timestamp("2000/01/03 04:00", tz="America/New_York"),
    },
    prioritize_rth={"us_stock": True, "us_fund": True},
    aggregate_periods={"default": []},
    inserted_aggregate_periods={"default": [Timedelta("1m")] + tsdb.DEFAULT_AGGREGATES},
)

# endregion

if __name__ == "__main__":
    main()
