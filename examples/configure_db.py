"Timeseries Configuration Script"

import dotenv
from pandas import Timedelta, Timestamp

from psyscale import PsyscaleDB, set_psyscale_log_level
from psyscale.dev import TimeseriesConfig, DEFAULT_AGGREGATES

set_psyscale_log_level("INFO")


def main():
    dotenv.load_dotenv(dotenv.find_dotenv())
    db = PsyscaleDB()

    db.configure_timeseries_schema(
        minute_tables=STD_MINUTE_CONFIG,
        aggregate_tables=STD_AGGREGATE_CONFIG,
    )


# region ---- ---- ---- Example Standard Database Configurations ---- ---- ----

# Make this an empty list to have the option to clear the entire database
# STD_ASSET_LIST = []
STD_ASSET_LIST = ["us_fund", "us_stock", "crypto"]

# Minute Schema Imports Minute Data and Aggreagtes higher Timeframe information
STD_MINUTE_CONFIG = TimeseriesConfig(
    STD_ASSET_LIST,  # type:ignore
    rth_origins={
        "us_stock": Timestamp("2000/01/03 08:30", tz="America/New_York"),
        "us_fund": Timestamp("2000/01/03 08:30", tz="America/New_York"),
    },
    eth_origins={
        "us_stock": Timestamp("2000/01/03 04:00", tz="America/New_York"),
        "us_fund": Timestamp("2000/01/03 04:00", tz="America/New_York"),
    },
    prioritize_rth={"us_stock": True, "us_fund": True},
    aggregate_periods={"default": DEFAULT_AGGREGATES},
    inserted_aggregate_periods={"default": [Timedelta("1m")]},
)


STD_TICK_PERIODS = [
    Timedelta("5sec"),
    Timedelta("15sec"),
    Timedelta("30sec"),
    Timedelta("1min"),
]
STD_TICK_PERIODS.extend(DEFAULT_AGGREGATES)

# Tick Schema imports Tick Data and Aggregates HTF Data
STD_TICK_CONFIG = TimeseriesConfig(
    STD_ASSET_LIST,  # type:ignore
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
STD_AGGREGATE_CONFIG = TimeseriesConfig(
    STD_ASSET_LIST,  # type:ignore
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
    inserted_aggregate_periods={"default": [Timedelta("1m")] + DEFAULT_AGGREGATES},
)

# endregion

if __name__ == "__main__":
    main()
