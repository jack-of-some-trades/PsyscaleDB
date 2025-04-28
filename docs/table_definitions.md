
# Security Schema
```mermaid
classDiagram
    class security.symbols {
        << Table >>
        +int pkey :: Serial Primary Key
        +text symbol
        +text source
        +text exchange
        +text asset_class
        +text name
        +boolean store_tick = false
        +boolean store_minute = false
        +boolean store_aggregate = false
        +boolean store [generated]
        +jsonb attrs
        --
        +UNIQUE(symbol, source, exchange)
        +CHECK(store_tick + store_minute + store_aggregate <= 1)
        +INDEX attrs_gin_idx
        +INDEX name_trgm_idx
        +INDEX symbol_trgm_idx
    }

    class security._metadata {
        << Materialized View >>
        +int pkey :: Foreign Key
        +text table_name
        +text schema_name
        +text timeframe
        +boolean is_raw_data
        +text trading_hours_type
        +timestamptz start_date
        +timestamptz end_date
    }

    security.symbols --> security._metadata : pkey
```
# Schema: Data_Schemas

### Data Table Naming Scheme : 
### <asset_class>_<_timeframe>[_raw][_ext/_rth /_eth]
- Asset_class : set of assets grouped by origin timestamp
- _timeframe : # of seconds aggregated into a time_bucket. 
    - Tick Tables have timeframe == 0
- [_raw] : Optional 'raw' flag to denote if data was inserted
    - Has flag : Table contains data that was instered from a data broker. Must have manual data insertions in the future
    - No flag : Table is a continuous aggregate that must be refreshed after data insertions
- [_ext/_rth /_eth] : Optional Extended Trade Hours Flag. 
    - No Flag: Asset is in a market that has no extended trade hours
    - _ext : Asset has Extended Trade Hours. This table contains an 'rth' column. 
        - Bars cleanly aggregate both 'rth' and 'eth' times
    - _rth : Asset has Extended Trade Hours. This table has no 'RTH' column, it is only 'rth' data aggregated to the 'rth_origin' timestamp.
    - _eth : Asset has Extended Trade Hours. This table contains an 'rth' column and data is only aggregated to the 'eth_origin' timestamp.
        - '_eth' Tables don't cleanly aggregate to both 'rth' and 'eth' Timeframes. e.g. 1hr bars on NYSE. 
        - The Aggregates cross over the 'rth'/'eth' boundry containting data from both ranges. 'rth' column takes the label the aggregate begins in.

### Extended Trade Hours Session Map:
- EXT_MAP = 
    - "pre": 1,
    - "rth_pre_break": 0,
    - "rth": 0,
    - "break": 3,
    - "rth_post_break": 0,
    - "post": 2,
    - "closed": -1
    
```mermaid
classDiagram
    class security.symbols {
        +int pkey :: Serial Primary Key
    }

    class _ORIGINS_TABLE {
        +asset_class TEXT :: PRIMARY KEY
        +origin_rth TIMESTAMPTZ NOT NULL
        +origin_eth TIMESTAMPTZ NOT NULL
        +origin_htf TIMESTAMPTZ NOT NULL
    }
    
    class TICK_TABLE {
        +int pkey :: Serial Foreign Key
        +timestamptz dt
        +smallint rth :: Optional
        +double price
        +double volume
        +Primary_Key(pkey, dt)
    }

    class AGGREGATE_TABLE {
        +int pkey :: Serial Foreign Key
        +timestamptz dt
        +smallint rth :: Optional
        +double open
        +double close
        +double high
        +double low
        +double volume
        +double vwap
        +int ticks
        +Primary_Key(pkey, dt)
    }

    class CONTINUOUS_AGGREGATE {
        +int pkey :: Serial Foreign Key
        +timestamptz dt :: Derived
        TimeBucket(dt, origin) timestamptz dt
        +smallint rth :: Optional
        +double open
        +double close
        +double high
        +double low
        +double volume
        +double vwap
        +int ticks
        +Primary_Key(pkey, dt)
    }

    security.symbols --> CONTINUOUS_AGGREGATE : pkey
    security.symbols --> AGGREGATE_TABLE : pkey
    security.symbols --> TICK_TABLE : pkey
    _ORIGINS_TABLE --> CONTINUOUS_AGGREGATE : Origin_Timestamp
```