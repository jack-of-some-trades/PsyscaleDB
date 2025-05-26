"Symbols Table Partial Class Functions"

import logging
from typing import (
    Iterable,
    Literal,
    Any,
    Optional,
    Tuple,
)
from pandas import DataFrame, Series
from psycopg import sql
from psyscale.async_core import PsyscaleAsyncCore
from psyscale.core import PsyscaleConnectParams, PsyscaleCore

from .psql import (
    SYMBOL_ARGS,
    STRICT_SYMBOL_ARGS,
    SymbolArgs,
    Operation as Op,
    AssetTbls,
    Schema,
    GenericTbls,
)

# pylint: disable='protected-access'
log = logging.getLogger("psyscale_log")


class SymbolsPartial(PsyscaleCore):
    "Psyscale Symbols Table upsert and search functions"

    def __init__(
        self,
        conn_params: Optional[PsyscaleConnectParams | str] = None,
        *,
        down_on_del: bool = False,
        docker_compose_fpath: Optional[str] = None,
    ):
        # Chain the __init__ Docstring up the MRO since nothing changed
        self.__class__.__init__.__doc__ = super().__init__.__doc__
        super().__init__(
            conn_params,
            down_on_del=down_on_del,
            docker_compose_fpath=docker_compose_fpath,
        )
        self._ensure_securities_schema_format()

    def _ensure_securities_schema_format(self):
        with self._cursor() as cursor:
            cursor.execute(self[Op.SELECT, GenericTbls.SCHEMA_TABLES](Schema.SECURITY))
            tables: set[str] = {rsp[0] for rsp in cursor.fetchall()}

            if AssetTbls.SYMBOLS not in tables:
                # Init Symbols Table & pg_trgm Text Search Functions
                log.info("Creating Table '%s'.'%s'", Schema.SECURITY, AssetTbls.SYMBOLS)
                cursor.execute(self[Op.CREATE, AssetTbls._SYMBOL_SEARCH_FUNCS]())
                cursor.execute(self[Op.CREATE, AssetTbls.SYMBOLS]())

            # Init Symbol Data Range Metadata table & support function
            log.debug(
                "Ensuring Table '%s'.'%s' Exists",
                Schema.SECURITY,
                AssetTbls._METADATA,
            )
            cursor.execute(self[Op.CREATE, AssetTbls._METADATA_FUNC]())
            cursor.execute(self[Op.CREATE, AssetTbls._METADATA]())

    def upsert_securities(
        self,
        symbols: DataFrame,
        source: str,
        *,
        on_conflict: Literal["update", "ignore"] = "update",
    ) -> Tuple[Series, Series]:
        """
        Insert the Dataframe of symbols into the database.
        Primary Each for each entry is (Ticker, Exchange, Source)

        -- PARAMS --

        symbols: Dataframe.
            - Required Columns {ticker:str, name:str, exchange:str}:
                -- symbol:str - Ticker Symbol abbreviation
                -- name:Str - Full String Name of the Symbol
                -- exchange:str - Abbrv. Exchange Name for the symbol
                -- asset_class:str - Type of asset, This must match the Timeseries Config
                    asset_classes otherwise data for this symbol cannot be inserted into the
                    database.

            - Any Extra Columns will be packaged into JSON and dumped into an 'attrs' Dictionary.
            To prevent bloat, drop all columns that will not be used.

        source: string.
            - string representation of what API Sourced the symbol
            - e.g. Alpaca, IBKR, Polygon, Coinbase, etc.

        on_conflict: Literal["update", "ignore"] : default = "update"
            Update or Do Nothing when given a symbol that has a conflicting primary key.

        -- RETURNS --
            Tuple of [Series, Series]
            First Series Object is inserted Symbols
            Second Series Object is Updated Symbols
        """
        symbols_fmt = _prep_symbols_upsert(symbols, source)
        if symbols_fmt is None:
            return Series(), Series()

        with self._cursor() as cursor:
            # Create & Inject the Data into a Temporary Table
            cursor.execute(self[Op.CREATE, AssetTbls.SYMBOLS_BUFFER]())
            copy_cmd = self[Op.COPY, AssetTbls.SYMBOLS_BUFFER](
                # Sends the COPY Cmd & the order of the Columns of the Dataframe
                [str(c) for c in symbols_fmt.columns]
            )
            with cursor.copy(copy_cmd) as copy:
                for row in symbols_fmt.itertuples(index=False, name=None):
                    # Writes each row as a Tuple that matches the Dataframe Column Order
                    copy.write_row(row)

            # Merge the Temp Table By inserting / upserting from the Temporary Table
            _op = Op.UPSERT if on_conflict == "update" else Op.INSERT
            cursor.execute(self[_op, AssetTbls.SYMBOLS_BUFFER](source))
            response = DataFrame(cursor.fetchall())

        return _pkg_symbols_upsert_response(response, _op)

    def search_symbols(
        self,
        filter_args: dict[SymbolArgs | str, Any],
        return_attrs: bool = False,
        attrs_search: bool = False,
        limit: int | None = 100,
        *,
        strict_symbol_search: bool | Literal["ILIKE", "LIKE", "="] = False,
    ) -> list[dict]:
        """
        Search the database's symbols table returning all the symbols that match the given criteria.
        Search function supports trigram based fuzzy name + symbol search.

        -- PARAMS --
        - filter_args: dict[SymbolArgs | str : Any]
            - The filtering arguments that need to be matched against. By default only the keys that
            match the table column names (SymbolArgs Literal, e.g. pkey, name, symbol, etc.) will be
            used.
            - All Arguments aside from 'name' and 'symbol' will be used in a strict '=' comparison
            filter. 'name' will always be used in a fuzzystr trigram search where the results are
            ordered by relevancy. by default, 'symbol' will also be a fuzzystr trigram search.
            - When a 'pkey' filter is given, all other filter keys are ignored and the table is
            searched for the given integer pkey. This is because, by table definition, the pkey
            will be unique and can only ever return a single row.
            - Additional argument keys  that are not column names of the table (Not in SymbolArgs
            Literal) will be ignored by default. See attrs_search for more on this behavior.

        - return_attrs: boolean.
            - When True return an 'attrs' dictionary that has all additional attributes of the
            symbol that are stored in the 'attrs' column of the table.

        - attrs_search: boolean.
            - When True any additional keys that are given as filters args, but not recognized as
            table columns, will be used in a strict search against that 'attrs' JSON Column of the
            table.
            - When False additional keys within the filter_args are ignored.
            - i.e. when true, if {'shortable':True} is passed in filter_args then only rows that
            have a defined 'shortable'= True Attrubute will be returned.
            - This search will only ever be a strict '=' comparison, so if searching for a string
            or int the value given must be exact.

        - limit: int
            - The Optional Integer limit of symbol results to return.

        - strict_symbol_search: Boolean | Literal["ILIKE", "LIKE", "="] : default False.
            - When False a fuzzystr trigram search is used and the results are ordered by relevancy.
            Even if an exact match for the symbol is returned, this will still result in other
            similar symbols being returned.
            - When not False the symbol search will use the given PostgreSQL comparator. True
            equates to passing 'ILIKE' Which ignores case.
            - This is far more useful when passing a symbol with wildcard args. e.g.
            'ILIKE' + {symbol:'sp'} will likely not return results, 'ILIKE' + {symbol:'sp%'}
            will return all symbols starting with 'SP', case insensitive.
        """

        name, symbol, limit, filters, attrs = _prep_symbol_search(
            filter_args, attrs_search, limit, strict_symbol_search
        )

        with self._cursor(dict_cursor=True) as cursor:
            cursor.execute(self[Op.SELECT, AssetTbls.SYMBOLS](name, symbol, filters, return_attrs, attrs, limit))
            return cursor.fetchall()

    def distinct_sources(self, filters: dict[SymbolArgs | str, Any] = {}) -> list[str]:
        "Return the distinct Data Broker Sources stored in the Symbols Table."
        return [arg[0] for arg in self.select_distinct_symbols({"source"}, filters)]

    def distinct_exchanges(self, filters: dict[SymbolArgs | str, Any] = {}) -> list[str]:
        "Return the Distinct Exchanges stored in the Symbols Table."
        return [arg[0] for arg in self.select_distinct_symbols({"exchange"}, filters)]

    def distinct_asset_classes(self, filters: dict[SymbolArgs | str, Any] = {}) -> list[str]:
        "Return the distinct Asset Classes stored in the Symbols Table."
        return [arg[0] for arg in self.select_distinct_symbols({"asset_class"}, filters)]

    def select_distinct_symbols(
        self, args: Iterable[SymbolArgs], filters: dict[SymbolArgs | str, Any] = {}
    ) -> list[tuple]:
        "Select the distinct combinations of the given arguments from the Symbols Table."
        args = [arg for arg in args if arg in SYMBOL_ARGS]  # filter to only valid args
        _filters = [(k, "=", v) for k, v in filters.items()]
        rsp, _ = self.execute(
            self[Op.SELECT, GenericTbls.TABLE](Schema.SECURITY, AssetTbls.SYMBOLS, args, _filters, distinct=True),
        )
        return rsp

    def update_symbol(self, symbols: int | str | list[int | str], args: dict[SymbolArgs | str, Any]) -> bool:
        """
        Update a Single Symbol or list of symbols with the given arguments.

        This method is remained general for utility purposes. It should generally just be used to
        update the stored_[tick/minute/aggregate] columns. All Other parameters should remain
        constant by nature. To insert symbols see the API_Extension that allows this to be done
        in bulk.

        Note: Setting any stored column = False does not Delete any Data.

        :params:
        - symobls : int, str, or list[int | str]
            - Identifying symbol (str) or primary key (int) of the symbol to update.
            May be a single value or list of values.
            - Interger Pkeys are preferred method since multiple rows of the same symbol may be
            inserted into the database. Unique Symbol collisions only occur on conflicting
            (symbol, exchange, source) combinations while pkeys are always unique.

        - args : Dict[SymbolArgs, Any]
            - A Dictionary of Column Values to update. If PKEY is passed as a Key it will be
            ignored. Extra Keys are Ignored.
            - Note: This can throw a psycopg.Database Error if passed an update to Symbol, Source,
            or Exchange that would result in a change that would violate the UNIQUE flag on those
            collective parameters.

        :returns: Boolean, True on Successful Update.
        """
        # Convert varierty of symbol inputs to consistent list of integer pkeys
        if isinstance(symbols, int):
            pkeys = [symbols]
        elif isinstance(symbols, str):
            pkeys = [self._get_pkey(symbols)]
            if pkeys[0] is None:
                log.warning("Cannot Update Symbol %s, symbol is not known.", symbols)
                return False
        else:
            _tmp_list = [(self._get_pkey(symbol), symbol) for symbol in symbols]
            pkeys = [_tmp_list for pkey, _ in _tmp_list if pkey is not None]
            unknown_symbols = [symbol for pkey, symbol in _tmp_list if pkey is None]
            log.warning("Cannot Update Symbol(s) %s, symbol is not known.", unknown_symbols)

        if "pkey" in args:
            args.pop("pkey")

        _args = [(k, v) for k, v in args.items() if k in SYMBOL_ARGS]
        if len(_args) == 0:
            log.warning(
                "Attemping to update Database symbol but no arg updates where given. pkey(s) = %s",
                pkeys,
            )
            return False

        if len(pkeys) == 0:
            log.warning("Attemping to update Database symbol(s) but no pkeys were given")
            return False

        # Convert The pkeys to a List so only one Update Command needs to be sent.
        _filter = sql.SQL("pkey=ANY(ARRAY[{pkeys}])").format(pkeys=sql.SQL(",").join(sql.Literal(v) for v in pkeys))

        with self._cursor() as cursor:
            cursor.execute(self[Op.UPDATE, AssetTbls.SYMBOLS](_args, _filter))
            return cursor.statusmessage is not None and cursor.statusmessage == "UPDATE 1"

        return False  # Default return if cursor throws error


class AsyncSymbolsPartial(PsyscaleAsyncCore, SymbolsPartial):
    "Async Symbols Table search function"

    async def upsert_securities_async(
        self,
        symbols: DataFrame,
        source: str,
        *,
        on_conflict: Literal["update", "ignore"] = "update",
    ) -> Tuple[Series, Series]:
        """
        Insert the Dataframe of symbols into the database.
        Primary Each for each entry is (Ticker, Exchange, Source)

        -- PARAMS --

        symbols: Dataframe.
            - Required Columns {ticker:str, name:str, exchange:str}:
                -- symbol:str - Ticker Symbol abbreviation
                -- name:Str - Full String Name of the Symbol
                -- exchange:str - Abbrv. Exchange Name for the symbol
                -- asset_class:str - Type of asset, This must match the Timeseries Config
                    asset_classes otherwise data for this symbol cannot be inserted into the
                    database.

            - Any Extra Columns will be packaged into JSON and dumped into an 'attrs' Dictionary.
            To prevent bloat, drop all columns that will not be used.

        source: string.
            - string representation of what API Sourced the symbol
            - e.g. Alpaca, IBKR, Polygon, Coinbase, etc.

        on_conflict: Literal["update", "ignore"] : default = "update"
            Update or Do Nothing when given a symbol that has a conflicting primary key.

        -- RETURNS --
            Tuple of [Series, Series]
            First Series Object is inserted Symbols
            Second Series Object is Updated Symbols
        """
        symbols_fmt = _prep_symbols_upsert(symbols, source)
        if symbols_fmt is None:
            return Series(), Series()

        async with self._acursor() as cursor:
            # Create & Inject the Data into a Temporary Table
            await cursor.execute(self[Op.CREATE, AssetTbls.SYMBOLS_BUFFER]())
            copy_cmd = self[Op.COPY, AssetTbls.SYMBOLS_BUFFER](
                # Sends the COPY Cmd & the order of the Columns of the Dataframe
                [str(c) for c in symbols_fmt.columns]
            )
            async with cursor.copy(copy_cmd) as copy:
                for row in symbols_fmt.itertuples(index=False, name=None):
                    # Writes each row as a Tuple that matches the Dataframe Column Order
                    await copy.write_row(row)

            # Merge the Temp Table By inserting / upserting from the Temporary Table
            _op = Op.UPSERT if on_conflict == "update" else Op.INSERT
            await cursor.execute(self[_op, AssetTbls.SYMBOLS_BUFFER](source))
            response = DataFrame(await cursor.fetchall())

        return _pkg_symbols_upsert_response(response, _op)

    async def search_symbols_async(
        self,
        filter_args: dict[SymbolArgs | str, Any],
        return_attrs: bool = False,
        attrs_search: bool = False,
        limit: int | None = 100,
        *,
        strict_symbol_search: bool | Literal["ILIKE", "LIKE", "="] = False,
    ) -> list[dict]:
        "See symbol_search method docstring"
        name, symbol, limit, filters, attrs = _prep_symbol_search(
            filter_args, attrs_search, limit, strict_symbol_search
        )

        async with self._acursor(dict_cursor=True) as cursor:
            await cursor.execute(self[Op.SELECT, AssetTbls.SYMBOLS](name, symbol, filters, return_attrs, attrs, limit))
            return await cursor.fetchall()


def _prep_symbol_search(
    filter_args: dict[SymbolArgs | str, Any],
    attrs_search: bool,
    limit: int | None,
    strict_symbol_search: bool | Literal["ILIKE", "LIKE", "="],
) -> tuple:

    if "pkey" in filter_args:
        # Fast Track PKEY Searches since there will only ever be 1 symbol returned
        filters = [("pkey", "=", filter_args["pkey"])]
        name, symbol, attrs, limit = None, None, None, 1
    else:
        filters = [(k, "=", v) for k, v in filter_args.items() if k in STRICT_SYMBOL_ARGS]

        name = filter_args.get("name", None)
        symbol = filter_args.get("symbol", None)

        if strict_symbol_search and symbol is not None:
            # Determine if symbol is passed as a strict or fuzzy parameter
            compare_method = (
                strict_symbol_search
                if isinstance(strict_symbol_search, str)
                else "ILIKE"  # Default search for similar symbols that match ignoring case.
            )
            filters.append(("symbol", compare_method, symbol))
            symbol = None  # Prevents the 'similarity' search from being added

        # Filter all extra given filter params into a separate dict
        attrs = dict((k, v) for k, v in filter_args.items() if k not in SYMBOL_ARGS) if attrs_search else None
        if attrs and len(attrs) == 0:
            attrs = None

    return name, symbol, limit, filters, attrs


def _prep_symbols_upsert(symbols: DataFrame, source: str) -> DataFrame | None:

    if not isinstance(source, str) or source == "":
        log.error("Cannot Insert Symbols, Invalid Source, Source = %s", source)
        return
    if not isinstance(symbols, DataFrame):
        log.error("Cannot Insert Symbols, Invalid Symbols Argument")
        return

    symbols.columns = symbols.columns.str.lower()
    req_cols = {"symbol", "name", "exchange", "asset_class"}
    missing_cols = req_cols.difference(symbols.columns)
    if len(missing_cols) != 0:
        log.error("Cannot insert symbols. Dataframe missing Columns: %s", missing_cols)
        return

    # Convert to a format that can be inserted into the database
    symbols_fmt = symbols[[*req_cols]].copy()

    # Turn all extra Columns into an attributes json obj.
    symbols_fmt.loc[:, "attrs"] = symbols[[*set(symbols.columns).difference(req_cols)]].apply(
        lambda x: x.to_json(), axis="columns"
    )

    return symbols_fmt


def _pkg_symbols_upsert_response(response: DataFrame, _op: Op) -> Tuple[Series, Series]:
    if len(response) == 0:
        return Series(), Series()

    if _op == Op.INSERT:
        # All Returned symbols in response were inserted, none updated.
        return response[0], Series()
    else:
        # Second Column is xmax, on insertion this is 0, on update its != 0
        inserted = response[1] == "0"
        return response.loc[inserted, 0], response.loc[~inserted, 0]
