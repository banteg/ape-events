# Add module top-level imports here
from typing import Optional

import pandas as pd
from ape import plugins
from ape.types import ContractLog
from ape.api.query import ContractEventQuery, QueryAPI
from pony import orm
import os


db = orm.Database()


class LogQuery(db.Entity):
    contract = orm.Required(str)
    event_name = orm.Required(str)
    event_abi = orm.Required(orm.Json)
    last_cached_block = orm.Required(int)
    logs = orm.Set(lambda: LogCache)


class LogCache(db.Entity):
    query = orm.Required(LogQuery)
    data = orm.Required(orm.Json)


class CacheLogsProvider(QueryAPI):
    def __init__(self):
        db.bind(
            provider="postgres",
            user=os.environ.get("PGUSER", "postgres"),
            host=os.environ.get("PGHOST", "127.0.0.1"),
            password=os.environ.get("PGPASS", None),
            database="ape-events",
        )
        db.generate_mapping(create_tables=True)

    def estimate_query(self, query: ContractEventQuery) -> Optional[int]:
        print(query.dict(include={"contract": True, "event": True}))
        if not isinstance(query, ContractEventQuery):
            return None

        return 100

    def perform_query(self, query: ContractEventQuery) -> pd.DataFrame:
        if not isinstance(query, ContractEventQuery):
            return None

        return ["success!"]

    def update_cache(self, query: ContractEventQuery, result: pd.DataFrame):
        if not isinstance(query, ContractEventQuery):
            return
        print(f"updated cache {query} with {result}")


@plugins.register(plugins.QueryPlugin)
def query_engines():
    return [CacheLogsProvider]
