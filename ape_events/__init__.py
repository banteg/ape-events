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
    address = orm.Required(str)
    event_name = orm.Required(str)
    event_abi = orm.Required(orm.Json)
    last_cached_block = orm.Required(int)
    logs = orm.Set(lambda: LogCache)

    orm.PrimaryKey(address, event_name)


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
        if not isinstance(query, ContractEventQuery):
            return None

        with orm.db_session:
            try:
                db_query = LogQuery[query.contract, query.event.name]
            except orm.ObjectNotFound:
                db_query = LogQuery(
                    address=query.contract,
                    event_name=query.event.name,
                    event_abi=query.event.dict(),
                    last_cached_block=0,
                )

        return (
            100
            * (self.chain_manager.blocks.height - db_query.last_cached_block)
            / self.provider.block_page_size
        )

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
