import os
import pickle
from typing import List, Optional

from ape import plugins
from ape.api.query import ContractEventQuery, QueryAPI
from ape.types import ContractLog, LogFilter
from pony import orm

db = orm.Database()


class LogQuery(db.Entity):
    _table_ = "log_queries"
    address = orm.Required(str)
    event_name = orm.Required(str)
    event_abi = orm.Required(orm.Json)
    last_cached_block = orm.Required(int)
    logs = orm.Set(lambda: LogCache)

    orm.PrimaryKey(address, event_name)


class LogCache(db.Entity):
    _table_ = "cached_logs"
    query = orm.Required(LogQuery)
    data = orm.Required(bytes)


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

    def perform_query(self, query: ContractEventQuery) -> List[ContractLog]:
        if not isinstance(query, ContractEventQuery):
            return None
        with orm.db_session:
            db_query = LogQuery[query.contract, query.event.name]
            cached_logs = [
                pickle.loads(log.data)
                for log in orm.select(log for log in LogCache if log.query == db_query)
            ]

        log_filter = LogFilter.from_event(
            event=query.event,
            addresses=[query.contract],
            start_block=db_query.last_cached_block + 1,
            stop_block=query.stop_block,
        )
        fetched_logs = self.provider.get_contract_logs(log_filter)
        return cached_logs + list(fetched_logs)

    def update_cache(self, query: ContractEventQuery, result: List[ContractLog]):
        if not isinstance(query, ContractEventQuery):
            return
        with orm.db_session:
            db_query = LogQuery[query.contract, query.event.name]
            for log in result:
                if log.block_number < db_query.last_cached_block:
                    continue
                LogCache(
                    query=db_query,
                    data=pickle.dumps(log),
                )
            db_query.last_cached_block = query.stop_block - 1


@plugins.register(plugins.QueryPlugin)
def query_engines():
    return [CacheLogsProvider]
