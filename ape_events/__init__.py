# Add module top-level imports here
from typing import Optional

import pandas as pd
from ape import plugins
from ape.api.query import ContractEventQuery, QueryAPI


class CacheLogsProvider(QueryAPI):
    def estimate_query(self, query: ContractEventQuery) -> Optional[int]:
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
