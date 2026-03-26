from typing import List, Tuple
from pyspark.sql import DataFrame

def ngrams(grouped_dfs: List[Tuple[List[str], DataFrame]], n: int = 4) -> DataFrame:
    pass