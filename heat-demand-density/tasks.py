from collections import defaultdict
from configparser import ConfigParser
from pathlib import Path
from typing import Callable
from typing import List

import fsspec
import pandas as pd


def load_valuation_office(urls: List[Path], filesystem_name: str) -> pd.DataFrame:
    dfs: List[pd.DataFrame] = []
    fs = fsspec.filesystem(filesystem_name)
    for url in urls:
        with fs.open(url) as f:
            df = pd.read_excel(url)
        dfs.append(df)
    return pd.concat(dfs)


def load_bers(url: str, filesystem_name: str) -> pd.DataFrame:
    fs = fsspec.filesystem(filesystem_name)
    with fs.open(url) as f:
        return pd.read_parquet(url)


def load_benchmark_uses(url: str, filesystem_name: str) -> pd.DataFrame:
    fs = fsspec.filesystem(filesystem_name)
    uses_grouped_by_category = defaultdict()
    for file in fs.glob(url + "/*.txt"):
        name = file.split("/")[-1].replace(".txt", "")
        with fs.open(file, "r") as f:
            uses_grouped_by_category[name] = [line.rstrip() for line in f]
    return {i: k for k, v in uses_grouped_by_category.items() for i in v}
