from configparser import ConfigParser
from pathlib import Path
from typing import Callable
from typing import List

import fsspec
import pandas as pd


def load_valuation_office(urls: List[Path], filesystem_name: str):
    dfs: List[pd.DataFrame] = []
    fs = fsspec.filesystem(filesystem_name)
    for url in urls:
        with fs.open(url) as f:
            df = pd.read_excel(url)
        dfs.append(df)
    return pd.concat(dfs)


def load_bers(url: str, filesystem_name: str):
    fs = fsspec.filesystem(filesystem_name)
    with fs.open(url) as f:
        return pd.read_parquet(url)
