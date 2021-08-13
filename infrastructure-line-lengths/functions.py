from pathlib import Path
from typing import List
from typing import Optional
from urllib.request import urlretrieve

import geopandas as gpd
import pandas as pd


def check_file_exists(filepath: Path):
    assert filepath.exists(), f"Please download & unzip {filepath} to data/external/"


def download_file(url: str, filepath: Path) -> None:
    if not filepath.exists():
        urlretrieve(url=url, filename=str(filepath))


def read_network(dirpath: Path) -> gpd.GeoDataFrame:
    network = [gpd.read_file(filepath) for filepath in dirpath.iterdir()]
    return gpd.GeoDataFrame(pd.concat(network), crs="EPSG:29903").to_crs(epsg=2157)
