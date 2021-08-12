from pathlib import Path
from typing import List
from typing import Optional

import geopandas as gpd
import pandas as pd


def check_file_exists(filepath: Path):
    assert filepath.exists(), f"Please download & unzip {filepath} to data/external/"


def read_network(dirpath: Path, levels: Optional[List[str]] = None) -> gpd.GeoDataFrame:
    network = []
    for filepath in dirpath.iterdir():

        if levels:
            region = gpd.read_file(filepath).query(f"`Level` == {str(levels)}")
        else:
            region = gpd.read_file(filepath)

        network.append(region)

    return gpd.GeoDataFrame(pd.concat(network), crs="EPSG:29903").to_crs(epsg=2157)
