import json
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


def read_hv_network(dirpath: Path) -> gpd.GeoDataFrame:
    network = [gpd.read_file(filepath) for filepath in dirpath.iterdir()]
    return gpd.GeoDataFrame(pd.concat(network), crs="EPSG:29903").to_crs(epsg=2157)


def read_mvlv_network(dirpath: Path, ids: List[str]) -> gpd.GeoDataFrame:
    network = []
    for id in ids:
        gdf = gpd.read_file(dirpath / f"{id}.dgn")
        network.append(gdf)
    return gpd.GeoDataFrame(pd.concat(network), crs="EPSG:29903").to_crs(epsg=2157)


def read_file(filepath: Path, crs: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(filepath)
    gdf.crs = crs
    return gdf


def read_csv(filepath: Path, header: str) -> List[str]:
    return pd.read_csv(filepath, squeeze=True, header=header)


def extract_rows_in_list(
    gdf: gpd.GeoDataFrame, on_column: str, list_of_values: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    rows_in_list = gdf[on_column].isin(list_of_values)
    return gdf[rows_in_list]


def query(gdf: gpd.GeoDataFrame, query_str: str) -> gpd.GeoDataFrame:
    return gdf.query(query_str)


def cut_lines_on_boundaries(
    lines: gpd.GeoDataFrame, boundaries: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    return gpd.overlay(lines, boundaries, "union")
