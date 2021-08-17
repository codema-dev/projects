from pathlib import Path
from typing import List
from typing import Optional
from urllib.request import urlretrieve

import geopandas as gpd
import pandas as pd


def check_file_exists(filepath: Path):
    assert filepath.exists(), f"Please download & unzip {filepath} to data/external/"


def create_folder_structure(data_dirpath: Path) -> None:
    data_dirpath.mkdir(exist_ok=True)
    external_dir = data_dirpath / "external"
    external_dir.mkdir(exist_ok=True)
    interim_dir = data_dirpath / "interim"
    interim_dir.mkdir(exist_ok=True)
    processed_dir = data_dirpath / "processed"
    processed_dir.mkdir(exist_ok=True)


def extract_columns(gdf: gpd.GeoDataFrame, columns: List[str]) -> gpd.GeoDataFrame:
    return gdf[columns].copy()


def download_file(url: str, filepath: Path) -> None:
    if not filepath.exists():
        urlretrieve(url=url, filename=str(filepath))


def read_file(
    filepath: Path, crs: str, columns: Optional[List[str]] = None
) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(filepath)
    gdf.crs = crs
    if columns:
        return gdf[columns]
    else:
        return gdf


def cut_lines_on_boundaries(
    lines: gpd.GeoDataFrame, boundaries: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    return gpd.overlay(lines, boundaries, "union")


def extract_in_boundary(
    gdf: gpd.GeoDataFrame, boundary: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    return gpd.sjoin(gdf, boundary.to_crs(epsg=2157), op="intersects")
