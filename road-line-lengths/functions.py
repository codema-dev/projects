from pathlib import Path
from typing import List

import geopandas as gpd
import osmnx
import pandas as pd


def create_folder_structure(data_dirpath: Path) -> None:
    data_dirpath.mkdir(exist_ok=True)
    external_dir = data_dirpath / "external"
    external_dir.mkdir(exist_ok=True)
    interim_dir = data_dirpath / "interim"
    interim_dir.mkdir(exist_ok=True)
    processed_dir = data_dirpath / "processed"
    processed_dir.mkdir(exist_ok=True)


def load_file(
    url: str, filepath: Path, columns: List[str], to_crs="EPSG:4326"
) -> gpd.GeoDataFrame:
    if filepath.exists():
        gdf = gpd.read_parquet(filepath)[columns].to_crs(to_crs)
    else:
        gdf = gpd.read_file(url)[columns].to_crs(to_crs)
        gdf.to_parquet(filepath)
    return gdf


def dissolve_geometries_to_shapely_polygon(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return gdf.dissolve().iloc[0].item()


def load_roads(polygon: gpd.GeoDataFrame, filepath: Path, columns: List[str]):
    breakpoint()
    if filepath.exists():
        roads = gpd.read_parquet(filepath)
    else:
        roads = osmnx.geometries_from_polygon(polygon, tags={"highway": True})
        roads[columns].reset_index().to_parquet(filepath)
    return roads
