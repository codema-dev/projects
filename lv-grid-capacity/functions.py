from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd


def create_folder_structure(data_dirpath: Path) -> None:
    data_dirpath.mkdir(exist_ok=True)
    external_dir = data_dirpath / "external"
    external_dir.mkdir(exist_ok=True)
    interim_dir = data_dirpath / "interim"
    interim_dir.mkdir(exist_ok=True)
    processed_dir = data_dirpath / "processed"
    processed_dir.mkdir(exist_ok=True)


def convert_to_geodataframe(
    df: pd.DataFrame,
    x: str,
    y: str,
    from_crs: str,
    to_crs: str = "EPSG:2157",
) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[x], df[y], crs=from_crs)
    ).to_crs(to_crs)


def groupby_sum(
    gdf: gpd.GeoDataFrame,
    by: List[str],
    on_columns: List[str],
) -> gpd.GeoDataFrame:
    return gdf.groupby(by)[on_columns].sum()
