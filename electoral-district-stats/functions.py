from pathlib import Path

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


def amalgamate_to_granularity(
    df: pd.DataFrame, granularity: str, columns: str, on: str
):
    breakpoint()
    return (
        df.groupby([granularity, columns])[on]
        .sum()
        .reset_index()
        .pivot(index=granularity, columns=columns, values=on)
        .fillna(0)
    )
