from os import PathLike
from os import PathLike
from typing import Dict

import geopandas as gpd
import pandas as pd


def _convert_dataframe_to_geodataframe(
    df: pd.DataFrame,
    x: str,
    y: str,
    from_crs: str,
    to_crs: str = "EPSG:2157",
) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[x], df[y], crs=from_crs)
    ).to_crs(to_crs)


def extract_dublin_substations(
    upstream: Dict[str, PathLike], product: PathLike
) -> None:
    lv_substations = pd.read_csv(upstream["download_esb_substation_capacities"]).pipe(
        _convert_dataframe_to_geodataframe,
        x="Longitude",
        y="Latitude",
        from_crs="EPSG:4326",
        to_crs="EPSG:2157",
    )
    small_area_boundaries = gpd.read_file(
        str(upstream["download_dublin_small_area_boundaries"])
    ).to_crs("EPSG:2157")
    dublin_lv_substations = gpd.sjoin(
        lv_substations, small_area_boundaries, op="within"
    )
    dublin_lv_substations.to_file(str(product), driver="GPKG")
