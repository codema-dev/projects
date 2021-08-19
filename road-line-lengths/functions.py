from pathlib import Path
from typing import List
from typing import Optional

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
    url: str, filepath: Path, columns: Optional[List[str]] = None, to_crs="EPSG:4326"
) -> gpd.GeoDataFrame:
    if filepath.exists():
        gdf = gpd.read_parquet(filepath)
    else:
        gdf = gpd.read_file(url).to_crs(to_crs)
        if columns:
            gdf = gdf[columns]
            gdf.to_parquet(filepath)
        else:
            gdf.to_parquet(filepath)
    return gdf


def dissolve_geometries_to_shapely_polygon(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return gdf.dissolve().iloc[0].item()


def load_roads(polygon: gpd.GeoDataFrame, filepath: Path, columns: List[str]):
    if filepath.exists():
        roads = gpd.read_parquet(filepath)
    else:
        roads = osmnx.geometries_from_polygon(polygon, tags={"highway": True})
        roads[columns].reset_index().to_parquet(filepath)
    return roads


def extract_lines(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    is_line = gdf.geometry.geom_type == "LineString"
    return gdf[is_line].copy()


def cut_lines_on_boundaries(
    lines: gpd.GeoDataFrame, boundaries: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    return gpd.overlay(lines, boundaries, "union")


def measure_line_lengths_in_boundaries(
    gdf: gpd.GeoDataFrame, boundary_column_name: str
) -> gpd.GeoDataFrame:
    line_lengths = pd.concat(
        [gdf[boundary_column_name], gdf.geometry.to_crs(epsg=2157).length], axis=1
    )
    return (
        line_lengths.groupby(boundary_column_name, as_index=False)
        .sum()
        .rename(columns={0: "line_length_m"})
    )


def save_to_gpkg(gdf: gpd.GeoDataFrame, filepath: Path) -> None:
    gdf.to_file(filepath, driver="GPKG")
