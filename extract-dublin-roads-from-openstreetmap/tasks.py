from os import PathLike
from typing import Dict
from typing import List

import geopandas as gpd
import osmnx


def download_roads_from_openstreetmaps(
    product: PathLike, upstream: Dict[str, PathLike], columns: List[str]
) -> None:
    dublin_boundary_polygon = gpd.read_file(
        str(upstream["download_dublin_boundary"])
    ).geometry.item()

    roads = osmnx.geometries_from_polygon(
        dublin_boundary_polygon, tags={"highway": True}
    )
    selected_columns = roads[columns].reset_index()
    selected_columns.to_file(str(product), driver="GPKG")


def cut_lines_on_boundaries(
    product: PathLike,
    upstream: Dict[str, PathLike],
) -> None:
    network = gpd.read_file(str(upstream["download_roads_from_openstreetmaps"]))
    small_area_boundaries = gpd.read_file(
        str(upstream["download_dublin_small_area_boundaries"])
    )

    # extract lines & convert to Irish Transverse Mercator CRS
    is_line = network.geometry.geom_type == "LineString"
    lines = network[is_line].to_crs(epsg=2157)
    lines_in_boundaries = gpd.overlay(lines, small_area_boundaries, "intersection")

    lines_in_boundaries.to_file(str(product))


def sum_small_area_line_lengths(
    product: PathLike, upstream: Dict[str, PathLike]
) -> None:
    lines = gpd.read_file(str(upstream["cut_lines_on_boundaries"]), driver="GPKG")

    lines["line_length_m"] = lines.geometry.length
    line_length_totals = (
        lines.groupby(["small_area", "highway"])["line_length_m"]
        .sum()
        .unstack()
        .fillna(0)
    )

    line_length_totals.to_csv(product)
