from pathlib import Path
from typing import Any
from typing import Dict
from typing import List

import geopandas as gpd
import pandas as pd


def _check_esb_data_is_uploaded(dirpath: str) -> None:
    message = "Please upload ESB CAD Network data (ESBdata_20210107) to data/raw/"
    assert Path(dirpath).exists(), message


def convert_hv_data_to_parquet(product: Any, dirpath: str) -> None:
    _check_esb_data_is_uploaded(dirpath)
    network = [gpd.read_file(filepath) for filepath in Path(dirpath).iterdir()]
    hv_network = gpd.GeoDataFrame(pd.concat(network), crs="EPSG:29903")
    hv_network.to_crs(epsg=2157).to_parquet(product)


def convert_mv_lv_data_to_parquet(product: Any, upstream: Any, dirpath: str) -> None:
    _check_esb_data_is_uploaded(dirpath)
    dublin_mv_index = pd.read_csv(upstream["download_dublin_mv_index"], squeeze=True)
    network = [gpd.read_file(Path(dirpath) / f"{id}.dgn") for id in dublin_mv_index]
    mv_lv_network = gpd.GeoDataFrame(pd.concat(network), crs="EPSG:29903")
    mv_lv_network.to_crs(epsg=2157).to_parquet(product)


def extract_hv_stations(product: Any, upstream: Any, levels: List[int]) -> None:
    hv_network = gpd.read_parquet(upstream["convert_hv_data_to_parquet"])
    level_is_a_station = hv_network["Level"].isin(levels)
    stations = hv_network[level_is_a_station]
    stations.to_file(str(product), driver="GPKG")


def extract_mv_lv_stations(
    product: Any, upstream: Any, text_mappings: Dict[str, str]
) -> None:
    mv_lv_network = gpd.read_parquet(upstream["convert_mv_lv_data_to_parquet"])
    mv_lv_network["Text"] = mv_lv_network["Text"].str.decode("utf-8", errors="ignore")
    text_is_a_station = mv_lv_network["Text"].isin(text_mappings.keys())
    stations = mv_lv_network[text_is_a_station].copy()
    stations["Type"] = stations["Text"].map(text_mappings)
    stations.to_file(str(product), driver="GPKG")


def _extract_line_lengths(
    gdf: gpd.GeoDataFrame,
    level_mappings: Dict[int, str],
    columns: List[str],
) -> None:
    level_is_a_line = gdf["Level"].isin(level_mappings.keys())
    lines = gdf[level_is_a_line]
    line_lengths = pd.concat(
        [lines[columns], lines.geometry.length.rename("line_length_m")], axis=1
    )
    line_lengths["Type"] = line_lengths["Level"].map(level_mappings)
    return line_lengths


def extract_hv_line_lengths(
    product: Any,
    upstream: Any,
    level_mappings: Dict[int, str],
    columns: List[str],
) -> None:
    hv_network = gpd.read_parquet(upstream["convert_hv_data_to_parquet"])
    line_lengths = _extract_line_lengths(
        hv_network, level_mappings=level_mappings, columns=columns
    )
    line_lengths.to_parquet(product)


def extract_mv_lv_line_lengths(
    product: Any,
    upstream: Any,
    level_mappings: Dict[str, str],
    columns: List[str],
) -> None:
    hv_network = gpd.read_parquet(upstream["convert_mv_lv_data_to_parquet"])
    line_lengths = _extract_line_lengths(
        hv_network, level_mappings=level_mappings, columns=columns
    )
    line_lengths.to_parquet(product)


def extract_hv_line_length_in_small_area_boundaries(
    product: Any, upstream: Any
) -> None:
    hv_line_lengths = gpd.read_parquet(upstream["extract_hv_line_lengths"])
    dublin_small_area_boundaries = gpd.read_file(
        str(upstream["download_dublin_small_area_boundaries"])
    )

    lines_in_boundaries = gpd.overlay(
        hv_line_lengths,
        dublin_small_area_boundaries[["small_area", "geometry"]],
        "intersection",
    )

    lines_in_boundaries.to_file(str(product), driver="GPKG")


def extract_mv_lv_line_length_in_small_area_boundaries(
    product: Any, upstream: Any
) -> None:
    mv_lv_line_lengths = gpd.read_parquet(upstream["extract_mv_lv_line_lengths"])
    dublin_small_area_boundaries = gpd.read_file(
        str(upstream["download_dublin_small_area_boundaries"])
    )

    lines_in_boundaries = gpd.overlay(
        mv_lv_line_lengths,
        dublin_small_area_boundaries[["small_area", "geometry"]],
        "intersection",
    )

    lines_in_boundaries.to_file(str(product), driver="GPKG")


def sum_small_area_mv_lv_line_lengths(product: Any, upstream: Any) -> None:
    line_lengths = gpd.read_file(
        str(upstream["extract_mv_lv_line_length_in_small_area_boundaries"])
    )

    line_length_totals = (
        line_lengths.groupby(["small_area", "Type"])["line_length_m"]
        .sum()
        .unstack()
        .fillna(0)
    )

    line_length_totals.to_csv(product)


def sum_small_area_hv_line_lengths(product: Any, upstream: Any) -> None:
    line_lengths = gpd.read_file(
        str(upstream["extract_hv_line_length_in_small_area_boundaries"])
    )

    line_length_totals = (
        line_lengths.groupby(["small_area", "Type"])["line_length_m"]
        .sum()
        .unstack()
        .fillna(0)
    )

    line_length_totals.to_csv(product)
