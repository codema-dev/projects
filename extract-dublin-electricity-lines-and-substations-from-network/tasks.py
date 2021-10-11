from os import PathLike
from pathlib import Path
from shutil import unpack_archive
from typing import Any
from typing import Dict
from typing import List

import geopandas as gpd
import pandas as pd


def check_esb_cad_data_is_uploaded(product: PathLike) -> None:
    message = "Please upload zipped ESB CAD Network data!"
    assert Path(product).exists(), message


def unzip_esb_cad_data(product: PathLike, upstream: Dict[str, PathLike]) -> None:
    unpack_archive(
        filename=upstream["check_esb_cad_data_is_uploaded"],
        extract_dir=Path(product).parent,
    )


def convert_hv_data_to_parquet(product: Any, upstream: Dict[str, PathLike]) -> None:
    dirpath = Path(upstream["unzip_esb_cad_data"]) / "Dig Request Style" / "HV Data"
    network = [gpd.read_file(filepath) for filepath in dirpath.iterdir()]
    hv_network = pd.concat(network)

    # set coordinate reference system to irish grid
    hv_network.crs = "EPSG:29903"

    # convert to irish transverse mercator
    hv_network.to_crs(epsg=2157).to_parquet(product)


def convert_mv_lv_data_to_parquet(product: Any, upstream: Dict[str, PathLike]) -> None:
    dublin_mv_index = pd.read_csv(upstream["download_dublin_mv_index"], squeeze=True)
    dirpath = Path(upstream["unzip_esb_cad_data"]) / "Dig Request Style" / "MV-LV Data"
    network = [gpd.read_file(dirpath / f"{id}.dgn") for id in dublin_mv_index]
    mv_lv_network = pd.concat(network)

    # set coordinate reference system to irish grid
    mv_lv_network.crs = "EPSG:29903"

    # convert to irish transverse mercator
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

    # convert Text from bytes to string
    mv_lv_network["Text"] = mv_lv_network["Text"].str.decode("utf-8", errors="ignore")

    text_is_a_station = mv_lv_network["Text"].isin(text_mappings.keys())
    stations = mv_lv_network[text_is_a_station].copy()
    stations["Type"] = stations["Text"].map(text_mappings)
    stations.to_file(str(product), driver="GPKG")


def extract_hv_lines_in_small_area_boundaries(
    product: Any,
    upstream: Any,
    level_mappings: Dict[int, str],
    columns: List[str],
) -> None:
    network = gpd.read_parquet(upstream["convert_hv_data_to_parquet"])
    dublin_small_area_boundaries = gpd.read_file(
        str(upstream["download_dublin_small_area_boundaries"])
    )

    level_is_a_line = network["Level"].isin(level_mappings.keys())
    lines = network.loc[level_is_a_line, columns].copy()
    lines["Type"] = lines["Level"].map(level_mappings)
    lines_in_boundaries = gpd.overlay(
        lines,
        dublin_small_area_boundaries[["small_area", "geometry"]],
        "intersection",
    )

    lines_in_boundaries.to_parquet(product)


def extract_mv_lv_lines_in_small_area_boundaries(
    product: Any,
    upstream: Any,
    level_mappings: Dict[int, str],
    columns: List[str],
) -> None:
    network = gpd.read_parquet(upstream["convert_mv_lv_data_to_parquet"])
    dublin_small_area_boundaries = gpd.read_file(
        str(upstream["download_dublin_small_area_boundaries"])
    )

    level_is_a_line = network["Level"].isin(level_mappings.keys())
    lines = network.loc[level_is_a_line, columns].copy()
    lines["Type"] = lines["Level"].map(level_mappings)
    lines_in_boundaries = gpd.overlay(
        lines,
        dublin_small_area_boundaries[["small_area", "geometry"]],
        "intersection",
    )

    lines_in_boundaries.to_parquet(product)


def calculate_hv_line_lengths(product: Any, upstream: Any) -> None:
    lines = gpd.read_parquet(upstream["extract_hv_lines_in_small_area_boundaries"])
    lines["line_length_m"] = lines.geometry.length
    lines.to_file(str(product), driver="GPKG")


def calculate_mv_lv_line_lengths(product: Any, upstream: Any) -> None:
    lines = gpd.read_parquet(upstream["extract_mv_lv_lines_in_small_area_boundaries"])
    lines["line_length_m"] = lines.geometry.length
    lines.to_file(str(product), driver="GPKG")


def sum_small_area_mv_lv_line_lengths(product: Any, upstream: Any) -> None:
    line_lengths = gpd.read_file(str(upstream["calculate_mv_lv_line_lengths"]))

    line_length_totals = (
        line_lengths.groupby(["small_area", "Type"])["line_length_m"]
        .sum()
        .unstack()
        .fillna(0)
    )

    line_length_totals.to_csv(product)


def sum_small_area_hv_line_lengths(product: Any, upstream: Any) -> None:
    line_lengths = gpd.read_file(str(upstream["calculate_hv_line_lengths"]))

    line_length_totals = (
        line_lengths.groupby(["small_area", "Type"])["line_length_m"]
        .sum()
        .unstack()
        .fillna(0)
    )

    line_length_totals.to_csv(product)
