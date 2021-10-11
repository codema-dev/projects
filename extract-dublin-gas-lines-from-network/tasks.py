from os import PathLike
from pathlib import Path
from shutil import unpack_archive
from typing import Any
from typing import Dict

import geopandas as gpd
import pandas as pd


def check_gni_data_is_uploaded(product: PathLike) -> None:
    message = "Please upload GNI CAD Network data (Tx and Dx) to data/raw/"
    assert Path(product).exists(), message


def unzip_gni_cad_data(product: PathLike, upstream: Dict[str, PathLike]) -> None:
    unpack_archive(
        filename=upstream["check_gni_data_is_uploaded"],
        extract_dir=Path(product).parent,
    )


def convert_gni_data_to_parquet(product: Any, upstream: Dict[str, PathLike]) -> None:
    dirpath = Path(upstream["unzip_gni_cad_data"])
    filenames = [
        f for f in dirpath.iterdir() if ("Centreline" in f.name) and ("shp" in f.suffix)
    ]
    lines = pd.concat([gpd.read_file(f, crs="EPSG:2157") for f in filenames])
    lines.to_parquet(product)


def extract_lines_in_small_area_boundaries(
    product: Any,
    upstream: Any,
) -> None:
    lines = gpd.read_parquet(upstream["convert_gni_data_to_parquet"])
    dublin_small_area_boundaries = gpd.read_file(
        str(upstream["download_dublin_small_area_boundaries"])
    )

    lines_in_boundaries = gpd.overlay(
        lines,
        dublin_small_area_boundaries[["small_area", "geometry"]],
        "intersection",
    )

    lines_in_boundaries.to_parquet(product)


def calculate_line_lengths(product: Any, upstream: Any) -> None:
    lines = gpd.read_parquet(upstream["extract_lines_in_small_area_boundaries"])
    lines["line_length_m"] = lines.geometry.length
    lines.to_file(str(product), driver="GPKG")


def sum_small_area_line_lengths(product: Any, upstream: Any) -> None:
    line_lengths = gpd.read_file(str(upstream["calculate_line_lengths"]), driver="GPKG")

    line_length_totals = (
        line_lengths.groupby(["small_area", "diameter"])["line_length_m"]
        .sum()
        .unstack()
        .fillna(0)
    )

    line_length_totals.to_csv(product)
