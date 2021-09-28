from pathlib import Path
from typing import Any
from typing import List

import geopandas as gpd
import pandas as pd


def _check_esb_data_is_uploaded(dirpath: str) -> None:
    message = "Please upload ESB CAD Network data (ESBdata_20210107) to data/raw/"
    assert Path(dirpath).exists(), message


def convert_hv_data_to_parquet(product: Any, dirpath: str) -> gpd.GeoDataFrame:
    _check_esb_data_is_uploaded(dirpath)
    network = [gpd.read_file(filepath) for filepath in Path(dirpath).iterdir()]
    hv_network = gpd.GeoDataFrame(pd.concat(network), crs="EPSG:29903")
    hv_network.to_crs(epsg=2157).to_parquet(product)


def convert_mv_lv_network_to_parquet(
    product: Any, upstream: Any, dirpath: str
) -> gpd.GeoDataFrame:
    _check_esb_data_is_uploaded(dirpath)
    dublin_mv_index = pd.read_csv(upstream["download_dublin_mv_index"], squeeze=True)
    network = [gpd.read_file(Path(dirpath) / f"{id}.dgn") for id in dublin_mv_index]
    mv_lv_network = gpd.GeoDataFrame(pd.concat(network), crs="EPSG:29903")
    mv_lv_network.to_crs(epsg=2157).to_parquet(product)


def extract_line_lengths(
    product: Any,
    upstream: Any,
    levels: List[str],
    columns: List[str],
) -> None:
    hv_network = gpd.read_parquet(upstream["convert_hv_data_to_parquet"])
    level_is_a_line = hv_network["Level"].isin(levels)
    lines = hv_network[level_is_a_line]
    line_lengths = pd.concat(
        [lines[columns], lines.geometry.length.rename("line_length_m")], axis=1
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
