from pathlib import Path
from typing import Any

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
