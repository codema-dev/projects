from pathlb import Path
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
