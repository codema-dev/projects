from pathlib import Path
from typing import Any
from typing import Dict
from typing import List

import geopandas as gpd
import pandas as pd


def _check_gni_data_is_uploaded(filepath: str) -> None:
    message = "Please upload GNI CAD Network data (Tx and Dx) to data/raw/"
    assert Path(filepath).exists(), message


def save_gni_data_to_gpkg(product: Any, filepaths: List[str]) -> None:
    _check_gni_data_is_uploaded(filepaths[0])
    lines = pd.concat([gpd.read_file(f) for f in filepaths])
    lines.to_file(str(product), driver="GPKG")
