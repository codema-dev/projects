from typing import Any
from typing import Dict
from typing import List

import geopandas as gpd
import pandas as pd


def extract_hv_line_lengths(
    product: Any,
    upstream: Any,
    columns: List[str],
) -> None:
    lines = gpd.read_file(upstream["download_dublin_small_area_boundaries"])
    line_lengths = pd.concat(
        [lines, lines.geometry.length.rename("line_length_m")], axis=1
    )
    line_lengths.to_parquet(product)
