# %%
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from dublin_building_stock.non_residential import (
    load_small_area_boundaries,
    load_uses_benchmarks,
    load_benchmarks,
    load_vo_private,
    load_vo_public,
    load_vo_private,
    anonymise_vo_private,
)

data_dir = Path("../data")

# %%
small_area_boundaries = load_small_area_boundaries(data_dir)
# %%
uses_linked_to_benchmarks = load_uses_benchmarks(data_dir)
# %%
benchmarks = load_benchmarks(data_dir)
# %%
vo_public = load_vo_public(data_dir, uses_linked_to_benchmarks, benchmarks)
# %%
vo_private = load_vo_private(
    data_dir, small_area_boundaries, uses_linked_to_benchmarks, benchmarks
)
# %%
vo_private_anonymised = anonymise_vo_private(vo_private, vo_public)
# %%
