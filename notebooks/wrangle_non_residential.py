# %%
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from dublin_building_stock.non_residential import (
    load_uses_benchmarks,
    load_benchmarks,
    create_valuation_office_public,
    create_valuation_office_private,
    anonymise_valuation_office_private,
)

data_dir = Path("../data")

# %%
small_area_boundaries_2011 = gpd.read_file(
    data_dir / "Dublin_Census2011_Small_Areas_generalised20m"
)
# %%
uses_linked_to_benchmarks = load_uses_benchmarks(data_dir)
# %%
benchmarks = load_benchmarks(data_dir)
# %%
create_valuation_office_public(
    data_dir, uses_linked_to_benchmarks, benchmarks, small_area_boundaries_2011
)
# %%
create_valuation_office_private(
    data_dir, uses_linked_to_benchmarks, benchmarks, small_area_boundaries_2011
)
# %%
vo_public = gpd.read_file(data_dir / "valuation_office_public.gpkg", driver="GPKG")
vo_private = gpd.read_file(data_dir / "valuation_office_private.gpkg", driver="GPKG")
anonymise_valuation_office_private(data_dir, vo_private, vo_public)
# %%
vo_public = gpd.read_file(data_dir / "valuation_office_public.gpkg", driver="GPKG")
# %%
