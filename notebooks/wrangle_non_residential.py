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
    create_m_and_r,
    create_geocoded_m_and_r,
)

data_dir = Path("../data")


# %%
dublin_boundary = gpd.read_file(data_dir / "dublin_boundary.geojson", driver="GeoJSON")

# %%
dublin_routing_key_boundaries = gpd.read_file(
    data_dir / "dublin_routing_key_boundaries.geojson", driver="GeoJSON"
)

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
create_m_and_r(data_dir)

# %%
m_and_r = pd.read_csv(data_dir / "m_and_r.csv")
create_geocoded_m_and_r(
    data_dir, m_and_r, dublin_boundary, dublin_routing_key_boundaries
)

# %%
m_and_r_locations = gpd.read_file(
    data_dir / "M&R_clean_addresses_geocoded_by_google_maps.geojson", driver="GeoJSON"
)
m_and_r_geocoded = m_and_r_locations.merge(
    m_and_r,
    left_on="raw_address",
    right_on="address",
    how="right",
)

# %%
m_and_r_geocoded.to_file(data_dir / "M&R_geocoded.geojson", driver="GeoJSON")

# %%
create_epa_industrial_sites(data_dir)
