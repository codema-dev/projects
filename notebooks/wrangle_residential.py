# %%
from pathlib import Path

import geopandas as gpd
import pandas as pd

from dublin_building_stock.residential import (
    create_census_2011_hh_indiv,
    create_census_2011_small_area_hhs,
    create_census_2016_hh_age,
    create_census_2016_hh_age_indiv,
    create_census_2016_hh_boilers,
    create_census_2016_hh_type,
    create_dublin_ber_private,
    create_dublin_ber_public,
    create_latest_stock,
)

data_dir = Path("../data")

# %%
dublin_small_area_boundaries_2011 = gpd.read_file(
    data_dir / "Dublin_Census2011_Small_Areas_generalised20m"
)
dublin_small_area_boundaries_2016 = gpd.read_file(
    data_dir
    / "Dublin_Small_Areas_Ungeneralised_-_OSi_National_Statistical_Boundaries_-_2015-shp"
)

# %%
create_census_2016_hh_age(data_dir, dublin_small_area_boundaries_2016)
create_census_2016_hh_type(data_dir, dublin_small_area_boundaries_2016)
create_census_2016_hh_boilers(data_dir, dublin_small_area_boundaries_2016)

# %%
census_2016_hh_age = pd.read_csv(data_dir / "census_2016_hh_age.csv")
create_census_2016_hh_age_indiv(data_dir, census_2016_hh_age)

# %%
create_census_2011_small_area_hhs(data_dir, dublin_small_area_boundaries_2011)

# %%
census_2011_small_area_hhs = pd.read_parquet(
    data_dir / "census_2011_small_area_hhs.parquet"
)
create_census_2011_hh_indiv(data_dir, census_2011_small_area_hhs)

# %%
create_dublin_ber_public(data_dir)

# %%
small_areas_2011_vs_2011 = pd.read_csv(data_dir / "small_areas_2011_vs_2011.csv")
create_dublin_ber_private(data_dir, small_areas_2011_vs_2011)

# %%
census_2011_hh_indiv = pd.read_parquet(data_dir / "census_2011_hh_indiv.parquet")
dublin_ber_private = pd.read_parquet(data_dir / "dublin_ber_private.parquet")
create_latest_stock(data_dir, census_2011_hh_indiv, dublin_ber_private)

# %%
dublin_indiv_hh = pd.read_csv(data_dir / "dublin_indiv_hh.csv", low_memory=False)

# %%
