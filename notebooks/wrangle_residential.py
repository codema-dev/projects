# %%
from pathlib import Path

import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from dublin_building_stock.residential import (
    anonymise_census_2011_hh_indiv_to_routing_key_boundaries,
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
from dublin_building_stock.spatial_operations import get_geometries_within

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
dublin_routing_key_boundaries = gpd.read_file(
    data_dir / "dublin_routing_key_boundaries.geojson", driver="GeoJSON"
)

# %%
dublin_municipality_boundaries = gpd.read_file(
    data_dir / "Dublin_Census2011_Admin_Counties_generalised20m"
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
# %%
# Anonymise Census 2011 to share on Google Colab
census_2011_hh_indiv = pd.read_parquet(data_dir / "census_2011_hh_indiv.parquet")
anonymise_census_2011_hh_indiv_to_routing_key_boundaries(
    data_dir,
    census_2011_hh_indiv,
    dublin_routing_key_boundaries,
    dublin_small_area_boundaries_2011,
)

# %%
# Extract BER Public data to share on Google Colab
ber_public = dd.read_parquet(data_dir / "BERPublicsearch_parquet")
use_columns = [
    "CountyName",
    "DwellingTypeDescr",
    "Year_of_Construction",
    "EnergyRating",
    "GroundFloorArea",
    "FirstFloorArea",
    "SecondFloorArea",
    "ThirdFloorArea",
    "GroundFloorHeight",
    "FirstFloorHeight",
    "SecondFloorHeight",
    "ThirdFloorHeight",
    "NoStoreys",
    "RoofArea",
    "DoorArea",
    "WallArea",
    "WindowArea",
    "UValueWall",
    "UValueRoof",
    "UValueFloor",
    "UvalueDoor",
    "UValueWindow",
]
dublin_ber_public = (
    ber_public.loc[:, use_columns]
    .loc[ber_public["CountyName"].str.contains("Dublin")]
    .compute()
)
dublin_ber_public.to_csv(data_dir / "dublin_ber_public.csv", index=False)

# %%
