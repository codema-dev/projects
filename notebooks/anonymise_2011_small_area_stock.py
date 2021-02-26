# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.10.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
from os import path
from shutil import unpack_archive
from urllib.request import urlretrieve

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from dublin_building_stock import join

# %% [markdown]
# # Amalgamate Cross-tabulated Small Area building stock for all Dublin LAs

# %%
dublin_buildings_at_small_area = (
    pd.concat(
        [
            pd.read_csv(f"../data/{local_authority}_SA_2011.csv")
            for local_authority in ["DCC", "DLR", "FCC", "SD"]
        ]
    )
    .query("`sa_2011` != ['Dublin City', 'South Dublin']")
    .query("`period_built_unstandardised` != ['Total', 'All Houses']")
    .replace({">3": 1, "<3": 1, ".": np.nan})
    .dropna(subset=["value"])
    .assign(
        value=lambda df: df["value"].astype(np.int32),
        sa_2011=lambda df: df["sa_2011"].str.replace(r"_", r"/"),
        period_built_unstandardised=lambda df: df["period_built_unstandardised"]
        .str.lower()
        .str.replace("2006 or later", "2006 - 2011"),
    )
)

# %% [markdown]
# # Get 2011 Small Area Boundaries (linked to Postcodes)

# %%
small_area_boundaries_filepath = (
    "../data/small_areas_boundaries_2011_linked_to_autoaddress_dublin_postcodes.geojson"
)
if not path.exists(small_area_boundaries_filepath):
    urlretrieve(
        url="https://zenodo.org/record/4564475/files/small_areas_boundaries_2011_linked_to_autoaddress_dublin_postcodes.geojson",
        filename=small_area_boundaries_filepath,
    )

small_area_boundaries = gpd.read_file(
    small_area_boundaries_filepath, driver="GeoJSON"
).to_crs(epsg=2157)


# %% [markdown]
# # Expand each Small Area to Individual Buildings

# %%
def expand_to_indiv_buildings(stock, on="value"):
    return pd.DataFrame(stock.values.repeat(stock[on], axis=0), columns=stock.columns)


dublin_indiv_buildings_at_small_area = expand_to_indiv_buildings(
    dublin_buildings_at_small_area
).drop(columns="value")

# %% [markdown]
# # Extract Dublin Small Area boundaries by pulling Dublin LA SAs from All-Of-Ireland SA Boundaries

# %%
dublin_small_areas = gpd.GeoDataFrame(
    dublin_indiv_buildings_at_small_area.merge(
        small_area_boundaries, how="left", left_on="sa_2011", right_on="SMALL_AREA"
    )
    .loc[:, ["SMALL_AREA", "geometry"]]
    .drop_duplicates()
)

# %% [markdown]
# # Anonymise stock to Postcode level

# %%
dublin_indiv_buildings_at_postcode_level = dublin_indiv_buildings_at_small_area.merge(
    small_area_boundaries, how="left", left_on="sa_2011", right_on="SMALL_AREA"
).drop(columns=["sa_2011", "SMALL_AREA", "geometry"])

# %% [markdown]
# # Save

# %%
dublin_indiv_buildings_at_postcode_level.to_csv(
    "../data/dublin_building_stock_up_to_2011.csv", index=False
)
