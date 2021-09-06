# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.5
#   kernelspec:
#     display_name: 'Python 3.9.6 64-bit (''urban-atlas'': conda)'
#     name: python3
# ---

# %%
from pathlib import Path
import geopandas as gpd

# %% tags=["parameters"]
upstream = None
product = None

# %%
urban_atlas = gpd.read_file(upstream["check_urban_atlas_is_uploaded"])

# %%
small_areas = gpd.read_file(upstream["download_small_areas"])

# %%
urban_atlas_in_small_areas = gpd.overlay(
    urban_atlas.to_crs(epsg=2157),
    small_areas.to_crs(epsg=2157),
    how="intersection",
)

# %%
urban_atlas_in_small_areas.to_file(product["gpkg"], driver="GPKG")

# %%
urban_atlas_small_area_item_area = (
    urban_atlas_in_small_areas.groupby(["small_area", "ITEM"])
    .agg({"geometry": lambda x: x.area.sum()})
    .rename(columns={"geometry": "area_m2"})
)

# %%
urban_atlas_small_area_item_area.to_csv(product["csv"])
