# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.5
#   kernelspec:
#     display_name: 'Python 3.9.6 64-bit (''boiler-map'': conda)'
#     name: python3
# ---

# %%
import geopandas as gpd
import pandas as pd

# %% tags=["parameters"]
product = None
upstream = ["download_small_area_boundaries", "download_small_area_statistics"]

# %%
dublin_small_area_boundaries_2016 = gpd.read_file(
    upstream["download_small_area_boundaries"]
)

# %%
boiler_column_map = {
    "T6_5_NCH": "No central heating",
    "T6_5_OCH": "Oil",
    "T6_5_NGCH": "Natural gas",
    "T6_5_ECH": "Electricity",
    "T6_5_CCH": "Coal (incl. anthracite)",
    "T6_5_PCH": "Peat (incl. turf)",
    "T6_5_LPGCH": "Liquid petroleum gas (LPG)",
    "T6_5_WCH": "Wood (incl. wood pellets)",
    "T6_5_OTH": "Other",
    "T6_5_NS": "Not stated",
    "T6_5_T": "Total",
}

# %%
use_columns = ["small_area"] + list(boiler_column_map.keys())

# %%
ireland_small_area_boiler_statistics = (
    pd.read_csv(upstream["download_small_area_statistics"])
    .assign(small_area=lambda df: df["GEOGID"].str[7:])
    .loc[:, use_columns]
    .rename(columns=boiler_column_map)
)

# %%
dublin_small_area_boiler_statistics = dublin_small_area_boundaries_2016.merge(
    ireland_small_area_boiler_statistics
)


# %%
dublin_small_area_boiler_statistics.to_file(product["data"], driver="GPKG")
