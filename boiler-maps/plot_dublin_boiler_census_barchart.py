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
import pandas_bokeh

# %% tags=["parameters"]
product = None
upstream = None

# %%
dublin_small_area_boiler_statistics = gpd.read_file(
    upstream["extract_dublin_boiler_statistics"]["data"]
)

# %%
pandas_bokeh.output_file(product["barchart"])

# %%
use_columns = [
    "local_authority",
    "No central heating",
    "Oil",
    "Natural gas",
    "Electricity",
    "Coal (incl. anthracite)",
    "Peat (incl. turf)",
    "Liquid petroleum gas (LPG)",
    "Wood (incl. wood pellets)",
    "Other",
    "Not stated",
]

local_authority_boiler_statistics = (
    dublin_small_area_boiler_statistics[use_columns].groupby("local_authority").sum().T
)

# %%
local_authority_boiler_statistics[
    "All of Dublin"
] = local_authority_boiler_statistics.sum(axis=1)

# %%
local_authority_boiler_statistics.plot_bokeh(
    kind="barh", xlabel="Number of Dwellings", number_format="1.00"
)

# %%
local_authority_boiler_statistics.to_csv(product["local_authority_stats"])
