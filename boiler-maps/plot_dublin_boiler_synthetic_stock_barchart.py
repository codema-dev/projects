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

import pandas_bokeh

# %% tags=["parameters"]
product = None
upstream = None

# %%
small_area_boundaries = gpd.read_file(upstream["download_small_area_boundaries"])

# %%
buildings = pd.read_parquet(upstream["download_buildings"])

# %%
local_authority_map = small_area_boundaries.set_index("small_area")[
    "local_authority"
].to_dict()

# %%
buildings["local_authority"] = buildings["small_area"].map(local_authority_map)

# %%
where_building_uses_a_heat_pump = buildings["main_sh_boiler_efficiency"] > 100

# %%
main_sh_boiler_fuel = buildings["main_sh_boiler_fuel"].mask(
    where_building_uses_a_heat_pump, "Heat Pump"
)

# %%
boiler_fuels = pd.concat([buildings["local_authority"], main_sh_boiler_fuel], axis=1)

# %%
pandas_bokeh.output_file(product["sh_barchart"])

# %%
local_authority_statistics = (
    boiler_fuels.groupby(["local_authority", "main_sh_boiler_fuel"])
    .size()
    .unstack(fill_value=0)
    .transpose()
)

# %%
local_authority_statistics["All of Dublin"] = local_authority_statistics.sum(axis=1)

# %%
local_authority_statistics.plot_bokeh(
    kind="barh", xlabel="Number of Dwellings", ylabel="", number_format="1.00"
)

# %%
local_authority_statistics.to_csv(product["sh_stats"])
