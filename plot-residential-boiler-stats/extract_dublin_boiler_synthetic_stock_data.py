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
import numpy as np
import pandas as pd


# %% tags=["parameters"]
product = None
upstream = ["download_synthetic_bers"]

# %%
buildings = pd.read_parquet(upstream["download_synthetic_bers"])

# %%
building_uses_a_heat_pump = buildings["main_sh_boiler_efficiency"] > 100

# %%
main_sh_boiler_fuel = buildings["main_sh_boiler_fuel"].mask(
    building_uses_a_heat_pump, "Heat Pump"
)

# %%
building_is_an_apartment = (
    buildings["dwelling_type"].str.lower().str.contains("apartment")
)

# %%
dwelling_types = pd.Series(
    np.where(building_is_an_apartment, "Apartment", "House"), name="dwelling_type"
)

# %%
boiler_fuels = pd.concat(
    [buildings["small_area"], dwelling_types, main_sh_boiler_fuel], axis=1
)

# %%
small_area_boiler_fuels_by_dwelling_type = (
    boiler_fuels.groupby(["small_area", "dwelling_type", "main_sh_boiler_fuel"])
    .size()
    .unstack(fill_value=0)
)

# %%
small_area_boiler_fuels_by_dwelling_type.to_csv(product["by_dwelling_type"])
