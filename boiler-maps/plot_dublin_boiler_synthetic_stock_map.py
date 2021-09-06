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
small_area_boundaries = gpd.read_file(upstream["download_small_area_boundaries"]).loc[
    :, ["small_area", "geometry"]
]

# %%
buildings = pd.read_parquet(upstream["download_buildings"])

# %%
where_building_uses_a_heat_pump = buildings["main_sh_boiler_efficiency"] > 100

# %%
main_sh_boiler_fuel = buildings["main_sh_boiler_fuel"].mask(
    where_building_uses_a_heat_pump, "Heat Pump"
)

# %%
is_a_major_fuel_type = main_sh_boiler_fuel.isin(
    ["Mains Gas", "Heating Oil", "Electricity", "Heat Pump"]
)  #

# %%
major_boiler_fuels = main_sh_boiler_fuel.where(is_a_major_fuel_type, "Other")

# %%
boiler_fuels = pd.concat([buildings["small_area"], major_boiler_fuels], axis=1)

# %%

# %%
small_area_total = (
    boiler_fuels.groupby(["small_area", "main_sh_boiler_fuel"])
    .size()
    .unstack(fill_value=0)
    .reset_index()
)

# %%
boiler_fuel_map = small_area_boundaries.merge(small_area_total)

# %%
pandas_bokeh.output_file(product["map"])

# %%
hovertool_string = """
    <h2>@small_area</h2>

    <table style="background-color:#084594;color:#ffffff">
        <tr>
            <th>Boiler<br>Type</th>
            <th>Number of<br>Dwellings</th>
        </tr>
        <tr>
            <td>Mains Gas</td>
            <td>@{Mains Gas}</td>
        </tr>
        <tr>
            <td>Heating Oil</td>
            <td>@{Heating Oil}</td>
        </tr>
        <tr>
            <td>Electricity</td>
            <td>@Electricity</td>
        </tr>
        <tr>
            <td>Heat Pump</td>
            <td>@{Heat Pump}</td>
        </tr>
        <tr>
            <td>Other</td>
            <td>@Other</td>
        </tr>
    </table>
"""
boiler_fuel_map.plot_bokeh(
    figsize=(500, 500),
    fill_alpha=0.75,
    dropdown=[
        "Mains Gas",
        "Heating Oil",
        "Electricity",
        "Heat Pump",
    ],
    hovertool_string=hovertool_string,
)
