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
upstream = ["extract_dublin_boiler_statistics"]

# %%
dublin_small_area_boiler_statistics = gpd.read_file(
    upstream["extract_dublin_boiler_statistics"]["data"]
)

# %%
boiler_columns = [
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
    "Total",
]

# %%
selected_fuel_types = ["Natural gas", "Oil", "Electricity", "No central heating"]

# %%
remaining_fuel_types = [c for c in boiler_columns if c not in selected_fuel_types]

# %%
use_columns = selected_fuel_types + ["small_area", "geometry"]

# %%
other = (
    dublin_small_area_boiler_statistics[remaining_fuel_types]
    .sum(axis=1)
    .rename("Other")
)

# %%
boiler_map = pd.concat(
    [other, dublin_small_area_boiler_statistics[use_columns]], axis=1
)

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
            <td>Natural gas</td>
            <td>@{Natural gas}</td>
        </tr>
        <tr>
            <td>Oil</td>
            <td>@Oil</td>
        </tr>
        <tr>
            <td>Electricity</td>
            <td>@Electricity</td>
        </tr>
        <tr>
            <td>No central heating</td>
            <td>@{No central heating}</td>
        </tr>
        <tr>
            <td>Other</td>
            <td>@Other</td>
        </tr>
    </table>
"""
dublin_small_area_boiler_statistics.plot_bokeh(
    figsize=(500, 500),
    dropdown=["Natural gas", "Oil", "Electricity", "No central heating", "Other"],
    hovertool_string=hovertool_string,
    fill_alpha=0.9,
)

# %%
dublin_small_area_boiler_statistics.to_csv(product["csv"])
