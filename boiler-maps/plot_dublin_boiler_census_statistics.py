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
pandas_bokeh.output_file(product["map"])

# %%
hovertool_string = """
    <h2>@edname<br>@small_area</h2>

    <table style="background-color:#084594;color:#ffffff">
        <tr>
            <th>Boiler<br>Type</th>
            <th>Number of Dwellings</th>
        </tr>
        <tr>
            <td>No central heating</td>
            <td>@{No Central Heating}</td>
        </tr>
        <tr>
            <td>Natural gas</td>
            <td>@{Natural gas}</td>
        </tr>
        <tr>
            <td>Electricity</td>
            <td>@Electricity</td>
        </tr>
        <tr>
            <td>Oil</td>
            <td>@Oil</td>
        </tr>
        <tr>
            <td>Coal</td>
            <td>@{Coal (incl. anthracite)}</td>
        </tr>
        <tr>
            <td>Peat</td>
            <td>@{Peat (incl. turf)}</td>
        </tr>
        <tr>
            <td>LPG</td>
            <td>@{Liquid petroleum gas (LPG)}</td>
        </tr>
        <tr>
            <td>Wood</td>
            <td>@{Wood (incl. wood pellets)}</td>
        </tr>
        <tr>
            <td>Other</td>
            <td>@Other</td>
        </tr>
        <tr>
            <td>Not Stated</td>
            <td>@{Not stated}</td>
        </tr>
        <tr>
            <td><strong>Total</strong></td>
            <td><strong>@Total</strong></td>
        </tr>
    </table>
"""
dublin_small_area_boiler_statistics.plot_bokeh(
    figsize=(700, 900),
    dropdown=[
        "Natural gas",
        "Electricity",
        "Oil",
    ],
    hovertool_string=hovertool_string,
    fill_alpha=0.75,
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
local_authority_boiler_statistics.plot_bokeh(kind="barh")

# %%
local_authority_boiler_statistics.to_csv(product["local_authority_stats"])
