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
buildings = pd.read_parquet(upstream["download_buildings"])

# %%
where_building_uses_a_heat_pump = buildings["main_sh_boiler_efficiency"] > 100

# %%
main_sh_boiler_fuel = buildings["main_sh_boiler_fuel"].mask(
    where_building_uses_a_heat_pump, "Heat Pump"
)

# %%
boiler_fuels = pd.concat(
    [buildings["year_of_construction"], main_sh_boiler_fuel], axis=1
)

# %%
pandas_bokeh.output_file(product["trendline"])

# %%
start_year = 1900

# %%
end_year = 2021

# %%
trendline = (
    boiler_fuels.groupby(["year_of_construction", "main_sh_boiler_fuel"])
    .size()
    .unstack(fill_value=0)
    .cumsum()
    .loc[start_year:]
)

# %%
p = trendline.plot_bokeh(
    kind="area",
    xlabel="Year of Construction",
    ylabel="",
    number_format="1.00",
    legend="top_left",
    figsize=(1000, 450),
    xticks=range(start_year, end_year, 5),
    sizing_mode="scale_width",
    vertical_xlabel=True,
    fontsize_legend="8px",
    stacked=True,
)

# %%
trendline.to_csv(product["stats"])
