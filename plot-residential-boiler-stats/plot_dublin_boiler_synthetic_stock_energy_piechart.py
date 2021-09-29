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
demands = (
    buildings["main_sh_demand"]
    + buildings["main_hw_demand"]
    + buildings["suppl_sh_demand"]
    + buildings["suppl_sh_demand"]
)
demands = demands.rename("heat_demand_kwh")

# %%
boiler_individual_demands = pd.concat([main_sh_boiler_fuel, demands], axis=1)

# %%
boiler_overall_demands = (
    boiler_individual_demands.groupby("main_sh_boiler_fuel")
    .sum()
    .divide(1e6)
    .rename(columns={"heat_demand_kwh": "heat_demand_gwh"})
)

# %%
boiler_overall_demand = boiler_overall_demands.sum()

# %%
boiler_overall_demand_percentages = 100 * boiler_overall_demands / boiler_overall_demand
boiler_overall_demand_percentages = boiler_overall_demand_percentages.rename(
    columns={"heat_demand_gwh": "percentage_heat_demand"}
)

# %%
pandas_bokeh.output_file(product["piechart"])

# %%
p = boiler_overall_demand_percentages.squeeze().plot_bokeh(
    kind="pie",
    xlabel="Number of Dwellings",
    ylabel="",
    number_format="1.00",
    legend=False,
)

# %%
boiler_overall_demand_percentages.to_csv(product["stats"])
