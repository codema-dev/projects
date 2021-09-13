# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.12.0
# kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import geopandas as gpd
import pandas as pd
import seaborn as sns

import pandas_bokeh

sns.set()  # override matplotlib with seaborn

# + tags=["parameters"]
upstream = [
    "calculate_pre_retrofit_small_area_heat_pump_viability",
    "calculate_post_retrofit_small_area_heat_pump_viability",
    "download_small_area_boundaries",
]
product = None
# -

small_area_heat_pump_viability_pre_retrofit = pd.read_csv(
    upstream["calculate_pre_retrofit_small_area_heat_pump_viability"]
).rename(
    columns={
        "percentage_of_heat_pump_ready_dwellings": "percentage_of_pre_retrofit_heat_pump_ready_dwellings"
    }
)

small_area_heat_pump_viability_post_retrofit = pd.read_csv(
    upstream["calculate_post_retrofit_small_area_heat_pump_viability"]
).rename(
    columns={
        "percentage_of_heat_pump_ready_dwellings": "percentage_of_post_retrofit_heat_pump_ready_dwellings"
    }
)

small_area_boundaries = gpd.read_file(str(upstream["download_small_area_boundaries"]))

small_area_heat_pump_viability_map = small_area_boundaries.merge(
    small_area_heat_pump_viability_pre_retrofit
).merge(small_area_heat_pump_viability_post_retrofit)

ax = small_area_heat_pump_viability_map.plot(
    column="percentage_of_pre_retrofit_heat_pump_ready_dwellings",
    figsize=(15, 15),
    legend=True,
    linewidth=0.25,
)
ax.set_title("Percentage Pre Retrofit Heat Pump Viability [13/09/2021]", size=20)

ax = small_area_heat_pump_viability_map.plot(
    column="percentage_of_post_retrofit_heat_pump_ready_dwellings",
    figsize=(15, 15),
    legend=True,
    linewidth=0.25,
)
ax.set_title("Percentage Post Retrofit Heat Pump Viability [13/09/2021]", size=20)

pandas_bokeh.output_file(product["html"])
small_area_heat_pump_viability_map.plot_bokeh(
    figsize=(500, 500),
    dropdown=[
        "percentage_of_pre_retrofit_heat_pump_ready_dwellings",
        "percentage_of_post_retrofit_heat_pump_ready_dwellings",
    ],
)

small_area_heat_pump_viability_map.to_file(product["gpkg"], driver="GPKG")
