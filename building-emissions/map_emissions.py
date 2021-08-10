from pathlib import Path

from bokeh.io import export_svg
from bokeh.io import save
from bokeh.io import show
import geopandas as gpd
import numpy as np
import pandas as pd
import pandas_bokeh

from globals import DATA_DIR

## Parametrize
# overwrite parameters with arguemnts generated in prefect pipeline

# + tags=["parameters"]
SAVE_AS_HTML: bool = True
DATA_DIR: str = DATA_DIR
emission_map_filepath: str = (
    DATA_DIR / "processed" / "dublin_small_area_emissions_tco2_per_y.geojson"
)
# -

## Load

emission_map = gpd.read_file(emission_map_filepath)

## Set Globals

local_authorities = emission_map["local_authority"].unique()

## Plot

emission_map["Total"] = emission_map.drop(
    columns=["small_area", "local_authority", "geometry"]
).sum(axis="columns")
emission_map["Quintile"] = pd.qcut(emission_map["Total"], q=20)
emission_map["category"] = emission_map["Quintile"].cat.codes
emission_map["Quintile"] = emission_map["Quintile"].astype("string")

column_names = list(
    emission_map.columns.drop(
        ["small_area", "local_authority", "geometry", "Total", "Quintile", "category"]
    )
)
column_values = ["@{" + name + "}" for name in column_names]
hovertool_string = "<table>"
for name, value in zip(column_names, column_values):
    hovertool_string += (
        "<tr>" + "<td>" + name + "</td>" "<td>" + value + "</td>" + "</tr>"
    )
hovertool_string += "</table>"
figure = emission_map.plot_bokeh(
    figsize=(700, 900),
    category="category",
    show_colorbar=False,
    hovertool_string=hovertool_string,
    legend="Emissions [tCO2]",
    show_figure=True,
)
